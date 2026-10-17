//! Job-control contracts for [`SubAgentTool`]: independent per-job
//! cancellation, parent-cancel cascade, panic safety.

use super::*;
use std::sync::Arc;

use serde_json::json;

use super::test::{BlockedModel, spawned_job_id, wait_for_terminal};
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::new_call_id;
use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::tool::ToolDispatch;
use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};
use tinytools::Tool;

pub(super) struct PanickingModel;

#[async_trait::async_trait]
impl ChatModel<()> for PanickingModel {
    async fn invoke(
        &self,
        _state: &(),
        _request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        panic!("child model exploded");
    }
}

fn blocked_tool() -> (
    Arc<SubAgentTool<(), ()>>,
    Arc<tokio::sync::Semaphore>,
    Arc<tokio::sync::Semaphore>,
) {
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut harness = AgentHarness::new();
    harness.register_model(
        "worker",
        Arc::new(BlockedModel {
            started: started.clone(),
            release: release.clone(),
        }),
    );
    let tool = Arc::new(SubAgentTool::new(
        Arc::new(SubAgent::new("worker", "works", Arc::new(harness))),
        ChildDataPolicy::new(|_: &()| ()),
    ));
    (tool, started, release)
}

async fn spawn(tool: &SubAgentTool<(), ()>, parent: &RunContext<()>) -> String {
    let result = tool
        .invoke_in_parent_context(
            &(),
            json!({"input": "work"}),
            tinytools::ToolCallOptions::default(),
            parent,
        )
        .await
        .expect("spawn succeeds");
    spawned_job_id(&result)
}

async fn cancel_via_tool(
    jobs: &SubAgentJobsTool,
    parent: &RunContext<()>,
    job_id: &str,
) -> anyhow::Result<tinytools::ToolResult> {
    ToolDispatch::<(), ()>::execute(
        jobs,
        &(),
        new_call_id(),
        json!({"action": "cancel", "job_id": job_id}),
        tinytools::ToolCallOptions::default(),
        parent,
    )
    .await
}

#[tokio::test]
async fn cancelling_one_job_leaves_its_sibling_running() {
    let (tool, started, release) = blocked_tool();
    let jobs = tool.job_registry().clone();
    let jobs_tool = SubAgentJobsTool::new(jobs.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let owner = parent.instance_id();

    let first = spawn(&tool, &parent).await;
    let second = spawn(&tool, &parent).await;
    let _first = started.acquire().await.unwrap();
    let _second = started.acquire().await.unwrap();

    let result = cancel_via_tool(&jobs_tool, &parent, &first)
        .await
        .expect("owner may cancel");
    assert!(result.output().contains("cancelled"));
    let cancelled = wait_for_terminal(&jobs, &first, owner).await;
    assert_eq!(cancelled.status, SubAgentJobStatus::Cancelled);

    assert_eq!(
        jobs.get_owned(&second, owner).unwrap().status,
        SubAgentJobStatus::Running,
        "sibling keeps running"
    );
    assert!(
        !parent.cancellation.is_cancelled(),
        "child cancel must not cancel the parent"
    );

    release.add_permits(1);
    let done = wait_for_terminal(&jobs, &second, owner).await;
    assert_eq!(done.status, SubAgentJobStatus::Completed);
}

#[tokio::test]
async fn jobs_await_waits_for_the_terminal_snapshot() {
    let (tool, started, release) = blocked_tool();
    let jobs = SubAgentJobsTool::new(tool.job_registry().clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let id = spawn(&tool, &parent).await;
    let _started = started.acquire().await.unwrap();
    let waiter = ToolDispatch::<(), ()>::execute(
        &jobs,
        &(),
        new_call_id(),
        json!({"action":"await","job_id":id}),
        tinytools::ToolCallOptions::default(),
        &parent,
    );
    release.add_permits(1);
    let result = waiter.await.unwrap();
    let value: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["output"], "finished later");
}

#[tokio::test]
async fn cancel_is_owner_checked_and_rejects_terminal_jobs() {
    let (tool, started, release) = blocked_tool();
    let jobs = tool.job_registry().clone();
    let jobs_tool = SubAgentJobsTool::new(jobs.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let stranger = RunContext::new(RunConfig::new("stranger"), ());

    let job = spawn(&tool, &parent).await;
    let _permit = started.acquire().await.unwrap();

    assert!(
        cancel_via_tool(&jobs_tool, &stranger, &job).await.is_err(),
        "a non-owner cannot cancel"
    );
    assert_eq!(
        jobs.get_owned(&job, parent.instance_id()).unwrap().status,
        SubAgentJobStatus::Running
    );

    release.add_permits(1);
    wait_for_terminal(&jobs, &job, parent.instance_id()).await;
    assert!(
        cancel_via_tool(&jobs_tool, &parent, &job).await.is_err(),
        "terminal jobs cannot be cancelled again"
    );
}

#[tokio::test]
async fn parent_cancellation_cascades_to_running_jobs() {
    let (tool, started, _release) = blocked_tool();
    let jobs = tool.job_registry().clone();
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let job = spawn(&tool, &parent).await;
    let _permit = started.acquire().await.unwrap();
    parent.cancellation.cancel();

    let job = wait_for_terminal(&jobs, &job, parent.instance_id()).await;
    assert_eq!(job.status, SubAgentJobStatus::Cancelled);
}

#[tokio::test]
async fn panicking_child_marks_the_job_failed() {
    let mut harness = AgentHarness::new();
    harness.register_model("worker", Arc::new(PanickingModel));
    let tool = SubAgentTool::new(
        Arc::new(SubAgent::new("worker", "works", Arc::new(harness))),
        ChildDataPolicy::new(|_: &()| ()),
    );
    let jobs = tool.job_registry().clone();
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let job_id = spawn(&tool, &parent).await;
    let job = wait_for_terminal(&jobs, &job_id, parent.instance_id()).await;
    assert_eq!(job.status, SubAgentJobStatus::Failed);
    assert!(
        job.error.as_deref().is_some_and(|e| e.contains("panicked")),
        "error explains the panic: {:?}",
        job.error
    );
}

async fn message_via_tool(
    tool: &SubAgentMessageTool,
    parent: &RunContext<()>,
    args: serde_json::Value,
) -> serde_json::Value {
    let result = ToolDispatch::<(), ()>::execute(
        tool,
        &(),
        new_call_id(),
        args,
        tinytools::ToolCallOptions::default(),
        parent,
    )
    .await
    .expect("message accepted");
    serde_json::from_str(&result.output()).expect("JSON result")
}

#[tokio::test]
async fn message_with_a_repeated_request_id_is_queued_once() {
    let jobs = SubAgentJobRegistry::new();
    let tool = SubAgentMessageTool::new(jobs.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, steering) = jobs.create("worker", parent.instance_id());
    let args = json!({"job_id": job_id.as_str(), "message": "look here", "request_id": "r1"});

    let first = message_via_tool(&tool, &parent, args.clone()).await;
    assert_eq!(first["status"], "message_queued");
    assert!(first.get("duplicate").is_none());
    assert_eq!(steering.pending(), 1);

    let again = message_via_tool(&tool, &parent, args).await;
    assert_eq!(again["duplicate"], true);
    assert_eq!(steering.pending(), 1, "duplicate is not enqueued again");

    // A new id and an id-less message are both delivered.
    message_via_tool(
        &tool,
        &parent,
        json!({"job_id": job_id.as_str(), "message": "m", "request_id": "r2"}),
    )
    .await;
    message_via_tool(
        &tool,
        &parent,
        json!({"job_id": job_id.as_str(), "message": "m"}),
    )
    .await;
    assert_eq!(steering.pending(), 3);
}

#[tokio::test]
async fn message_request_ids_are_bounded_per_job() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, steering) = jobs.create("worker", 1);
    for index in 0..=tinyagents_harness::steering::RecentRequestIds::DEFAULT_CAPACITY {
        let duplicate = jobs
            .send_message_with_request_id(job_id.as_str(), 1, "m", Some(&format!("r{index}")))
            .unwrap();
        assert!(!duplicate);
    }
    // `r0` aged out of the 64-id window, so it is delivered again.
    assert!(
        !jobs
            .send_message_with_request_id(job_id.as_str(), 1, "m", Some("r0"))
            .unwrap()
    );
    assert_eq!(steering.pending(), 66);
}

#[tokio::test]
async fn message_tool_can_recover_from_a_terminal_job_selector_when_enabled() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, _) = jobs.create("worker", parent.instance_id());
    jobs.cancel_owned(job_id.as_str(), parent.instance_id())
        .expect("the owned job can be cancelled");
    jobs.mark_result(&job_id, Err(TinyAgentsError::Cancelled));

    let args = json!({"job_id": job_id.as_str(), "message": "please continue"});
    let recovering = SubAgentMessageTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &recovering,
        &(),
        new_call_id(),
        args.clone(),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a completed job cannot receive a message");
    assert!(matches!(
        error.downcast_ref::<TinyAgentsError>(),
        Some(TinyAgentsError::ModelRetry(_))
    ));

    let recovering_jobs = SubAgentJobsTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &recovering_jobs,
        &(),
        new_call_id(),
        json!({"action": "cancel", "job_id": job_id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a terminal job cannot be cancelled again");
    assert!(matches!(
        error.downcast_ref::<TinyAgentsError>(),
        Some(TinyAgentsError::ModelRetry(_))
    ));

    let default = SubAgentMessageTool::new(jobs);
    let error = ToolDispatch::<(), ()>::execute(
        &default,
        &(),
        new_call_id(),
        args,
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("default message tools preserve the typed terminal error");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Terminal { .. })
    ));
}

#[test]
fn registry_lock_failures_are_not_selector_denials() {
    let error =
        super::jobs::registry_failure(tinyagents_tasks::DetachedTaskRegistryError::LockPoisoned);
    assert!(matches!(error, SubAgentJobError::Registry(_)));
    assert!(!super::jobs::is_recoverable_selector_denial(&error));
}

#[tokio::test]
async fn terminal_jobs_never_return_to_running() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, _steering) = jobs.create("worker", 1);
    jobs.cancel_owned(job_id.as_str(), 1).expect("cancel");
    jobs.mark_result(&job_id, Err(TinyAgentsError::Cancelled));
    jobs.mark_running(&job_id);
    assert_eq!(
        jobs.get(job_id.as_str()).unwrap().status,
        SubAgentJobStatus::Cancelled
    );
}

#[tokio::test]
async fn settled_jobs_release_their_cancellation_token() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, _steering) = jobs.create("worker", 1);
    assert!(jobs.holds_live_cancellation(&job_id));
    jobs.cancel_owned(job_id.as_str(), 1).expect("cancel");
    assert!(!jobs.holds_live_cancellation(&job_id));
    assert!(!jobs.get(job_id.as_str()).unwrap().status.is_terminal());
    jobs.mark_result(&job_id, Err(TinyAgentsError::Cancelled));
    assert!(jobs.get(job_id.as_str()).unwrap().status.is_terminal());

    let (aborted, _steering) = jobs.create("worker", 1);
    jobs.mark_aborted(&aborted, true);
    assert!(!jobs.holds_live_cancellation(&aborted));
}

#[tokio::test]
async fn jobs_tool_treats_a_null_action_as_query() {
    let jobs = SubAgentJobRegistry::new();
    let tool = SubAgentJobsTool::new(jobs.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    jobs.create("worker", parent.instance_id());
    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"action": null}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("null action queries");
    let listed: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(listed.as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn oversized_message_request_ids_are_rejected() {
    let jobs = SubAgentJobRegistry::new();
    let tool = SubAgentMessageTool::new(jobs.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, steering) = jobs.create("worker", parent.instance_id());
    let too_long = "x".repeat(129);
    let rejected = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"job_id": job_id.as_str(), "message": "m", "request_id": too_long}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await;
    assert!(rejected.is_err());
    assert_eq!(steering.pending(), 0);
    let at_limit = message_via_tool(
        &tool,
        &parent,
        json!({"job_id": job_id.as_str(), "message": "m", "request_id": "x".repeat(128)}),
    )
    .await;
    assert_eq!(at_limit["status"], "message_queued");
}

#[tokio::test]
async fn a_budget_overrun_that_raced_an_owner_cancel_settles_cancelled() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, _steering) = jobs.create("worker", 1);
    jobs.cancel_owned(job_id.as_str(), 1).expect("cancel");
    jobs.mark_budget_overrun(
        &job_id,
        crate::subagent::AppliedResult {
            text: "late".into(),
            ..Default::default()
        },
        "over budget".into(),
    );
    let job = jobs.get(job_id.as_str()).unwrap();
    assert_eq!(job.status, SubAgentJobStatus::Cancelled);
    assert!(job.incomplete_kind.is_none() && job.output.is_none());
}

#[tokio::test]
async fn named_job_alias_uses_configured_operation_and_identifier_fields() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, _) = jobs.create("worker", parent.instance_id());
    let schema = json!({
        "type": "object",
        "required": ["task_id"],
        "properties": {"task_id": {"type": "string"}}
    });
    let tool = SubAgentJobsTool::new(jobs.clone())
        .with_tool_name("list_subagents")
        .with_description("List or query subagent work.")
        .with_parameters(schema.clone())
        .with_fixed_action(SubAgentJobAction::Query)
        .with_identifier_field("task_id");

    assert_eq!(tool.name(), "list_subagents");
    assert_eq!(tool.description(), "List or query subagent work.");
    assert_eq!(tool.parameters_schema(), schema);
    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"task_id": job_id.as_str(), "action": "cancel"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("alias queries the requested job");
    let returned: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(returned["id"], job_id.as_str());
    assert_eq!(
        jobs.get_owned(job_id.as_str(), parent.instance_id())
            .unwrap()
            .status,
        SubAgentJobStatus::Queued,
        "the caller cannot replace a configured query with a cancel action"
    );
}

#[tokio::test]
async fn fixed_job_alias_rejects_missing_identifiers_and_defaults_stay_compatible() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let alias = SubAgentJobsTool::new(jobs.clone())
        .with_tool_name("cancel_subagent")
        .with_fixed_action(SubAgentJobAction::Cancel)
        .with_identifier_field("task_id");
    let missing = ToolDispatch::<(), ()>::execute(
        &alias,
        &(),
        new_call_id(),
        json!({"action": "query"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await;
    assert!(
        missing.is_err(),
        "fixed cancel still requires its mapped task id"
    );

    let default_jobs = SubAgentJobsTool::new(jobs);
    assert_eq!(default_jobs.name(), "subagent_jobs");
    assert_eq!(
        default_jobs.parameters_schema()["properties"]["action"]["enum"],
        json!(["query", "cancel", "close", "await"])
    );
    let default_message = SubAgentMessageTool::new(SubAgentJobRegistry::new());
    assert_eq!(default_message.name(), "subagent_message");
    assert_eq!(
        default_message.parameters_schema()["properties"]["job_id"]["type"],
        "string"
    );
}

#[test]
fn await_alias_timeout_policy_matches_its_validated_deadline() {
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_await_timeout_field("timeout_secs", 120, 3600);
    assert_eq!(
        jobs.timeout_policy(&json!({"timeout_secs": 1})),
        tinytools::ToolTimeout::Millis(1_000)
    );
    assert_eq!(
        jobs.timeout_policy(&json!({"timeout_secs": 5000})),
        tinytools::ToolTimeout::Millis(3_600_000)
    );
    assert_eq!(
        jobs.timeout_policy(&json!({"timeout_secs": "invalid"})),
        tinytools::ToolTimeout::Millis(120_000)
    );
    assert_eq!(
        SubAgentJobsTool::new(SubAgentJobRegistry::new()).timeout_policy(&json!({})),
        tinytools::ToolTimeout::Inherit
    );
}

#[test]
fn named_aliases_preserve_configured_execution_metadata() {
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new()).with_metadata(
        tinytools::PermissionLevel::Execute,
        tinytools::ToolScope::AgentOnly,
        tinytools::ToolCategory::System,
        tinytools::ToolTimeout::Unbounded,
    );
    assert_eq!(jobs.permission_level(), tinytools::PermissionLevel::Execute);
    assert_eq!(jobs.scope(), tinytools::ToolScope::AgentOnly);
    assert_eq!(jobs.category(), tinytools::ToolCategory::System);
    assert_eq!(
        jobs.timeout_policy(&json!({})),
        tinytools::ToolTimeout::Unbounded
    );

    let message = SubAgentMessageTool::new(SubAgentJobRegistry::new()).with_metadata(
        tinytools::PermissionLevel::Execute,
        tinytools::ToolScope::CliRpcOnly,
        tinytools::ToolCategory::Workflow,
        tinytools::ToolTimeout::Millis(5_000),
    );
    assert_eq!(
        message.permission_level(),
        tinytools::PermissionLevel::Execute
    );
    assert_eq!(message.scope(), tinytools::ToolScope::CliRpcOnly);
    assert_eq!(message.category(), tinytools::ToolCategory::Workflow);
    assert_eq!(
        message.timeout_policy(&json!({})),
        tinytools::ToolTimeout::Millis(5_000)
    );
}

#[tokio::test]
async fn job_alias_falls_back_to_durable_session_identifier() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, _) = jobs.create("worker", parent.instance_id());
    let alias = SubAgentJobsTool::new(jobs)
        .with_tool_name("wait_subagent")
        .with_fixed_action(SubAgentJobAction::Query)
        .with_identifier_fields("task_id", "subagent_session_id")
        .with_required_identifier();

    assert_eq!(
        alias.parameters_schema()["properties"]["task_id"]["type"],
        "string"
    );
    assert_eq!(
        alias.parameters_schema()["properties"]["subagent_session_id"]["type"],
        "string"
    );

    let result = ToolDispatch::<(), ()>::execute(
        &alias,
        &(),
        new_call_id(),
        json!({"task_id":"", "subagent_session_id":job_id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result.output()).unwrap()["id"],
        job_id.as_str()
    );
}

#[tokio::test]
async fn named_message_alias_routes_mapped_id_and_message_fields() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, steering) = jobs.create("worker", parent.instance_id());
    let schema = json!({
        "type": "object",
        "required": ["task_id", "text"],
        "properties": {
            "task_id": {"type": "string"},
            "text": {"type": "string"}
        }
    });
    let tool = SubAgentMessageTool::new(jobs)
        .with_tool_name("steer_subagent")
        .with_description("Send the worker a steering message.")
        .with_parameters(schema.clone())
        .with_argument_fields("task_id", "session_id", "text", "request_key");
    assert_eq!(tool.name(), "steer_subagent");
    assert_eq!(tool.description(), "Send the worker a steering message.");
    assert_eq!(tool.parameters_schema(), schema);

    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"task_id": job_id.as_str(), "text": "focus on the test"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("alias sends the mapped message");
    let returned: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(returned["job_id"], job_id.as_str());
    assert_eq!(returned["status"], "message_queued");
    assert_eq!(steering.pending(), 1);
}

#[tokio::test]
async fn lane_alias_routes_to_the_child_run_queue_and_default_message_stays_steering() {
    use tinyagents_harness::run_queue::{QueueLane, RunQueue};

    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    for (mode, lane) in [("steer", QueueLane::Steer), ("collect", QueueLane::Collect)] {
        let run_queue = Arc::new(RunQueue::new());
        let (job_id, steering) = jobs.create_with_cancellation_and_queue(
            "worker",
            parent.instance_id(),
            tinyagents_harness::cancel::CancellationToken::new(),
            JobLink::default(),
            Some(run_queue.clone()),
        );
        let alias = SubAgentMessageTool::new(jobs.clone())
            .with_tool_name("queue_message")
            .with_argument_fields("task_id", "session_id", "text", "request_key")
            .with_lane_field("mode");
        ToolDispatch::<(), ()>::execute(
            &alias,
            &(),
            new_call_id(),
            json!({"task_id":job_id.as_str(), "text":"follow this lane", "mode":mode}),
            tinytools::ToolCallOptions::default(),
            &parent,
        )
        .await
        .expect("lane alias enqueues a message");
        let queued = run_queue.drain_now(lane);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].text(), "follow this lane");
        assert_eq!(steering.pending(), 0, "lane alias uses the run queue");
    }

    let run_queue = Arc::new(RunQueue::new());
    let (job_id, _) = jobs.create_with_cancellation_and_queue(
        "worker",
        parent.instance_id(),
        tinyagents_harness::cancel::CancellationToken::new(),
        JobLink::default(),
        Some(run_queue.clone()),
    );
    let alias = SubAgentMessageTool::new(jobs.clone())
        .with_argument_fields("task_id", "session_id", "text", "request_key")
        .with_lane_field("mode");
    ToolDispatch::<(), ()>::execute(
        &alias,
        &(),
        new_call_id(),
        json!({"task_id":job_id.as_str(), "text":"unknown uses steer", "mode":"bogus"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("legacy unknown lane defaults to steer");
    assert_eq!(
        run_queue.drain_now(QueueLane::Steer)[0].text(),
        "unknown uses steer"
    );
    assert!(run_queue.drain_now(QueueLane::Collect).is_empty());

    let (job_id, steering) = jobs.create("worker", parent.instance_id());
    let generic = SubAgentMessageTool::new(jobs).with_argument_fields(
        "task_id",
        "session_id",
        "text",
        "request_key",
    );
    ToolDispatch::<(), ()>::execute(
        &generic,
        &(),
        new_call_id(),
        json!({"task_id":job_id.as_str(), "text":"legacy steering"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("generic message keeps its old steering path");
    assert_eq!(steering.pending(), 1);
}

#[tokio::test(start_paused = true)]
async fn bounded_await_returns_still_running_without_cancelling_and_observes_terminal_race() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let (job_id, _) = jobs.create("worker", parent.instance_id());
    let wait = SubAgentJobsTool::new(jobs.clone())
        .with_tool_name("wait_subagent")
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier()
        .with_await_timeout_field("timeout_seconds", 120, 3600);
    let waiting = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        new_call_id(),
        json!({"task_id":job_id.as_str(),"timeout_seconds":1}),
        tinytools::ToolCallOptions::default(),
        &parent,
    );
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        result = &mut waiting => panic!("job unexpectedly completed before timeout: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let still_running = waiting.await.expect("timeout is a normal job snapshot");
    let value: Value = serde_json::from_str(&still_running.output()).unwrap();
    assert_eq!(value["status"], "queued");
    assert!(jobs.holds_live_cancellation(&job_id));
    assert_eq!(
        jobs.get(job_id.as_str()).unwrap().status,
        SubAgentJobStatus::Queued
    );

    let (terminal_id, _) = jobs.create("worker", parent.instance_id());
    let terminal_wait = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        new_call_id(),
        json!({"task_id":terminal_id.as_str(),"timeout_seconds":1}),
        tinytools::ToolCallOptions::default(),
        &parent,
    );
    tokio::pin!(terminal_wait);
    tokio::select! {
        biased;
        result = &mut terminal_wait => panic!("job unexpectedly completed before timeout: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    jobs.mark_aborted(&terminal_id, true);
    let terminal = terminal_wait
        .await
        .expect("timeout race returns latest state");
    let value: Value = serde_json::from_str(&terminal.output()).unwrap();
    assert_eq!(value["status"], "failed");
}
