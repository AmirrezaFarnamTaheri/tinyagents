use super::*;
use crate::subagents::{SubAgentJobAction, SubAgentJobsTool, SubAgentMessageTool};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::CallId;
use tinyagents_harness::run_queue::{QueueLane, RunQueue};
use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::steering::{SteeringCommand, SteeringHandle};
use tinyagents_harness::tool::ToolDispatch;
use tinyagents_harness::tool::ToolSnapshot;
use tinyagents_orchestration::subagent::*;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};
use tinyinference_llm::tool::ToolCall;
use tinytools::{PermissionLevel, Tool, ToolCategory, ToolScope, ToolTimeout};

struct CloseCorrectionModel(std::sync::atomic::AtomicUsize);

#[async_trait]
impl ChatModel<()> for CloseCorrectionModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        let call = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call < 2 {
            let id = if call == 0 {
                "foreign-session"
            } else {
                "owned-session"
            };
            let mut response = ModelResponse::assistant("");
            response.message.tool_calls.push(ToolCall::new(
                format!("close-{call}"),
                "subagent_jobs",
                json!({"action": "close", "job_id": id}),
            ));
            Ok(response)
        } else {
            Ok(ModelResponse::assistant("finished"))
        }
    }
}

struct Planner;

#[async_trait]
impl SubagentPlanner<(), ()> for Planner {
    async fn prepare(
        &self,
        request: SubagentRequest<(), ()>,
    ) -> std::result::Result<PreparedSubagent<()>, SubagentError> {
        let parts = request.into_parts();
        Ok(PreparedSubagent::new(
            parts.task_key.task_id,
            "worker",
            vec![Message::user(parts.input)],
            ToolSnapshot::new(vec![]).unwrap(),
            parts.run_context,
        ))
    }
}

struct Executor;

#[async_trait]
impl SubagentExecutor<()> for Executor {
    async fn execute(
        &self,
        execution: SubagentExecution<()>,
    ) -> std::result::Result<SubagentOutcome, SubagentError> {
        Ok(SubagentOutcome::completed(
            execution.prepared.task_id,
            "driver result",
        ))
    }
}

struct BlockingExecutor {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

struct PauseThenCompleteExecutor {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

struct PanicExecutor;

#[async_trait]
impl SubagentExecutor<()> for PanicExecutor {
    async fn execute(
        &self,
        _: SubagentExecution<()>,
    ) -> std::result::Result<SubagentOutcome, SubagentError> {
        panic!("controlled executor panic")
    }
}

#[async_trait]
impl SubagentExecutor<()> for PauseThenCompleteExecutor {
    async fn execute(
        &self,
        execution: SubagentExecution<()>,
    ) -> std::result::Result<SubagentOutcome, SubagentError> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call == 0 {
            let mut outcome = SubagentOutcome::completed(execution.prepared.task_id, "waiting");
            outcome.status = SubagentOutcomeKind::AwaitingInput(SubagentPause {
                reason: "need a user answer".into(),
                resume: SubagentResume::default(),
            });
            Ok(outcome)
        } else {
            let mut outcome =
                SubagentOutcome::completed(execution.prepared.task_id, "resumed completion");
            outcome.worker_thread_id = Some("worker-thread".into());
            outcome.extensions.insert("iterations".into(), json!(3));
            Ok(outcome)
        }
    }
}

#[derive(Default)]
struct PausePersistence {
    pause: std::sync::Mutex<Option<SubagentOutcome>>,
    terminal: std::sync::Mutex<Option<SubagentOutcome>>,
}

#[derive(Clone)]
struct SingleSessionStore(Arc<std::sync::Mutex<DurableSubagentSession>>);

#[async_trait]
impl SubagentSessionStore for SingleSessionStore {
    async fn list_for_parent(
        &self,
        parent_session: &str,
        _: Option<&str>,
    ) -> std::result::Result<Vec<DurableSubagentSession>, SubagentError> {
        let session = self.0.lock().unwrap().clone();
        Ok((session.parent_session == parent_session)
            .then_some(session)
            .into_iter()
            .collect())
    }

    async fn get(
        &self,
        id: &str,
    ) -> std::result::Result<Option<DurableSubagentSession>, SubagentError> {
        let session = self.0.lock().unwrap().clone();
        Ok((session.subagent_session_id == id).then_some(session))
    }

    async fn upsert_running(
        &self,
        _: SubagentSessionUpsert,
    ) -> std::result::Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        Ok((self.0.lock().unwrap().clone(), ReuseDecision::ReusedRunning))
    }

    async fn close(&self, id: &str) -> std::result::Result<bool, SubagentError> {
        let mut session = self.0.lock().unwrap();
        if session.subagent_session_id != id {
            return Ok(false);
        }
        session.status = DurableSubagentStatus::Closed;
        session.reusable = false;
        Ok(true)
    }

    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> std::result::Result<bool, SubagentError> {
        let mut session = self.0.lock().unwrap();
        if session.subagent_session_id != id {
            return Ok(false);
        }
        session.current_task_id = Some(task_id.to_owned());
        session.status = status;
        session.reusable = status.reusable();
        session.latest_history = Some(history);
        Ok(true)
    }

    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> std::result::Result<bool, SubagentError> {
        let mut session = self.0.lock().unwrap();
        if session.subagent_session_id != id {
            return Ok(false);
        }
        session.current_task_id = Some(task_id.to_owned());
        session.status = DurableSubagentStatus::Failed;
        session.latest_error = Some(error);
        Ok(true)
    }
}

fn selector_test_session() -> DurableSubagentSession {
    DurableSubagentSession {
        subagent_session_id: "owned-session".into(),
        parent_session: "durable-parent".into(),
        parent_thread_id: None,
        worker_thread_id: None,
        agent_id: "worker".into(),
        display_name: None,
        model: None,
        sandbox_mode: "none".into(),
        action_root: None,
        task_key: "task".into(),
        lifecycle_key: None,
        task_title: "Task".into(),
        current_task_id: Some("task".into()),
        status: DurableSubagentStatus::Running,
        reusable: true,
        latest_history: None,
        latest_error: None,
        created_at: "created".into(),
        updated_at: "updated".into(),
        last_used_at: "used".into(),
    }
}

#[tokio::test]
async fn model_can_correct_foreign_durable_close_and_continue_to_owned_close() {
    let session = Arc::new(std::sync::Mutex::new(selector_test_session()));
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new())
        .with_sessions(Arc::new(SingleSessionStore(session.clone())), |_| {
            "durable-parent".into()
        })
        .recover_selector_errors();
    let model = Arc::new(CloseCorrectionModel(std::sync::atomic::AtomicUsize::new(0)));
    let mut harness = AgentHarness::new();
    harness.register_model("script", model.clone());
    harness.register_tool_dispatch(Arc::new(jobs));

    let run = harness
        .invoke(
            &(),
            (),
            RunConfig::new("selector-recovery"),
            vec![Message::user("close the session")],
        )
        .await
        .expect("the parent run should continue after correcting a foreign selector");

    assert_eq!(model.0.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(
        run.messages
            .iter()
            .any(|message| message.text().contains("finished"))
    );
    let closed = session.lock().unwrap();
    assert_eq!(closed.status, DurableSubagentStatus::Closed);
    assert!(!closed.reusable);
}

#[tokio::test]
async fn selector_misses_remain_fatal_by_default() {
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new()).with_sessions(
        Arc::new(SingleSessionStore(Arc::new(std::sync::Mutex::new(
            selector_test_session(),
        )))),
        |_| "durable-parent".into(),
    );
    let parent = RunContext::new(RunConfig::new("selector-default"), ());
    let error = ToolDispatch::<(), ()>::execute(
        &jobs,
        &(),
        CallId::new("close"),
        json!({"action": "close", "job_id": "foreign-session"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("default jobs tools retain fatal dispatch behavior");

    assert!(
        error
            .to_string()
            .contains("durable subagent session not found")
    );
}

#[test]
fn job_tool_metadata_and_default_schemas_are_preserved() {
    let jobs = SubAgentJobRegistry::new();
    let query = SubAgentJobsTool::new(jobs.clone());
    assert_eq!(query.name(), "subagent_jobs");
    let query_schema = query.parameters_schema();
    assert_eq!(query_schema["properties"]["job_id"]["type"], "string");
    assert_eq!(
        query_schema["properties"]["action"]["enum"],
        json!(["query", "cancel", "close", "await"])
    );

    let message = SubAgentMessageTool::new(jobs);
    assert_eq!(message.name(), "subagent_message");
    let message_schema = message.parameters_schema();
    for field in ["job_id", "session_id", "message", "request_id"] {
        assert_eq!(message_schema["properties"][field]["type"], "string");
    }

    let custom_schema = json!({"type":"object", "required":["task_ref"]});
    let jobs_alias = SubAgentJobsTool::new(SubAgentJobRegistry::new())
        .with_tool_name("host_jobs")
        .with_description("host query description")
        .with_parameters(custom_schema.clone())
        .with_metadata(
            PermissionLevel::Execute,
            ToolScope::CliRpcOnly,
            ToolCategory::Workflow,
            ToolTimeout::Millis(17),
        );
    assert_eq!(jobs_alias.name(), "host_jobs");
    assert_eq!(jobs_alias.description(), "host query description");
    assert_eq!(jobs_alias.parameters_schema(), custom_schema);
    assert_eq!(jobs_alias.permission_level(), PermissionLevel::Execute);
    assert_eq!(jobs_alias.scope(), ToolScope::CliRpcOnly);
    assert_eq!(jobs_alias.category(), ToolCategory::Workflow);
    assert_eq!(
        jobs_alias.timeout_policy(&json!({})),
        ToolTimeout::Millis(17)
    );

    let message_alias = SubAgentMessageTool::new(SubAgentJobRegistry::new())
        .with_tool_name("host_message")
        .with_description("host message description")
        .with_parameters(custom_schema.clone())
        .with_metadata(
            PermissionLevel::Dangerous,
            ToolScope::AgentOnly,
            ToolCategory::Workflow,
            ToolTimeout::Millis(29),
        );
    assert_eq!(message_alias.name(), "host_message");
    assert_eq!(message_alias.description(), "host message description");
    assert_eq!(message_alias.parameters_schema(), custom_schema);
    assert_eq!(message_alias.permission_level(), PermissionLevel::Dangerous);
    assert_eq!(message_alias.scope(), ToolScope::AgentOnly);
    assert_eq!(message_alias.category(), ToolCategory::Workflow);
    assert_eq!(
        message_alias.timeout_policy(&json!({})),
        ToolTimeout::Millis(29)
    );
}

#[test]
fn await_alias_timeout_uses_default_and_clamps_invalid_or_out_of_range_values() {
    assert_eq!(
        SubAgentJobsTool::new(SubAgentJobRegistry::new()).timeout_policy(&json!({})),
        ToolTimeout::Inherit,
        "an unconfigured await tool keeps the shared inherited timeout policy"
    );

    let tool = SubAgentJobsTool::new(SubAgentJobRegistry::new()).with_await_timeout_field(
        "deadline_secs",
        4,
        9,
    );
    assert_eq!(tool.timeout_policy(&json!({})), ToolTimeout::Millis(4_000));
    assert_eq!(
        tool.timeout_policy(&json!({"deadline_secs":"invalid"})),
        ToolTimeout::Millis(4_000)
    );
    assert_eq!(
        tool.timeout_policy(&json!({"deadline_secs":0})),
        ToolTimeout::Millis(1_000)
    );
    assert_eq!(
        tool.timeout_policy(&json!({"deadline_secs":100})),
        ToolTimeout::Millis(9_000)
    );
    assert_eq!(
        tool.timeout_policy(&json!({"deadline_secs":7})),
        ToolTimeout::Millis(7_000)
    );
}

#[tokio::test]
async fn fixed_query_alias_ignores_action_override_and_keeps_host_declaration() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("fixed-query-parent"), ());
    let (id, _, _, _, _) = jobs
        .register_driver_job(DriverJobRegistration {
            key: SubagentTaskKey::from_context(&parent, "fixed-query", None),
            agent: "worker".into(),
            owner: parent.instance_id().to_string(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .unwrap();
    let schema = json!({"type":"object", "required":["task_ref"]});
    let query = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Query)
        .with_identifier_field("task_ref")
        .with_tool_name("query_worker")
        .with_description("query one worker")
        .with_parameters(schema.clone())
        .with_required_identifier();
    assert_eq!(query.name(), "query_worker");
    assert_eq!(query.description(), "query one worker");
    assert_eq!(query.parameters_schema(), schema);

    let result = ToolDispatch::<(), ()>::execute(
        &query,
        &(),
        CallId::new("fixed-query-action-override"),
        json!({"task_ref": id.as_str(), "action":"cancel"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("fixed query ignores generic action input");
    assert_eq!(
        serde_json::from_str::<Value>(&result.output()).unwrap()["status"],
        "queued"
    );
    assert_eq!(
        jobs.get_owned(id.as_str(), parent.instance_id().to_string())
            .unwrap()
            .status,
        SubAgentJobStatus::Queued,
        "action=cancel was ignored by the fixed query alias"
    );
}

#[tokio::test]
async fn message_alias_routes_unique_contents_to_queue_lanes_or_default_steering() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("message-routing-parent"), ());
    let queue = Arc::new(RunQueue::<Message>::new());
    let steering = SteeringHandle::allow_all();
    let (id, steering, queue, _, _) = jobs
        .register_driver_job(DriverJobRegistration {
            key: SubagentTaskKey::from_context(&parent, "message-routing", None),
            agent: "worker".into(),
            owner: parent.instance_id().to_string(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: Some(steering),
            run_queue: Some(queue),
            continuing: false,
        })
        .unwrap();
    let lane_alias = SubAgentMessageTool::new(jobs.clone())
        .with_tool_name("send_worker_message")
        .with_argument_fields("task_ref", "session_ref", "content", "dedupe_key")
        .with_lane_field("delivery")
        .with_owner_scope(Arc::new(|run_id| run_id.to_string()));

    let send = |call_id: &'static str, args: Value| {
        ToolDispatch::<(), ()>::execute(
            &lane_alias,
            &(),
            CallId::new(call_id),
            args,
            tinytools::ToolCallOptions::default(),
            &parent,
        )
    };
    let first = send(
        "collect-once",
        json!({"task_ref":id.as_str(), "content":"collect payload", "delivery":"collect", "dedupe_key":"same"}),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&first.output()).unwrap()["status"],
        "message_queued"
    );
    let duplicate = send(
        "collect-duplicate",
        json!({"task_ref":id.as_str(), "content":"collect payload", "delivery":"collect", "dedupe_key":"same"}),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&duplicate.output()).unwrap()["duplicate"],
        true
    );
    assert_eq!(
        queue.status().await.total,
        1,
        "duplicate request enqueues once"
    );

    send(
        "steer-new",
        json!({"task_ref":id.as_str(), "content":"steer payload", "delivery":"steer", "dedupe_key":"new"}),
    )
    .await
    .unwrap();
    send(
        "unknown-lane-idless",
        json!({"task_ref":id.as_str(), "content":"unknown falls back to steer", "delivery":"future-lane"}),
    )
    .await
    .unwrap();
    let status = queue.status().await;
    assert_eq!((status.steers, status.collects, status.total), (2, 1, 3));
    let queued = queue.snapshot().await;
    assert!(queued.iter().any(|(lane, message)| {
        *lane == QueueLane::Collect && message.text() == "collect payload"
    }));
    assert!(
        queued.iter().any(|(lane, message)| {
            *lane == QueueLane::Steer && message.text() == "steer payload"
        })
    );
    assert!(queued.iter().any(|(lane, message)| {
        *lane == QueueLane::Steer && message.text() == "unknown falls back to steer"
    }));

    let default_steering = SubAgentMessageTool::new(jobs)
        .with_argument_fields("task_ref", "session_ref", "content", "dedupe_key")
        .with_owner_scope(Arc::new(|run_id| run_id.to_string()));
    ToolDispatch::<(), ()>::execute(
        &default_steering,
        &(),
        CallId::new("default-steering"),
        json!({"task_ref":id.as_str(), "content":"default steering payload"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        queue.status().await.total,
        3,
        "default message bypasses run queue"
    );
    let commands = steering.drain();
    assert_eq!(commands.len(), 1);
    assert!(matches!(
        &commands[0],
        SteeringCommand::InjectMessage(message) if message.text() == "default steering payload"
    ));
}

#[tokio::test(start_paused = true)]
async fn bounded_await_observes_terminal_state_after_deadline_before_same_future_is_polled() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let driver = blocking_driver(started.clone(), release.clone(), calls);
    let parent = RunContext::new(RunConfig::new("bounded-await-race-parent"), ());
    let task_key = SubagentTaskKey::from_context(&parent, "bounded-await-race", None);
    let request_key = task_key.clone();
    let tool = DriverSubAgentTool::new(
        "spawn_async_subagent",
        "Run the worker in the background.",
        driver,
        move |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("bounded-await-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                request_key.task_id.clone(),
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        },
        |_| Some("worker-session".into()),
    )
    .with_parameters(json!({
        "type":"object",
        "required":["agent_id","prompt"],
        "properties":{"agent_id":{"type":"string"},"prompt":{"type":"string"},"mode":{"type":"string"}}
    }))
    .with_job_registry(jobs.clone());
    let spawned = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-bounded-await-race"),
        json!({"agent_id":"worker","prompt":"finish at the deadline","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let task_id = serde_json::from_str::<Value>(&spawned.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    started.notified().await;

    let await_tool = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier()
        .with_await_timeout_field("timeout_secs", 30, 30);
    let same_wait = ToolDispatch::<(), ()>::execute(
        &await_tool,
        &(),
        CallId::new("bounded-await-race-wait"),
        json!({"task_id":task_id,"timeout_secs":1}),
        tinytools::ToolCallOptions::default(),
        &parent,
    );
    tokio::pin!(same_wait);
    tokio::select! {
        biased;
        result = &mut same_wait => panic!("child completed before bounded wait deadline: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }

    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    release.add_permits(1);
    let terminal = jobs
        .wait_owned(&task_id, parent.instance_id().to_string())
        .await
        .expect("child settles after its executor is released");
    assert_eq!(terminal.status, SubAgentJobStatus::Completed);

    let observed = same_wait.await.unwrap();
    let observed: Value = serde_json::from_str(&observed.output()).unwrap();
    assert_eq!(observed["status"], "completed");
    assert_eq!(observed["output"], "controlled completion");
}

struct FailingSessionGet;

#[async_trait]
impl SubagentSessionStore for FailingSessionGet {
    async fn list_for_parent(
        &self,
        _: &str,
        _: Option<&str>,
    ) -> std::result::Result<Vec<DurableSubagentSession>, SubagentError> {
        Ok(Vec::new())
    }

    async fn get(
        &self,
        _: &str,
    ) -> std::result::Result<Option<DurableSubagentSession>, SubagentError> {
        Err(SubagentError::Persistence("read failed".into()))
    }

    async fn upsert_running(
        &self,
        _: SubagentSessionUpsert,
    ) -> std::result::Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        unreachable!("selector test does not create sessions")
    }

    async fn close(&self, _: &str) -> std::result::Result<bool, SubagentError> {
        unreachable!("selector test does not close sessions")
    }

    async fn mark_finished(
        &self,
        _: &str,
        _: &str,
        _: DurableSubagentStatus,
        _: Vec<Message>,
    ) -> std::result::Result<bool, SubagentError> {
        unreachable!("selector test does not update sessions")
    }

    async fn mark_failed(
        &self,
        _: &str,
        _: &str,
        _: String,
    ) -> std::result::Result<bool, SubagentError> {
        unreachable!("selector test does not update sessions")
    }
}

struct FailingSessionClose(SingleSessionStore);

#[async_trait]
impl SubagentSessionStore for FailingSessionClose {
    async fn list_for_parent(
        &self,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> std::result::Result<Vec<DurableSubagentSession>, SubagentError> {
        self.0
            .list_for_parent(parent_session, parent_thread_id)
            .await
    }

    async fn get(
        &self,
        id: &str,
    ) -> std::result::Result<Option<DurableSubagentSession>, SubagentError> {
        self.0.get(id).await
    }

    async fn upsert_running(
        &self,
        upsert: SubagentSessionUpsert,
    ) -> std::result::Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        self.0.upsert_running(upsert).await
    }

    async fn close(&self, _: &str) -> std::result::Result<bool, SubagentError> {
        Err(SubagentError::Persistence("close failed".into()))
    }

    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> std::result::Result<bool, SubagentError> {
        self.0.mark_finished(id, task_id, status, history).await
    }

    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> std::result::Result<bool, SubagentError> {
        self.0.mark_failed(id, task_id, error).await
    }
}

#[tokio::test]
async fn recoverable_selector_policy_does_not_hide_session_store_errors() {
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new())
        .with_sessions(Arc::new(FailingSessionGet), |_| "durable-parent".into())
        .recover_selector_errors();
    let parent = RunContext::new(RunConfig::new("selector-store-error"), ());
    let error = ToolDispatch::<(), ()>::execute(
        &jobs,
        &(),
        CallId::new("close"),
        json!({"action": "close", "job_id": "foreign-session"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("storage failures remain fatal with selector recovery enabled");

    assert!(error.to_string().contains("read failed"));
    assert!(error.downcast_ref::<TinyAgentsError>().is_none());
}

#[tokio::test]
async fn recoverable_selector_policy_does_not_hide_close_persistence_errors() {
    let jobs = SubAgentJobsTool::new(SubAgentJobRegistry::new())
        .with_sessions(
            Arc::new(FailingSessionClose(SingleSessionStore(Arc::new(
                std::sync::Mutex::new(selector_test_session()),
            )))),
            |_| "durable-parent".into(),
        )
        .recover_selector_errors();
    let parent = RunContext::new(RunConfig::new("selector-close-error"), ());
    let error = ToolDispatch::<(), ()>::execute(
        &jobs,
        &(),
        CallId::new("close"),
        json!({"action": "close", "job_id": "owned-session"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("close persistence failures remain fatal with selector recovery enabled");

    assert!(error.to_string().contains("close failed"));
    assert!(error.downcast_ref::<TinyAgentsError>().is_none());
}

#[async_trait]
impl SubagentPersistence for PausePersistence {
    async fn load_terminal(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(self.terminal.lock().unwrap().clone())
    }

    async fn load(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentResume>, SubagentError> {
        Ok(self
            .pause
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|outcome| match &outcome.status {
                SubagentOutcomeKind::AwaitingInput(pause) => Some(pause.resume.clone()),
                _ => None,
            }))
    }

    async fn load_pause(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(self.pause.lock().unwrap().clone())
    }

    async fn save_pause(
        &self,
        pause: PersistedSubagentPause,
    ) -> std::result::Result<SubagentPausePersistenceDisposition, SubagentError> {
        let mut current = self.pause.lock().unwrap();
        let disposition = if current.is_some() {
            SubagentPausePersistenceDisposition::Replaced
        } else {
            SubagentPausePersistenceDisposition::Inserted
        };
        *current = Some(pause.outcome);
        Ok(disposition)
    }

    async fn record_terminal(
        &self,
        _: &SubagentTaskKey,
        outcome: &SubagentOutcome,
        _: Option<&SubagentResume>,
    ) -> std::result::Result<SubagentTerminalPersistenceDisposition, SubagentError> {
        *self.terminal.lock().unwrap() = Some(outcome.clone());
        Ok(SubagentTerminalPersistenceDisposition::Inserted)
    }
}

#[async_trait]
impl SubagentExecutor<()> for BlockingExecutor {
    async fn execute(
        &self,
        execution: SubagentExecution<()>,
    ) -> std::result::Result<SubagentOutcome, SubagentError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.started.notify_one();
        let task_id = execution.prepared.task_id;
        tokio::select! {
            _ = execution.cancellation.cancelled() => Err(SubagentError::Cancelled),
            permit = self.release.acquire() => {
                let permit = permit.map_err(|error| SubagentError::Execution(error.to_string()))?;
                permit.forget();
                Ok(SubagentOutcome::completed(task_id, "controlled completion"))
            }
        }
    }
}

struct Persistence;

#[async_trait]
impl SubagentPersistence for Persistence {
    async fn load_terminal(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(None)
    }
    async fn load(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentResume>, SubagentError> {
        Ok(None)
    }
    async fn load_pause(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(None)
    }
    async fn save_pause(
        &self,
        _: PersistedSubagentPause,
    ) -> std::result::Result<SubagentPausePersistenceDisposition, SubagentError> {
        Ok(SubagentPausePersistenceDisposition::Inserted)
    }
    async fn record_terminal(
        &self,
        _: &SubagentTaskKey,
        _: &SubagentOutcome,
        _: Option<&SubagentResume>,
    ) -> std::result::Result<SubagentTerminalPersistenceDisposition, SubagentError> {
        Ok(SubagentTerminalPersistenceDisposition::Inserted)
    }
}

struct TerminalReplayPersistence(SubagentOutcome);

#[async_trait]
impl SubagentPersistence for TerminalReplayPersistence {
    async fn load_terminal(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(Some(self.0.clone()))
    }
    async fn load(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentResume>, SubagentError> {
        Ok(None)
    }
    async fn load_pause(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(None)
    }
    async fn save_pause(
        &self,
        _: PersistedSubagentPause,
    ) -> std::result::Result<SubagentPausePersistenceDisposition, SubagentError> {
        Ok(SubagentPausePersistenceDisposition::Existing)
    }
    async fn record_terminal(
        &self,
        _: &SubagentTaskKey,
        _: &SubagentOutcome,
        _: Option<&SubagentResume>,
    ) -> std::result::Result<SubagentTerminalPersistenceDisposition, SubagentError> {
        Ok(SubagentTerminalPersistenceDisposition::Existing)
    }
}

#[tokio::test]
async fn background_request_without_delivery_target_runs_inline() {
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver(),
        move |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "worker-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_request| None,
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("call-1"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();

    assert!(!result.is_error);
    let value: Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["output"], "driver result");
}

#[tokio::test]
async fn background_request_with_delivery_target_returns_task_reference() {
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "worker-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("parent-thread".into()),
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("call-2"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();

    assert!(!result.is_error);
    let value: Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(value["status"], "running");
    assert_eq!(value["delivery_target"], "parent-thread");
    assert_eq!(value["task_id"], "worker-task");
}

#[tokio::test(flavor = "current_thread")]
async fn awaited_background_completion_commits_before_jobs_become_terminal() {
    let jobs = SubAgentJobRegistry::new();
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let release_callback = Arc::new(tokio::sync::Semaphore::new(0));
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "awaited-completion-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("awaited-completion-task".into()),
    )
    .with_job_registry(jobs.clone())
    .with_owner_scope(Arc::new(|_| "parent-owner".to_owned()))
    .with_async_completion_handler({
        let callback_started = callback_started.clone();
        let release_callback = release_callback.clone();
        move |_target, result| {
            assert!(result.is_ok());
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            async move {
                callback_started.notify_one();
                release_callback.acquire().await.unwrap().forget();
            }
        }
    });
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let accepted = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("call-await-completion"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let job_id = serde_json::from_str::<Value>(&accepted.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut waiter = Box::pin(jobs.wait_owned(&job_id, "parent-owner"));
    tokio::select! {
        biased;
        result = &mut waiter => panic!("wait observed terminal status before host completion commit: {result:?}"),
        _ = callback_started.notified() => {},
    }
    assert_eq!(
        jobs.get_owned(&job_id, "parent-owner").unwrap().status,
        SubAgentJobStatus::Running
    );
    release_callback.add_permits(1);
    assert_eq!(waiter.await.unwrap().status, SubAgentJobStatus::Completed);
}

#[tokio::test(flavor = "current_thread")]
async fn awaited_completion_repairs_replayed_terminal_before_jobs_become_terminal() {
    let jobs = SubAgentJobRegistry::new();
    let callback_started = Arc::new(tokio::sync::Notify::new());
    let release_callback = Arc::new(tokio::sync::Semaphore::new(0));
    let observed = Arc::new(std::sync::Mutex::new(None));
    let driver = Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(Executor)),
            persistence: Some(Arc::new(TerminalReplayPersistence(
                SubagentOutcome::completed("replayed-task", "durable result"),
            ))),
        })
        .unwrap(),
    );
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver,
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "replayed-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("replayed-task".into()),
    )
    .with_job_registry(jobs.clone())
    .with_owner_scope(Arc::new(|_| "replay-owner".to_owned()))
    .with_async_completion_handler({
        let callback_started = callback_started.clone();
        let release_callback = release_callback.clone();
        let observed = observed.clone();
        move |_target, result| {
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            let observed = observed.clone();
            async move {
                *observed.lock().unwrap() = result.ok();
                callback_started.notify_one();
                release_callback.acquire().await.unwrap().forget();
            }
        }
    });
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let accepted = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("call-replayed-completion"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let job_id = serde_json::from_str::<Value>(&accepted.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::select! {
        biased;
        result = jobs.wait_owned(&job_id, "replay-owner") => {
            panic!("replayed job became terminal before host repair: {result:?}");
        }
        _ = callback_started.notified() => {}
    }
    let result = observed.lock().unwrap().clone().unwrap();
    assert_eq!(
        result.disposition,
        SubagentPersistenceDisposition::TerminalExisting
    );
    assert!(!result.should_emit_host_effects());
    assert_eq!(result.outcome.output, "durable result");
    release_callback.add_permits(1);
    assert_eq!(
        jobs.wait_owned(&job_id, "replay-owner")
            .await
            .unwrap()
            .status,
        SubAgentJobStatus::Completed
    );
}

#[test]
fn driver_subagent_tool_preserves_host_name_description_and_schema() {
    let schema = json!({
        "type":"object",
        "required":["agent_id","prompt"],
        "properties":{"agent_id":{"type":"string"},"prompt":{"type":"string"},"mode":{"type":"string"}}
    });
    let tool = DriverSubAgentTool::new(
        "delegate_to",
        "Delegate to an allowed specialist.",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "worker-task",
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| None,
    )
    .with_parameters(schema.clone());
    let declaration = ToolDispatch::<(), ()>::tool(&tool);
    assert_eq!(declaration.name(), "delegate_to");
    assert_eq!(
        declaration.description(),
        "Delegate to an allowed specialist."
    );
    assert_eq!(declaration.parameters_schema(), schema);
}

#[tokio::test]
async fn driver_subagent_tool_routes_execution_through_the_driver() {
    let tool = DriverSubAgentTool::new(
        "subagent",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "driver-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| None,
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("driver-route"),
        json!({"input":"do work", "mode":"inline"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&result.output()).unwrap()["output"],
        "driver result"
    );
}

#[tokio::test]
async fn driver_subagent_tool_routes_detached_completion_to_host_callback() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
    let callback_tx = tx.clone();
    let tool = DriverSubAgentTool::new(
        "subagent",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "driver-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("parent-session".into()),
    )
    .with_completion_handler(move |target, result| {
        if let Some(tx) = callback_tx.lock().unwrap().take() {
            let output = result.map(|outcome| (target.to_owned(), outcome.output));
            let _ = tx.send(output);
        }
    });
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let response = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("driver-background"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&response.output()).unwrap()["status"],
        "running"
    );
    let delivered = rx.await.unwrap().unwrap();
    assert_eq!(delivered, ("parent-session".into(), "driver result".into()));
}

fn driver() -> Arc<SubagentDriver<(), ()>> {
    Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(Executor)),
            persistence: Some(Arc::new(Persistence)),
        })
        .unwrap(),
    )
}

fn blocking_driver(
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Arc<SubagentDriver<(), ()>> {
    Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(BlockingExecutor {
                started,
                release,
                calls,
            })),
            persistence: Some(Arc::new(Persistence)),
        })
        .unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn driver_alias_registry_controls_the_same_scoped_job_end_to_end() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let completions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let driver = blocking_driver(started.clone(), release.clone(), calls.clone());
    let owner_keys = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let owner_scope: Arc<dyn Fn(u64) -> String + Send + Sync> = {
        let owner_keys = owner_keys.clone();
        Arc::new(move |run_id| {
            owner_keys
                .lock()
                .unwrap()
                .get(&run_id)
                .cloned()
                .unwrap_or_else(|| format!("run:{run_id}"))
        })
    };
    let schema = json!({
        "type":"object",
        "required":["agent_id","prompt"],
        "properties":{"agent_id":{"type":"string"},"prompt":{"type":"string"},"mode":{"type":"string"}}
    });
    let tool = DriverSubAgentTool::new(
        "spawn_async_subagent",
        "Run the allowed worker in the background.",
        driver,
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("child-run"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "same-host-task-id",
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        },
        |_| Some("parent-thread".into()),
    )
    .with_completion_handler({
        let completions = completions.clone();
        move |_, _| {
            completions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    })
    .with_parameters(schema)
    .with_job_registry(jobs.clone())
    .with_owner_scope(owner_scope.clone());
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let next_turn = RunContext::new(RunConfig::new("parent-next-turn"), ());
    let foreign = RunContext::new(RunConfig::new("foreign-parent"), ());
    owner_keys.lock().unwrap().extend([
        (parent.instance_id(), "durable-parent-session".into()),
        (next_turn.instance_id(), "durable-parent-session".into()),
        (foreign.instance_id(), "unrelated-parent-session".into()),
    ]);

    let first = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn"),
        json!({"agent_id":"worker","prompt":"do work","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let task_id = serde_json::from_str::<Value>(&first.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let duplicate = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-again"),
        json!({"agent_id":"worker","prompt":"do work","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let duplicate_value = serde_json::from_str::<Value>(&duplicate.output()).unwrap();
    let duplicate_id = duplicate_value["task_id"].as_str().unwrap().to_owned();
    assert_eq!(
        duplicate_id, task_id,
        "coalesced calls reuse the control id"
    );
    started.notified().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let query = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Query)
        .with_identifier_field("task_id")
        .with_owner_scope(owner_scope.clone())
        .with_required_identifier();
    let missing_identifier = ToolDispatch::<(), ()>::execute(
        &query,
        &(),
        CallId::new("query-without-identifier"),
        json!({"task_id":null}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .expect_err("a fixed query alias requires its configured identifier");
    assert!(
        missing_identifier
            .to_string()
            .contains("identifier is required")
    );
    let listed = ToolDispatch::<(), ()>::execute(
        &query,
        &(),
        CallId::new("list"),
        json!({"task_id":task_id}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&listed.output()).unwrap()["status"],
        "running"
    );

    let list = SubAgentJobsTool::new(jobs.clone()).with_owner_scope(owner_scope.clone());
    let visible = ToolDispatch::<(), ()>::execute(
        &list,
        &(),
        CallId::new("list-stable-owner"),
        json!({}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    let visible: Value = serde_json::from_str(&visible.output()).unwrap();
    assert!(
        visible
            .as_array()
            .unwrap()
            .iter()
            .any(|row| { row.get("id").and_then(Value::as_str) == Some(task_id.as_str()) })
    );
    let hidden = ToolDispatch::<(), ()>::execute(
        &list,
        &(),
        CallId::new("list-foreign-owner"),
        json!({}),
        tinytools::ToolCallOptions::default(),
        &foreign,
    )
    .await
    .unwrap();
    assert!(
        serde_json::from_str::<Value>(&hidden.output())
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );

    let steer = SubAgentMessageTool::new(jobs.clone())
        .with_tool_name("steer_subagent")
        .with_argument_fields("task_id", "session_id", "message", "request_id")
        .with_lane_field("mode")
        .with_owner_scope(owner_scope.clone());
    let queued = ToolDispatch::<(), ()>::execute(
        &steer,
        &(),
        CallId::new("collect"),
        json!({"task_id":task_id,"message":"quiet context","mode":"collect","request_id":"collect-1"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&queued.output()).unwrap()["status"],
        "message_queued"
    );
    let duplicate = ToolDispatch::<(), ()>::execute(
        &steer,
        &(),
        CallId::new("collect-duplicate"),
        json!({"task_id":task_id,"message":"quiet context","mode":"collect","request_id":"collect-1"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&duplicate.output()).unwrap()["duplicate"],
        true
    );
    let control_id = jobs.get(&task_id).unwrap().id;

    assert!(
        ToolDispatch::<(), ()>::execute(
            &query,
            &(),
            CallId::new("foreign-query"),
            json!({"task_id":task_id}),
            tinytools::ToolCallOptions::default(),
            &foreign,
        )
        .await
        .is_err()
    );
    let other_scope = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-foreign"),
        json!({"agent_id":"worker","prompt":"do work","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &foreign,
    )
    .await
    .unwrap();
    let other_value = serde_json::from_str::<Value>(&other_scope.output()).unwrap();
    let other_id = other_value["task_id"].as_str().unwrap().to_owned();
    assert_ne!(
        other_id, task_id,
        "different task-key scope gets distinct controls"
    );
    started.notified().await;

    let wait = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier()
        .with_await_timeout_field("timeout_secs", 120, 3600)
        .with_owner_scope(owner_scope.clone());
    let pending_wait = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        CallId::new("wait"),
        json!({"task_id":task_id,"timeout_secs":1}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    );
    tokio::pin!(pending_wait);
    tokio::select! {
        biased;
        result = &mut pending_wait => panic!("job completed before the wait deadline: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    let pending = pending_wait.await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&pending.output()).unwrap()["status"],
        "running"
    );
    assert_eq!(
        jobs.get(control_id.as_str()).unwrap().status,
        SubAgentJobStatus::Running
    );

    release.add_permits(2);
    let terminal_wait = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier()
        .with_owner_scope(owner_scope.clone());
    let terminal = ToolDispatch::<(), ()>::execute(
        &terminal_wait,
        &(),
        CallId::new("collect-result"),
        json!({"task_id":task_id}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&terminal.output()).unwrap()["status"],
        "completed"
    );
    assert_eq!(
        jobs.get(control_id.as_str()).unwrap().status,
        SubAgentJobStatus::Completed
    );
    let terminal_message = SubAgentMessageTool::new(jobs.clone())
        .with_argument_fields("task_id", "session_id", "message", "request_id")
        .with_owner_scope(owner_scope.clone());
    let terminal_error = ToolDispatch::<(), ()>::execute(
        &terminal_message,
        &(),
        CallId::new("message-terminal-job"),
        json!({"task_id":task_id,"message":"continue"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .expect_err("terminal jobs cannot receive messages");
    assert!(
        matches!(
            terminal_error.downcast_ref::<SubAgentJobError>(),
            Some(SubAgentJobError::Terminal { .. })
        ),
        "unexpected terminal error: {terminal_error:#}"
    );
    let recoverable_message = terminal_message.clone().recover_selector_errors();
    let recoverable_error = ToolDispatch::<(), ()>::execute(
        &recoverable_message,
        &(),
        CallId::new("recover-terminal-job"),
        json!({"task_id":task_id,"message":"continue"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .expect_err("recoverable selector policy reports terminal jobs to the model");
    assert!(matches!(
        recoverable_error.downcast_ref::<TinyAgentsError>(),
        Some(TinyAgentsError::ModelRetry(_))
    ));
    let cancel = SubAgentJobsTool::new(jobs.clone())
        .with_identifier_field("task_id")
        .with_owner_scope(owner_scope.clone())
        .recover_selector_errors();
    let cancel_error = ToolDispatch::<(), ()>::execute(
        &cancel,
        &(),
        CallId::new("recover-terminal-cancel"),
        json!({"action":"cancel","task_id":task_id}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .expect_err("recoverable selector policy handles terminal cancel selectors");
    assert!(matches!(
        cancel_error.downcast_ref::<TinyAgentsError>(),
        Some(TinyAgentsError::ModelRetry(_))
    ));
    assert_eq!(completions.load(std::sync::atomic::Ordering::SeqCst), 2);

    let replay = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-terminal-replay"),
        json!({"agent_id":"worker","prompt":"do work","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let replay_value = serde_json::from_str::<Value>(&replay.output()).unwrap();
    assert_eq!(replay_value["task_id"], task_id);
    assert_eq!(replay_value["status"], "completed");
    assert_eq!(replay_value["output"], "controlled completion");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(completions.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn jobs_tool_treats_a_null_action_as_query() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("null-action-parent"), ());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        blocking_driver(started.clone(), release.clone(), calls),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "null-action-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("null-action-thread".into()),
    )
    .with_job_registry(jobs.clone());
    let _started = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-null-action"),
        json!({"input":"work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    started.notified().await;

    let query = SubAgentJobsTool::new(jobs);
    let listed = ToolDispatch::<(), ()>::execute(
        &query,
        &(),
        CallId::new("null-action-query"),
        json!({"action": null}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let listed: Value = serde_json::from_str(&listed.output()).unwrap();
    assert_eq!(listed.as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn oversized_message_request_ids_are_rejected_by_the_moved_shell() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("oversized-request-parent"), ());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        blocking_driver(started.clone(), release.clone(), calls),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "oversized-request-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("oversized-request-thread".into()),
    )
    .with_job_registry(jobs.clone());
    ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-oversized-request"),
        json!({"input":"work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    started.notified().await;
    let job_id = jobs
        .list()
        .into_iter()
        .next()
        .expect("background execution registered a job")
        .id
        .as_str()
        .to_owned();

    let message = SubAgentMessageTool::new(jobs).with_argument_fields(
        "task_id",
        "session_id",
        "message",
        "request_id",
    );
    let rejected = ToolDispatch::<(), ()>::execute(
        &message,
        &(),
        CallId::new("oversized-request"),
        json!({"task_id":job_id, "message":"m", "request_id":"x".repeat(129)}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await;
    let error = rejected.expect_err("oversized id is rejected before steering is queued");
    assert!(
        matches!(
            error.downcast_ref::<SubAgentJobError>(),
            Some(SubAgentJobError::RequestIdTooLong)
        ),
        "unexpected rejection: {error:#}"
    );
}

#[tokio::test]
async fn driver_alias_pause_stays_nonterminal_and_continues_on_the_same_job() {
    let jobs = SubAgentJobRegistry::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let persistence = Arc::new(PausePersistence::default());
    let parent = RunContext::new(RunConfig::new("pause-parent"), ());
    let next_turn = RunContext::new(RunConfig::new("pause-parent-next-turn"), ());
    let lifecycle_key = SubagentTaskKey::from_context(&parent, "pause-then-continue", None);
    let sessions = Arc::new(SingleSessionStore(Arc::new(std::sync::Mutex::new(
        DurableSubagentSession {
            subagent_session_id: "session-123".into(),
            parent_session: "durable-parent-session".into(),
            parent_thread_id: Some("parent-thread".into()),
            worker_thread_id: Some("worker-thread".into()),
            agent_id: "worker".into(),
            display_name: Some("Worker".into()),
            model: None,
            sandbox_mode: "none".into(),
            action_root: None,
            task_key: "pause-then-continue".into(),
            lifecycle_key: Some(lifecycle_key.clone()),
            task_title: "Pause test".into(),
            current_task_id: Some("pause-then-continue".into()),
            status: DurableSubagentStatus::Running,
            reusable: true,
            latest_history: None,
            latest_error: None,
            created_at: "created".into(),
            updated_at: "updated".into(),
            last_used_at: "used".into(),
        },
    ))));
    let owner_scope: Arc<dyn Fn(u64) -> String + Send + Sync> =
        Arc::new(|_| "durable-parent-session".into());
    let continuation_key = lifecycle_key.clone();
    let driver = Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(PauseThenCompleteExecutor {
                calls: calls.clone(),
            })),
            persistence: Some(persistence.clone()),
        })
        .unwrap(),
    );
    let schema = json!({
        "type":"object",
        "required":["agent_id","prompt"],
        "properties":{"agent_id":{"type":"string"},"prompt":{"type":"string"},"continue":{"type":"boolean"}}
    });
    let tool = DriverSubAgentTool::new(
        "spawn_async_subagent",
        "worker description",
        driver,
        move |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("child-run"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            let request = if args["continue"] == true {
                SubagentRequest::continue_with_task_key(
                    continuation_key.clone(),
                    child,
                    (),
                    args["prompt"].as_str().unwrap_or_default(),
                    Some(SubagentResume::default()),
                )
            } else {
                SubagentRequest::fresh_from_parent(
                    parent,
                    child,
                    "pause-then-continue",
                    (),
                    args["prompt"].as_str().unwrap_or_default(),
                    None,
                )
            }
            .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?
            .with_target("worker");
            Ok(request)
        },
        |_| Some("parent-thread".into()),
    )
    .with_parameters(schema)
    .with_job_registry(jobs.clone())
    .with_owner_scope(owner_scope.clone());

    let spawned = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-pausing-child"),
        json!({"agent_id":"worker","prompt":"wait","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let spawned_value = serde_json::from_str::<Value>(&spawned.output()).unwrap();
    let task_id = spawned_value["task_id"].as_str().unwrap().to_owned();
    assert_ne!(task_id, "pause-then-continue");

    let wait = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_fields("task_id", "subagent_session_id")
        .with_required_identifier()
        .with_sessions(sessions.clone(), |_| "durable-parent-session".into())
        .with_owner_scope(owner_scope.clone());
    let paused = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        CallId::new("wait-for-pause"),
        json!({"task_id":"", "subagent_session_id":"session-123"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    let paused_value = serde_json::from_str::<Value>(&paused.output()).unwrap();
    assert_eq!(paused_value["status"], "awaiting_input");
    assert!(persistence.pause.lock().unwrap().is_some());
    let public_id = jobs
        .driver_job_for_key(&lifecycle_key, "durable-parent-session")
        .unwrap();
    assert!(!jobs.get(public_id.as_str()).unwrap().status.is_terminal());
    assert_eq!(
        jobs.get(&task_id).unwrap().status,
        SubAgentJobStatus::AwaitingInput
    );

    let steer = SubAgentMessageTool::new(jobs.clone())
        .with_argument_fields("task_id", "subagent_session_id", "message", "request_id")
        .with_sessions(sessions.clone(), |_| "durable-parent-session".into())
        .with_owner_scope(owner_scope.clone());
    ToolDispatch::<(), ()>::execute(
        &steer,
        &(),
        CallId::new("steer-paused-session"),
        json!({"subagent_session_id":"session-123","message":"resume with this"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();

    let continued = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("continue-child"),
        json!({"agent_id":"worker","prompt":"answer","continue":true,"mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    let continued_value = serde_json::from_str::<Value>(&continued.output()).unwrap();
    assert_eq!(continued_value["task_id"], task_id);
    assert_eq!(continued_value["status"], "running");
    let completed = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        CallId::new("wait-for-completion"),
        json!({"task_id":"", "subagent_session_id":"session-123"}),
        tinytools::ToolCallOptions::default(),
        &next_turn,
    )
    .await
    .unwrap();
    let completed_value = serde_json::from_str::<Value>(&completed.output()).unwrap();
    assert_eq!(completed_value["status"], "completed");
    assert_eq!(completed_value["output"], "resumed completion");
    assert_eq!(completed_value["worker_thread_id"], "worker-thread");
    assert_eq!(completed_value["extensions"]["iterations"], 3);
    assert_eq!(
        completed_value["extensions"]["task_id"],
        "pause-then-continue"
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancel_driver_job_by_full_key_checks_owner_and_waits_for_settlement() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let driver = blocking_driver(started.clone(), release.clone(), calls.clone());
    let parent = RunContext::new(RunConfig::new("cancel-parent"), ());
    let key = SubagentTaskKey::from_context(&parent, "cancel-task", None);
    let request_key = key.clone();
    let owner_scope: Arc<dyn Fn(u64) -> String + Send + Sync> = Arc::new(|_| "parent-a".into());
    let tool = DriverSubAgentTool::new("spawn_async_subagent", "worker description", driver, move |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("child-run"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                request_key.task_id.clone(),
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        }, |_| Some("thread-a".into())).with_parameters(json!({
        "type":"object",
        "required":["agent_id","prompt"],
        "properties":{"agent_id":{"type":"string"},"prompt":{"type":"string"},"mode":{"type":"string"}}
    })).with_job_registry(jobs.clone()).with_owner_scope(owner_scope);
    let spawned = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-cancellable"),
        json!({"agent_id":"worker","prompt":"do work","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let public_id = serde_json::from_str::<Value>(&spawned.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .expect("driver executor should start");

    let mut foreign_key = key.clone();
    foreign_key.parent_run_id.push_str("-foreign");
    assert_eq!(
        jobs.cancel_key_owned(&foreign_key, "parent-a").unwrap_err(),
        SubAgentJobError::NotFound("cancel-task".into())
    );
    assert_eq!(
        jobs.cancel_key_owned(&key, "parent-b").unwrap_err(),
        SubAgentJobError::NotFound("cancel-task".into())
    );
    let requested = jobs.cancel_key_owned(&key, "parent-a").unwrap();
    assert_eq!(requested.id.as_str(), public_id);
    assert_eq!(requested.status, SubAgentJobStatus::Running);
    assert!(
        !parent.cancellation.is_cancelled(),
        "cancelling a child must not cancel its parent run"
    );

    release.add_permits(1);
    let settled = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        jobs.wait_owned(&public_id, "parent-a"),
    )
    .await
    .expect("cooperative cancellation should settle")
    .unwrap();
    assert_eq!(settled.status, SubAgentJobStatus::Cancelled);
    assert!(matches!(
        jobs.cancel_key_owned(&key, "parent-a"),
        Err(SubAgentJobError::Terminal { .. })
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parent_cancellation_cascades_to_running_driver_jobs() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let child_cancellation = Arc::new(std::sync::Mutex::new(None));
    let captured_child_cancellation = child_cancellation.clone();
    let parent = RunContext::new(RunConfig::new("parent-cancel-cascade"), ());
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        blocking_driver(started.clone(), release, calls.clone()),
        move |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("parent-cancel-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            *captured_child_cancellation.lock().unwrap() = Some(child.cancellation.clone());
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "parent-cancel-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("parent-cancel-thread".into()),
    )
    .with_job_registry(jobs.clone());
    let spawned = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("spawn-parent-cancel"),
        json!({"input":"work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let task_id = serde_json::from_str::<Value>(&spawned.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    started.notified().await;
    parent.cancellation.cancel();
    assert!(
        child_cancellation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    let terminal = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        jobs.wait_owned(&task_id, parent.instance_id().to_string()),
    )
    .await
    .expect("parent cancellation settles the child job")
    .unwrap();
    assert_eq!(terminal.status, SubAgentJobStatus::Cancelled);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_one_driver_job_leaves_its_sibling_running() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let parent = RunContext::new(RunConfig::new("sibling-cancel-parent"), ());
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        blocking_driver(started, release.clone(), calls.clone()),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("sibling-cancel-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                args["task_id"].as_str().unwrap_or_default(),
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_| Some("sibling-cancel-thread".into()),
    )
    .with_job_registry(jobs.clone());
    let spawn = |call_id: &'static str, task_id: &'static str| {
        ToolDispatch::<(), ()>::execute(
            &tool,
            &(),
            CallId::new(call_id),
            json!({"task_id":task_id,"input":"work","mode":"background"}),
            tinytools::ToolCallOptions::default(),
            &parent,
        )
    };
    let first = spawn("spawn-sibling-a", "sibling-a").await.unwrap();
    let second = spawn("spawn-sibling-b", "sibling-b").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both sibling executors started");
    let first_id = serde_json::from_str::<Value>(&first.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_id = serde_json::from_str::<Value>(&second.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let cancel = SubAgentJobsTool::new(jobs.clone());
    ToolDispatch::<(), ()>::execute(
        &cancel,
        &(),
        CallId::new("cancel-sibling-a"),
        json!({"action":"cancel","job_id":first_id}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let cancelled = jobs
        .wait_owned(&first_id, parent.instance_id().to_string())
        .await
        .unwrap();
    assert_eq!(cancelled.status, SubAgentJobStatus::Cancelled);
    assert_eq!(
        jobs.get_owned(&second_id, parent.instance_id().to_string())
            .unwrap()
            .status,
        SubAgentJobStatus::Running,
        "cancelling one driver job leaves its sibling running",
    );
    release.add_permits(1);
    assert_eq!(
        jobs.wait_owned(&second_id, parent.instance_id().to_string())
            .await
            .unwrap()
            .status,
        SubAgentJobStatus::Completed,
    );
}

fn inline_blocking_alias(
    jobs: SubAgentJobRegistry,
    driver: Arc<SubagentDriver<(), ()>>,
) -> impl ToolDispatch<(), ()> {
    DriverSubAgentTool::new(
        "spawn_subagent",
        "worker description",
        driver,
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("inline-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "same-inline-task",
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        },
        |_| None,
    )
    .with_parameters(json!({
        "type":"object",
        "required":["prompt"],
        "properties":{"prompt":{"type":"string"}}
    }))
    .with_job_registry(jobs)
}

#[tokio::test(start_paused = true)]
async fn concurrent_inline_driver_alias_coalesces_and_awaits_shared_result() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = inline_blocking_alias(
        jobs.clone(),
        blocking_driver(started.clone(), release.clone(), calls.clone()),
    );
    let parent = RunContext::new(RunConfig::new("inline-parent"), ());
    let dispatch = |call_id| {
        ToolDispatch::<(), ()>::execute(
            &tool,
            &(),
            CallId::new(call_id),
            json!({"prompt":"inline"}),
            tinytools::ToolCallOptions::default(),
            &parent,
        )
    };
    let mut leader = Box::pin(dispatch("inline-leader"));
    tokio::select! {
        biased;
        result = &mut leader => panic!("leader completed before being released: {result:?}"),
        _ = started.notified() => {},
    }
    let mut follower = Box::pin(dispatch("inline-follower"));
    tokio::select! {
        biased;
        result = &mut follower => panic!("coalesced inline call returned before the shared result: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    release.add_permits(1);
    let leader = leader.await.unwrap();
    let follower = follower.await.unwrap();
    for result in [leader, follower] {
        assert!(!result.is_error);
        assert_eq!(
            serde_json::from_str::<Value>(&result.output()).unwrap()["status"],
            "completed"
        );
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn immediate_inline_replay_waits_for_background_winner_and_emits_one_completion() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let completions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let completion_notified = Arc::new(tokio::sync::Notify::new());
    let driver = blocking_driver(started.clone(), release.clone(), calls.clone());
    let schema = json!({
        "type":"object",
        "required":["prompt"],
        "properties":{"prompt":{"type":"string"}}
    });
    let make_tool = |background: bool| {
        let request = |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("mixed-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "mixed-mode-task",
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        };
        let name = if background {
            "spawn_async_subagent"
        } else {
            "spawn_subagent"
        };
        let delivery = if background {
            Some("parent-thread".to_owned())
        } else {
            None
        };
        let tool = DriverSubAgentTool::new(
            name,
            "worker description",
            driver.clone(),
            request,
            move |_| delivery.clone(),
        )
        .with_parameters(schema.clone())
        .with_job_registry(jobs.clone());
        if background {
            let completions = completions.clone();
            let completion_notified = completion_notified.clone();
            tool.with_completion_handler(move |_, result| {
                assert!(result.is_ok());
                completions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                completion_notified.notify_one();
            })
        } else {
            tool
        }
    };
    let background = make_tool(true);
    let inline = make_tool(false);
    let parent = RunContext::new(RunConfig::new("mixed-parent"), ());

    let spawned = ToolDispatch::<(), ()>::execute(
        &background,
        &(),
        CallId::new("background-first"),
        json!({"prompt":"same request","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let task_id = serde_json::from_str::<Value>(&spawned.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let inline_result = ToolDispatch::<(), ()>::execute(
        &inline,
        &(),
        CallId::new("inline-immediately-after-background"),
        json!({"prompt":"same request","mode":"inline"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    );
    tokio::pin!(inline_result);
    tokio::select! {
        biased;
        result = &mut inline_result => panic!("inline alias returned before background winner finished: {result:?}"),
        _ = started.notified() => {},
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    release.add_permits(1);
    let inline_result = inline_result.await.unwrap();
    assert!(!inline_result.is_error);
    assert_eq!(
        serde_json::from_str::<Value>(&inline_result.output()).unwrap()["status"],
        "completed"
    );
    completion_notified.notified().await;
    assert_eq!(completions.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        jobs.get(&task_id).unwrap().status,
        SubAgentJobStatus::Completed
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_inline_driver_alias_marks_its_registered_job_cancelled() {
    let jobs = SubAgentJobRegistry::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = inline_blocking_alias(
        jobs.clone(),
        blocking_driver(started.clone(), release, calls),
    );
    let parent = RunContext::new(RunConfig::new("drop-parent"), ());
    let mut execution = Box::pin(ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("drop-inline"),
        json!({"prompt":"inline"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    ));
    tokio::select! {
        biased;
        result = &mut execution => panic!("child completed before drop: {result:?}"),
        _ = started.notified() => {},
    }
    drop(execution);
    let job = jobs
        .list()
        .into_iter()
        .next()
        .expect("registered job remains visible");
    assert_eq!(job.status, SubAgentJobStatus::Cancelled);
    assert_eq!(
        jobs.get(job.id.as_str()).unwrap().status,
        SubAgentJobStatus::Cancelled
    );
}

#[tokio::test]
async fn panicking_background_driver_alias_settles_and_replays_as_tool_error() {
    let jobs = SubAgentJobRegistry::new();
    let driver = Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(PanicExecutor)),
            persistence: Some(Arc::new(Persistence)),
        })
        .unwrap(),
    );
    let tool = DriverSubAgentTool::new(
        "spawn_async_subagent",
        "worker description",
        driver,
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("panic-child"), ())
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "panic-task",
                (),
                args["prompt"].as_str().unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("worker"))
        },
        |_| Some("parent-thread".into()),
    )
    .with_parameters(json!({
        "type":"object",
        "required":["prompt"],
        "properties":{"prompt":{"type":"string"}}
    }))
    .with_job_registry(jobs.clone());
    let parent = RunContext::new(RunConfig::new("panic-parent"), ());
    let spawned = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("panic-spawn"),
        json!({"prompt":"panic","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let task_id = serde_json::from_str::<Value>(&spawned.output()).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let wait = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier();
    let failed = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        CallId::new("wait-for-panic"),
        json!({"task_id":task_id}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&failed.output()).unwrap()["status"],
        "failed"
    );
    assert_eq!(
        jobs.get(&task_id).unwrap().status,
        SubAgentJobStatus::Failed
    );

    let replay = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("panic-replay"),
        json!({"prompt":"panic","mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert!(
        replay.is_error,
        "failed replay retains ToolResult error semantics"
    );
    let replay_value = serde_json::from_str::<Value>(&replay.output()).unwrap();
    assert_eq!(replay_value["status"], "failed");
}

#[tokio::test]
async fn cancelling_paused_driver_job_settles_before_wait_returns() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("paused-cancel-parent"), ());
    let (id, _, _, _, _) = jobs
        .register_driver_job(DriverJobRegistration {
            key: SubagentTaskKey::from_context(&parent, "paused-cancel", None),
            agent: "worker".into(),
            owner: parent.instance_id().to_string(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .unwrap();
    jobs.mark_running(&id);
    jobs.mark_driver_result(
        &id,
        Ok(SubagentOutcome {
            status: SubagentOutcomeKind::AwaitingInput(SubagentPause {
                reason: "awaiting user".into(),
                resume: SubagentResume::default(),
            }),
            ..SubagentOutcome::completed(id.as_str(), "question")
        }),
    );
    assert_eq!(
        jobs.get(id.as_str()).unwrap().status,
        SubAgentJobStatus::AwaitingInput
    );

    let cancel = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Cancel)
        .with_identifier_field("task_id")
        .with_required_identifier();
    let cancelled = ToolDispatch::<(), ()>::execute(
        &cancel,
        &(),
        CallId::new("cancel-paused"),
        json!({"task_id":id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&cancelled.output()).unwrap()["status"],
        "cancelled"
    );

    let wait = SubAgentJobsTool::new(jobs.clone())
        .with_fixed_action(SubAgentJobAction::Await)
        .with_identifier_field("task_id")
        .with_required_identifier();
    let waited = ToolDispatch::<(), ()>::execute(
        &wait,
        &(),
        CallId::new("wait-cancelled-pause"),
        json!({"task_id":id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&waited.output()).unwrap()["status"],
        "cancelled"
    );
    assert_eq!(
        jobs.get(id.as_str()).unwrap().status,
        SubAgentJobStatus::Cancelled
    );
}
