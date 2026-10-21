use super::*;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyinference_llm::message::Message;
use tinyinference_llm::model::ModelResponse;
use tinyinference_llm::tool::ToolCall;
use tinytools::{PermissionLevel, Tool, ToolResult};

use crate::context::RunContext;
use crate::limits::RunLimits;
use crate::middleware::{
    Middleware, MiddlewareToolOutcome, ToolHandler, ToolInvocationIdentity, ToolMiddleware,
};
use crate::runtime::{AgentHarness, RunPolicy};
use crate::testkit::ScriptedModel;
use crate::tool::ToolRulePolicy;
use tinyagents_harness_tools::skills::{
    PackCatalog, PackRegistryHandle, ToolPack, USE_SKILL, UseSkillTool,
};

const CHECK: &str = "verify observed work";
static PACKS: &[ToolPack] = &[ToolPack {
    id: "code",
    summary: "Read and patch.",
    tools: &["read", "patch", "publish"],
    owners: &[],
    guide: "",
}];

#[derive(Clone, Debug, PartialEq)]
struct Observation {
    id: String,
    outer: String,
    target: String,
    arguments: Value,
}

#[derive(Default)]
struct Capture(Mutex<Vec<Observation>>);

#[async_trait]
impl Middleware<()> for Capture {
    fn name(&self) -> &str {
        "capture_execution"
    }

    fn observe_tool_execution(
        &self,
        _ctx: &RunContext<()>,
        _state: &(),
        invocation: &ToolInvocationIdentity,
        target: &dyn Tool,
        arguments: &Value,
    ) {
        self.0.lock().unwrap().push(Observation {
            id: invocation.call_id().as_str().to_owned(),
            outer: invocation.tool_name().to_owned(),
            target: target.name().to_owned(),
            arguments: arguments.clone(),
        });
    }
}

struct CodeTool(&'static str);

#[async_trait]
impl Tool for CodeTool {
    fn name(&self) -> &str {
        self.0
    }

    fn description(&self) -> &str {
        "execution observation fixture"
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        if self.0 == "patch" && args.get("dry_run") != Some(&Value::Bool(true)) {
            PermissionLevel::Write
        } else {
            PermissionLevel::ReadOnly
        }
    }

    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.0 == "publish" && args.get("dry_run") != Some(&Value::Bool(true))
    }

    fn is_concurrency_safe(&self, _args: &Value) -> bool {
        true
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        if args.get("fail") == Some(&Value::Bool(true)) {
            Ok(ToolResult::error(
                "attempt failed after entering the executor",
            ))
        } else {
            Ok(ToolResult::success(format!("{}:{args}", self.0)))
        }
    }
}

struct Rig {
    harness: AgentHarness<()>,
    capture: Arc<Capture>,
    activities: Arc<Mutex<Vec<FinishActivity>>>,
    _packed: Arc<Vec<Box<dyn Tool>>>,
}

fn rig(calls: Vec<ToolCall>) -> Rig {
    let mut response = ModelResponse::assistant("");
    response.message.tool_calls = calls;
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("draft"),
        ModelResponse::assistant("checked"),
    ]));
    let handle = PackRegistryHandle::default();
    let packed: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![
        Box::new(CodeTool("read")),
        Box::new(CodeTool("patch")),
        Box::new(CodeTool("publish")),
    ]);
    handle.bind(Arc::downgrade(&packed));
    let mut harness = AgentHarness::new();
    harness.register_model("mock", model);
    harness.register_tool(Arc::new(UseSkillTool::new(
        handle,
        PackCatalog::new(PACKS, "[missing]"),
    )));
    for name in ["read", "patch", "publish"] {
        harness.register_tool(Arc::new(CodeTool(name)));
    }
    harness.with_policy(RunPolicy {
        limits: RunLimits::default().with_max_model_calls(10),
        ..RunPolicy::default()
    });
    let capture = Arc::new(Capture::default());
    let activities: Arc<Mutex<Vec<FinishActivity>>> = Arc::default();
    let seen = activities.clone();
    harness.push_middleware(capture.clone());
    harness.push_middleware(Arc::new(
        VerifyBeforeFinishMiddleware::new(CHECK).with_trigger(move |activity| {
            seen.lock().unwrap().push(activity.clone());
            activity.executed_with_side_effects("patch")
                || activity.executed_with_side_effects("publish")
        }),
    ));
    Rig {
        harness,
        capture,
        activities,
        _packed: packed,
    }
}

fn packed(id: &str, tool: &str, arguments: Value) -> ToolCall {
    ToolCall::new(
        id,
        USE_SKILL,
        json!({"skill": "code", "tool": tool, "args": arguments}),
    )
}

async fn run(rig: &Rig) -> AgentRun {
    rig.harness
        .invoke_default(&(), vec![Message::user("do work")])
        .await
        .unwrap()
}

fn activity(rig: &Rig) -> FinishActivity {
    rig.activities.lock().unwrap().first().unwrap().clone()
}

#[tokio::test]
async fn packed_execution_records_the_bound_target_and_keeps_transcript_identity() {
    let rig = rig(vec![packed("call-1", "patch", json!({}))]);
    let run = run(&rig).await;
    assert_eq!(run.text().as_deref(), Some("checked"));
    assert_eq!(run.executed_tools, [USE_SKILL]);
    assert!(run.messages.iter().any(|message| matches!(message,
        Message::Assistant(assistant) if assistant.tool_calls.iter().any(|call|
            call.id == "call-1" && call.name == USE_SKILL))));
    let activity = activity(&rig);
    assert!(activity.called(USE_SKILL));
    assert!(!activity.called("patch"));
    assert!(activity.executed("patch"));
    assert!(activity.executed_with_side_effects("patch"));
    assert_eq!(
        *rig.capture.0.lock().unwrap(),
        [Observation {
            id: "call-1".to_owned(),
            outer: USE_SKILL.to_owned(),
            target: "patch".to_owned(),
            arguments: json!({}),
        }]
    );
}

#[tokio::test]
async fn previews_execute_without_mutation_evidence_and_failed_writes_still_need_checking() {
    for (arguments, needs_check) in [
        (json!({"dry_run": true}), false),
        (json!({"dry_run": false}), true),
        (json!({"dry_run": "true"}), true),
        (json!({}), true),
        (json!({"fail": true}), true),
    ] {
        let rig = rig(vec![packed("call-1", "patch", arguments)]);
        let run = run(&rig).await;
        assert!(activity(&rig).executed("patch"));
        assert_eq!(
            activity(&rig).executed_with_side_effects("patch"),
            needs_check
        );
        assert_eq!(
            run.text().as_deref(),
            Some(if needs_check { "checked" } else { "draft" })
        );
    }
}

#[tokio::test]
async fn external_effects_are_observed_even_with_read_only_permission() {
    let rig = rig(vec![packed("call-1", "publish", json!({}))]);
    assert_eq!(run(&rig).await.text().as_deref(), Some("checked"));
    assert!(activity(&rig).executed_with_side_effects("publish"));
}

#[tokio::test]
async fn disclosure_missing_tools_and_spoofed_arguments_do_not_create_target_evidence() {
    for call in [
        ToolCall::new(
            "call-1",
            USE_SKILL,
            json!({"skill": "code", "args": {"tool": "patch"}}),
        ),
        packed("call-1", "missing", json!({"tools_executed": ["patch"]})),
        ToolCall::new("call-1", "missing", json!({"tool": "patch"})),
    ] {
        let rig = rig(vec![call]);
        assert_eq!(run(&rig).await.text().as_deref(), Some("draft"));
        assert!(!activity(&rig).executed("patch"));
        assert!(!activity(&rig).executed_with_side_effects("patch"));
        assert!(
            activity(&rig)
                .tools_with_side_effects
                .iter()
                .all(|name| name == USE_SKILL)
        );
    }
}

struct RetargetBefore;

#[async_trait]
impl Middleware<()> for RetargetBefore {
    fn name(&self) -> &str {
        "retarget_before"
    }

    async fn before_tool(
        &self,
        _ctx: &mut RunContext<()>,
        _state: &(),
        call: &mut ToolCall,
    ) -> Result<()> {
        call.arguments["tool"] = json!("read");
        Ok(())
    }
}

#[tokio::test]
async fn before_tool_rewrites_are_observed_after_binding() {
    let mut rig = rig(vec![packed("call-1", "patch", json!({}))]);
    rig.harness.push_middleware(Arc::new(RetargetBefore));
    assert_eq!(run(&rig).await.text().as_deref(), Some("draft"));
    assert!(activity(&rig).executed("read"));
    assert!(!activity(&rig).executed("patch"));
}

enum WrapMode {
    ShortCircuit,
    Rewrite,
    Retry,
}

struct Wrap(WrapMode);

#[async_trait]
impl ToolMiddleware<()> for Wrap {
    fn name(&self) -> &str {
        "test_wrap"
    }

    async fn wrap_tool(
        &self,
        ctx: &RunContext<()>,
        state: &(),
        mut call: ToolCall,
        next: ToolHandler<'_, (), ()>,
    ) -> Result<MiddlewareToolOutcome> {
        match self.0 {
            WrapMode::ShortCircuit => return Ok(ToolResult::success("cached").into()),
            WrapMode::Rewrite => {
                call.id = "forged-id".to_owned();
                call.name = "forged-name".to_owned();
                call.arguments =
                    json!({"skill": "code", "tool": "read", "args": {"dry_run": true}});
            }
            WrapMode::Retry => {
                next.run(ctx, state, call.clone()).await?;
            }
        }
        next.run(ctx, state, call).await
    }
}

#[tokio::test]
async fn wrappers_cannot_replace_the_observed_pinned_target_or_outer_identity() {
    let mut rig = rig(vec![packed(
        "call-1",
        "patch",
        json!({"marker": "admitted"}),
    )]);
    rig.harness
        .push_tool_middleware(Arc::new(Wrap(WrapMode::Rewrite)));
    assert_eq!(run(&rig).await.text().as_deref(), Some("checked"));
    assert_eq!(
        *rig.capture.0.lock().unwrap(),
        [Observation {
            id: "call-1".to_owned(),
            outer: USE_SKILL.to_owned(),
            target: "patch".to_owned(),
            arguments: json!({"marker": "admitted"}),
        }]
    );
}

#[tokio::test]
async fn rule_refusals_and_short_circuited_wrappers_produce_no_execution_evidence() {
    for denied in [true, false] {
        let mut rig = rig(vec![packed("call-1", "patch", json!({}))]);
        if denied {
            rig.harness.with_policy(RunPolicy {
                tool_rules: ToolRulePolicy::new(
                    serde_json::from_value::<tinytools::ToolRules>(json!({
                        "rules": [{"effect": "deny", "match": {"name": "patch"}}]
                    }))
                    .unwrap(),
                ),
                ..RunPolicy::default()
            });
        } else {
            rig.harness
                .push_tool_middleware(Arc::new(Wrap(WrapMode::ShortCircuit)));
        }
        assert_eq!(run(&rig).await.text().as_deref(), Some("draft"));
        assert!(rig.capture.0.lock().unwrap().is_empty());
        assert!(activity(&rig).tools_executed.is_empty());
        assert!(activity(&rig).tools_with_side_effects.is_empty());
    }
}

#[tokio::test]
async fn retries_observe_each_real_attempt_but_finish_activity_is_a_set() {
    let mut rig = rig(vec![packed("call-1", "patch", json!({}))]);
    rig.harness
        .push_tool_middleware(Arc::new(Wrap(WrapMode::Retry)));
    assert_eq!(run(&rig).await.text().as_deref(), Some("checked"));
    let observations = rig.capture.0.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0], observations[1]);
    assert_eq!(activity(&rig).tools_executed.len(), 1);
}

#[tokio::test]
async fn parallel_calls_keep_targets_and_effective_arguments_separate() {
    let rig = rig(vec![
        ToolCall::new("read-id", "read", json!({"tool": "patch"})),
        ToolCall::new("preview-id", "patch", json!({"dry_run": true})),
        ToolCall::new("write-id", "patch", json!({"dry_run": false})),
    ]);
    assert_eq!(run(&rig).await.text().as_deref(), Some("checked"));
    let mut observations = rig.capture.0.lock().unwrap().clone();
    observations.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(
        observations
            .iter()
            .map(|item| (item.id.as_str(), item.target.as_str()))
            .collect::<Vec<_>>(),
        [
            ("preview-id", "patch"),
            ("read-id", "read"),
            ("write-id", "patch")
        ]
    );
    assert_eq!(observations[0].arguments, json!({"dry_run": true}));
    assert_eq!(observations[2].arguments, json!({"dry_run": false}));
    assert!(activity(&rig).executed_with_side_effects("patch"));
    assert!(!activity(&rig).executed_with_side_effects("read"));
}

#[test]
fn legacy_finish_state_does_not_invent_execution_evidence() {
    let activity: FinishActivity = serde_json::from_value(json!({
        "tool_rounds": 3, "tools_called": ["patch", "use_skill"]
    }))
    .unwrap();
    assert!(activity.called("patch"));
    assert!(activity.tools_executed.is_empty());
    assert!(activity.tools_with_side_effects.is_empty());
    assert_eq!(
        serde_json::from_value::<FinishActivity>(serde_json::to_value(&activity).unwrap()).unwrap(),
        activity
    );
}

struct PendingWrap(Arc<tokio::sync::Notify>);

#[async_trait]
impl ToolMiddleware<()> for PendingWrap {
    fn name(&self) -> &str {
        "pending_before_executor"
    }

    async fn wrap_tool(
        &self,
        _ctx: &RunContext<()>,
        _state: &(),
        _call: ToolCall,
        _next: ToolHandler<'_, (), ()>,
    ) -> Result<MiddlewareToolOutcome> {
        self.0.notify_one();
        std::future::pending().await
    }
}

struct PendingTool(Arc<tokio::sync::Notify>);

#[async_trait]
impl Tool for PendingTool {
    fn name(&self) -> &str {
        "patch"
    }
    fn description(&self) -> &str {
        "pending inside executor"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Write
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        self.0.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancellation_before_executor_entry_creates_no_observation() {
    let mut rig = rig(vec![packed("call-1", "patch", json!({}))]);
    let started = Arc::new(tokio::sync::Notify::new());
    rig.harness
        .push_tool_middleware(Arc::new(PendingWrap(started.clone())));
    let mut running = Box::pin(rig.harness.invoke_default(&(), vec![Message::user("work")]));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! {
            result = &mut running => panic!("wrapper returned: {result:?}"),
            () = started.notified() => {}
        }
    })
    .await
    .unwrap();
    drop(running);
    assert!(rig.capture.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_after_executor_entry_retains_the_started_attempt() {
    let mut rig = rig(vec![ToolCall::new("call-1", "patch", json!({}))]);
    let started = Arc::new(tokio::sync::Notify::new());
    rig.harness
        .register_tool(Arc::new(PendingTool(started.clone())));
    let mut running = Box::pin(rig.harness.invoke_default(&(), vec![Message::user("work")]));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! {
            result = &mut running => panic!("executor returned: {result:?}"),
            () = started.notified() => {}
        }
    })
    .await
    .unwrap();
    drop(running);
    let observations = rig.capture.0.lock().unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].target, "patch");
}

#[path = "middleware_execution_boundary_tests.rs"]
mod boundary_tests;
