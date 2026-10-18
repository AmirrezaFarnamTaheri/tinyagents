//! Tool rules in the agent loop: one rule set decides the catalogue, the
//! `tool_search` results and every call, including approval, nested calls,
//! indirect targets and a hosted definition's own rules.

use super::*;

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use serde_json::json;

use crate::runtime::RunPolicy;
use crate::testkit::ScriptedModel;
use crate::tool::ToolRulePolicy;
use tinyinference_llm::model::ModelResponse;
use tinytools::{
    IndirectCall, RuleContext, Tool, ToolExposure, ToolPolicy, ToolResult, ToolRules,
    ToolSubject,
};
use tokio::sync::Notify;

// ── Helpers ─────────────────────────────────────────────────────────────────

struct RuleTool {
    name: &'static str,
    exposure: ToolExposure,
    policy: ToolPolicy,
    /// For a dispatcher: the tool its `action` argument names.
    dispatches: bool,
    seen: Mutex<Vec<Value>>,
}

impl RuleTool {
    fn new(name: &'static str) -> Arc<Self> {
        Self::build(name, ToolExposure::Direct, ToolPolicy::read_only(), false)
    }

    fn deferred(name: &'static str) -> Arc<Self> {
        Self::build(name, ToolExposure::Deferred, ToolPolicy::read_only(), false)
    }

    fn approval_gated(name: &'static str) -> Arc<Self> {
        Self::build(
            name,
            ToolExposure::Direct,
            ToolPolicy::classified().requiring_approval(),
            false,
        )
    }

    fn dispatcher(name: &'static str) -> Arc<Self> {
        Self::build(name, ToolExposure::Direct, ToolPolicy::read_only(), true)
    }

    fn build(
        name: &'static str,
        exposure: ToolExposure,
        policy: ToolPolicy,
        dispatches: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            exposure,
            policy,
            dispatches,
            seen: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

#[async_trait]
impl Tool for RuleTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "rule test tool"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    fn exposure(&self) -> ToolExposure {
        self.exposure
    }
    fn policy(&self) -> ToolPolicy {
        self.policy.clone()
    }
    fn indirect_target(&self, args: &Value) -> Option<tinytools::IndirectCall> {
        if !self.dispatches {
            return None;
        }
        args.get("action")?
            .as_str()
            .map(|name| ToolSubject::named(name).into())
    }
    async fn execute(&self, arguments: Value) -> anyhow::Result<ToolResult> {
        self.seen.lock().unwrap().push(arguments);
        Ok(ToolResult::success(format!("{} ran", self.name)))
    }
}

fn calls(calls: Vec<(&str, &str, Value)>) -> ModelResponse {
    let mut response = ModelResponse::assistant("");
    for (id, name, args) in calls {
        response
            .message
            .tool_calls
            .push(ToolCall::new(id, name, args));
    }
    response
}

fn rules(value: Value) -> ToolRules {
    serde_json::from_value(value).expect("rules parse")
}

fn harness_with(model: Arc<ScriptedModel>, policy: ToolRulePolicy) -> AgentHarness<()> {
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("scripted", model as _);
    harness.with_policy(RunPolicy {
        tool_rules: policy,
        ..RunPolicy::default()
    });
    harness
}

fn tool_text(messages: &[Message], call_id: &str) -> String {
    messages
        .iter()
        .find_map(|message| match message {
            Message::Tool(tool) if tool.tool_call_id == call_id => Some(message.text()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no result for {call_id}"))
}

fn tool_names(request: &tinyinference_llm::model::ModelRequest) -> Vec<String> {
    request.tools.iter().map(|tool| tool.name.clone()).collect()
}

struct BlockingTargetTool {
    started: Notify,
    dropped: Arc<AtomicBool>,
}

impl BlockingTargetTool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Notify::new(),
            dropped: Arc::new(AtomicBool::new(false)),
        })
    }
}

#[async_trait]
impl Tool for BlockingTargetTool {
    fn name(&self) -> &str {
        "blocked_target"
    }
    fn description(&self) -> &str {
        "test tool whose target resolver stalls"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn indirect_target_async(&self, _: &Value) -> anyhow::Result<Option<IndirectCall>> {
        struct MarkDropped(Arc<AtomicBool>);
        impl Drop for MarkDropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let _drop_marker = MarkDropped(self.dropped.clone());
        self.started.notify_one();
        std::future::pending().await
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("unexpected execution"))
    }
}

async fn run(harness: &AgentHarness<()>, name: &str) -> crate::middleware::AgentRun {
    harness
        .invoke(&(), (), RunConfig::new(name), vec![Message::user("go")])
        .await
        .expect("run completes")
}

// ── Catalogue and search ────────────────────────────────────────────────────

#[tokio::test]
async fn denied_and_hidden_tools_leave_the_catalogue_and_search() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![(
            "search",
            "tool_search",
            json!({"query": "deferred"}),
        )]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({
        "rules": [
            { "id": "no-secrets", "effect": "deny", "match": { "name": "secret_*" } },
            { "effect": "hide", "match": { "name": "quiet" } },
        ],
    })));
    let mut harness = harness_with(model.clone(), policy);
    for tool in [
        RuleTool::new("open"),
        RuleTool::new("secret_direct"),
        RuleTool::new("quiet"),
        RuleTool::deferred("deferred_open"),
        RuleTool::deferred("secret_deferred"),
    ] {
        harness.register_tool(tool);
    }

    let run = run(&harness, "catalogue").await;

    let first = &model.requests()[0];
    let names = tool_names(first);
    assert!(names.contains(&"open".to_string()), "{names:?}");
    assert!(!names.contains(&"secret_direct".to_string()), "{names:?}");
    assert!(!names.contains(&"quiet".to_string()), "{names:?}");
    let search_schema = first
        .tools
        .iter()
        .find(|tool| tool.name == "tool_search")
        .expect("deferred tools advertise the bridge");
    assert!(!search_schema.description.contains("secret_deferred"));

    let answer = tool_text(&run.messages, "search");
    assert!(answer.contains("deferred_open"), "{answer}");
    assert!(!answer.contains("secret_deferred"), "{answer}");
}

#[tokio::test]
async fn an_unconfigured_policy_changes_nothing() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::assistant("done")]));
    let mut harness = harness_with(model.clone(), ToolRulePolicy::default());
    harness.register_tool(RuleTool::new("a"));
    harness.register_tool(RuleTool::new("b"));
    run(&harness, "plain").await;
    assert_eq!(tool_names(&model.requests()[0]), ["a", "b"]);
}

#[tokio::test]
async fn indirect_target_resolution_obeys_run_cancellation() {
    let tool = BlockingTargetTool::new();
    let dropped = tool.dropped.clone();
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(tool.clone());
    harness.with_policy(RunPolicy {
        tool_rules: ToolRulePolicy::new(rules(json!({ "rules": [
            { "effect": "deny", "match": { "name": "resolved_target" } },
        ] }))),
        ..RunPolicy::default()
    });
    let cancellation = crate::cancel::CancellationToken::new();
    let ctx = RunContext::new(RunConfig::new("cancel-target-resolution"), ())
        .with_cancellation(cancellation.clone());
    harness.register_model(
        "scripted",
        Arc::new(ScriptedModel::new(vec![calls(vec![(
            "target",
            "blocked_target",
            json!({}),
        )])])) as _,
    );
    let mut run = Box::pin(harness.invoke_in_context(&(), ctx, vec![Message::user("go")]));

    tokio::select! {
        _ = tool.started.notified() => {},
        result = &mut run => panic!("run unexpectedly completed: {result:?}"),
    }
    cancellation.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut run)
        .await
        .expect("cancellation should interrupt target resolution");
    assert!(matches!(
        result,
        Err(crate::error::TinyAgentsError::Cancelled)
    ));
    assert!(
        dropped.load(Ordering::SeqCst),
        "resolver future was not dropped"
    );
}

#[tokio::test]
async fn indirect_target_resolution_obeys_run_deadline() {
    let tool = BlockingTargetTool::new();
    let dropped = tool.dropped.clone();
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(tool.clone());
    harness.with_policy(RunPolicy {
        tool_rules: ToolRulePolicy::new(rules(json!({ "rules": [
            { "effect": "deny", "match": { "name": "resolved_target" } },
        ] }))),
        ..RunPolicy::default()
    });
    harness.register_model(
        "scripted",
        Arc::new(ScriptedModel::new(vec![calls(vec![(
            "target",
            "blocked_target",
            json!({}),
        )])])) as _,
    );
    let ctx = RunContext::new(
        RunConfig::new("deadline-target-resolution").with_timeout_ms(500),
        (),
    );
    let mut run = Box::pin(harness.invoke_in_context(&(), ctx, vec![Message::user("go")]));

    tokio::select! {
        _ = tool.started.notified() => {},
        result = &mut run => panic!("run unexpectedly completed: {result:?}"),
    }
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut run)
        .await
        .expect("run deadline should interrupt target resolution");
    assert!(matches!(
        result,
        Err(crate::error::TinyAgentsError::Timeout(_))
    ));
    assert!(
        dropped.load(Ordering::SeqCst),
        "resolver future was not dropped"
    );
}

// ── Calls ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_denied_call_is_refused_with_the_rule_and_a_hidden_one_runs() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![
            ("c1", "secret_direct", json!({})),
            ("c2", "quiet", json!({})),
        ]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({
        "name": "config",
        "rules": [
            { "id": "no-secrets", "effect": "deny", "match": { "name": "secret_*" }, "reason": "secrets stay put" },
            { "effect": "hide", "match": { "name": "quiet" } },
        ],
    })));
    let secret = RuleTool::new("secret_direct");
    let quiet = RuleTool::new("quiet");
    let mut harness = harness_with(model, policy);
    harness.register_tool(secret.clone());
    harness.register_tool(quiet.clone());

    let run = run(&harness, "calls").await;

    assert_eq!(secret.calls(), 0, "a denied tool never runs");
    assert_eq!(quiet.calls(), 1, "a hidden tool stays callable");
    let refusal = tool_text(&run.messages, "c1");
    assert!(refusal.contains("rule 'no-secrets'"), "{refusal}");
    assert!(refusal.contains("secrets stay put"), "{refusal}");
    assert_eq!(run.text().as_deref(), Some("done"));
}

#[tokio::test]
async fn context_conditions_select_the_rules_that_apply() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![("c1", "shell", json!({}))]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "effect": "deny", "match": { "name": "shell" }, "when": { "channel": "telegram" } },
    ] })))
    .with_context(RuleContext::new().with("channel", "telegram"));
    let shell = RuleTool::new("shell");
    let mut harness = harness_with(model, policy);
    harness.register_tool(shell.clone());
    run(&harness, "context").await;
    assert_eq!(shell.calls(), 0);
}

#[tokio::test]
async fn an_indirect_target_is_checked_against_the_rules() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![
            ("c1", "execute", json!({"action": "GMAIL_DELETE_EMAIL"})),
            ("c2", "execute", json!({"action": "GMAIL_SEND_EMAIL"})),
        ]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "id": "no-delete", "effect": "deny", "match": { "name": "*_delete_*" } },
    ] })));
    let execute = RuleTool::dispatcher("execute");
    let mut harness = harness_with(model, policy);
    harness.register_tool(execute.clone());

    let run = run(&harness, "indirect").await;

    assert_eq!(execute.calls(), 1, "only the allowed action ran");
    assert!(tool_text(&run.messages, "c1").contains("rule 'no-delete'"));
    assert_eq!(tool_text(&run.messages, "c2"), "execute ran");
}

struct PackedRuleTool(Arc<Mutex<Vec<Value>>>);

#[async_trait]
impl Tool for PackedRuleTool {
    fn name(&self) -> &str {
        "packed_delete"
    }
    fn description(&self) -> &str {
        "packed rule fixture"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.0.lock().unwrap().push(args);
        Ok(ToolResult::success("ran"))
    }
}

struct FamilyTool(&'static str);

#[async_trait]
impl Tool for FamilyTool {
    fn name(&self) -> &str {
        "dup"
    }
    fn description(&self) -> &str {
        "same name, different family"
    }
    fn parameters_schema(&self) -> Value {
        json!({ "type": "object" })
    }
    fn family(&self) -> Option<&str> {
        Some(self.0)
    }
    async fn execute(&self, _arguments: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(self.0))
    }
}

#[tokio::test]
async fn a_toolset_tool_never_takes_a_denied_registered_tools_name() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::assistant("done")]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "effect": "deny", "match": { "family": "registered" } },
    ] })));
    let mut harness = harness_with(model.clone(), policy);
    harness.register_tool(Arc::new(FamilyTool("registered")));
    let mut extra: crate::tool::ToolRegistry<(), ()> = crate::tool::ToolRegistry::new();
    extra.register(Arc::new(FamilyTool("toolset")));
    harness.with_toolset(Arc::new(extra));

    run(&harness, "collision").await;

    assert!(!tool_names(&model.requests()[0]).contains(&"dup".to_string()));
}

#[tokio::test]
async fn normalized_arguments_are_checked_against_the_rules_again() {
    // A JSON-encoded object is decoded by normalization before dispatch; the
    // final rule check sees the decoded target.
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![(
            "c1",
            "execute",
            Value::String("{\"action\":\"GMAIL_DELETE_EMAIL\"}".to_string()),
        )]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "id": "no-delete", "effect": "deny", "match": { "name": "*_delete_*" } },
    ] })));
    let execute = RuleTool::dispatcher("execute");
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("scripted", model as _);
    harness.with_policy(RunPolicy {
        invalid_args: crate::runtime::InvalidArgsPolicy::NormalizeThenReturnToolError,
        tool_rules: policy,
        ..RunPolicy::default()
    });
    harness.register_tool(execute.clone());

    let run = run(&harness, "normalized").await;

    assert_eq!(execute.calls(), 0);
    let text = tool_text(&run.messages, "c1");
    assert!(text.contains("rule 'no-delete'"), "{text}");
}

#[tokio::test]
async fn a_require_approval_rule_on_tool_search_refuses_discovery() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![("s1", "tool_search", json!({"query": "deferred"}))]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "effect": "require_approval", "match": { "name": "tool_search" } },
    ] })));
    let mut harness = harness_with(model.clone(), policy);
    harness.register_tool(RuleTool::deferred("deferred_open"));

    let run = run(&harness, "search-approval").await;

    let answer = tool_text(&run.messages, "s1");
    assert!(answer.contains("requires approval"), "{answer}");
    assert!(
        !tool_names(&model.requests()[0]).contains(&"tool_search".to_string()),
        "an approval-gated bridge is not advertised"
    );
    assert!(!answer.contains("deferred_open"), "{answer}");
}

#[tokio::test]
async fn unknown_tool_recovery_does_not_point_at_a_denied_tool_search() {
    let model = Arc::new(ScriptedModel::new(vec![
        calls(vec![("c1", "nope", json!({}))]),
        ModelResponse::assistant("done"),
    ]));
    let policy = ToolRulePolicy::new(rules(json!({ "rules": [
        { "effect": "deny", "match": { "name": "tool_search" } },
    ] })));
    let mut harness = harness_with(model, policy);
    harness.register_tool(RuleTool::deferred("deferred_open"));

    let run = run(&harness, "unknown").await;

    assert!(!tool_text(&run.messages, "c1").contains("tool_search"));
}
