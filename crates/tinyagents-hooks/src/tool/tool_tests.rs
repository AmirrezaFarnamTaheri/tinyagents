use super::*;
use crate::command::HookOutput;
use std::sync::{Arc, Mutex};
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::middleware::{Middleware, ToolInvocationIdentity};
use tinytools::{ToolContent, ToolResult};

#[derive(Clone)]
struct Host {
    session: &'static str,
}
impl HookIdentity for Host {
    fn session_id(&self) -> Option<String> {
        Some(self.session.into())
    }
    fn agent_id(&self) -> Option<String> {
        Some("agent".into())
    }
    fn cwd(&self) -> Option<std::path::PathBuf> {
        None
    }
}

struct TestHook {
    decision: HookOutput,
    observed: Arc<Mutex<Vec<ToolHookContext>>>,
}
#[async_trait]
impl ToolHook<Host> for TestHook {
    fn name(&self) -> &str {
        "test"
    }
    async fn before_tool_decision(
        &self,
        _host: &Host,
        context: &ToolHookContext,
    ) -> ToolHookDecision {
        self.observed.lock().unwrap().push(context.clone());
        self.decision.clone()
    }
    async fn after_tool_context(&self, _host: &Host, context: &ToolHookContext) -> Option<String> {
        self.observed.lock().unwrap().push(context.clone());
        Some("after context".into())
    }
}

fn middleware(
    decision: HookOutput,
    observed: Arc<Mutex<Vec<ToolHookContext>>>,
) -> ToolHooksMiddleware<Host> {
    ToolHooksMiddleware::new(vec![Arc::new(TestHook { decision, observed })])
}
fn run(session: &'static str) -> RunContext<Host> {
    RunContext::new(RunConfig::new("test"), Host { session })
}
fn call(id: &str) -> tinyinference_llm::tool::ToolCall {
    tinyinference_llm::tool::ToolCall::new(id, "read", serde_json::json!({"path":"secret"}))
}

struct NestedCaller {
    result: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl tinytools::Tool for NestedCaller {
    fn name(&self) -> &str {
        "caller"
    }
    fn description(&self) -> &str {
        "calls a nested tool"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }
    async fn execute(&self, _: serde_json::Value) -> anyhow::Result<ToolResult> {
        unreachable!()
    }
    async fn execute_with_context(
        &self,
        _: serde_json::Value,
        _: tinytools::ToolCallOptions,
        context: Option<&dyn tinytools::ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let extension = context
            .and_then(tinytools::ToolRunContext::host_extension)
            .and_then(|value| {
                value.downcast_ref::<tinyagents_harness::tool::ToolExecutionContext>()
            })
            .expect("harness nested-call context");
        let result = extension.call_tool("secret", serde_json::json!({})).await;
        *self.result.lock().unwrap() = Some(
            result
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "allowed".into()),
        );
        Ok(ToolResult::success("caller completed"))
    }
}

struct SecretTool(Arc<Mutex<bool>>);

#[async_trait]
impl tinytools::Tool for SecretTool {
    fn name(&self) -> &str {
        "secret"
    }
    fn description(&self) -> &str {
        "must be denied when called nested"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }
    async fn execute(&self, _: serde_json::Value) -> anyhow::Result<ToolResult> {
        *self.0.lock().unwrap() = true;
        Ok(ToolResult::success("secret ran"))
    }
}

struct DenySecretHook;

#[async_trait]
impl ToolHook<Host> for DenySecretHook {
    fn name(&self) -> &str {
        "deny_secret"
    }
    async fn before_tool_decision(&self, _: &Host, context: &ToolHookContext) -> ToolHookDecision {
        if context.tool_name == "secret" {
            HookOutput::deny("secret blocked")
        } else {
            HookOutput::default()
        }
    }
}

#[tokio::test]
async fn nested_tool_calls_cannot_bypass_hook_denials() {
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        runtime::AgentHarness,
    };
    use tinyinference_llm::{model::ModelResponse, providers::MockModel};

    let host = Host {
        session: "nested-hooks",
    };
    let ran_secret = Arc::new(Mutex::new(false));
    let nested_result = Arc::new(Mutex::new(None));
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "parent-call",
            "caller",
            serde_json::json!({}),
        ));
    let mut harness: AgentHarness<(), Host> = AgentHarness::new();
    harness.register_model(
        "mock",
        Arc::new(MockModel::with_responses(vec![
            response,
            ModelResponse::assistant("done"),
        ])),
    );
    harness.set_default_model("mock");
    harness.register_tool(Arc::new(NestedCaller {
        result: Arc::clone(&nested_result),
    }));
    harness.register_tool(Arc::new(SecretTool(Arc::clone(&ran_secret))));
    harness.push_middleware(Arc::new(ToolHooksMiddleware::new(vec![Arc::new(
        DenySecretHook,
    )])));
    harness
        .invoke_in_context(
            &host,
            RunContext::new(RunConfig::new("nested-hook-test"), host),
            vec![tinyinference_llm::message::Message::user("go")],
        )
        .await
        .expect("turn completes after nested refusal");

    assert!(
        !*ran_secret.lock().unwrap(),
        "denied nested tool body must not run"
    );
    assert!(
        nested_result
            .lock()
            .unwrap()
            .as_deref()
            .unwrap()
            .contains("secret blocked")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn command_engine_fires_generic_and_derived_shell_events() {
    use crate::command::{HookEngine, config, environment::HookEnvironment};
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("hooks.json"),
        r#"{"version":1,"hooks":{"preToolUse":[{"command":"./generic.sh"}],"beforeShellExecution":[{"command":"./shell.sh"}]}}"#,
    ).expect("hooks config");
    for (name, message) in [("generic.sh", "generic"), ("shell.sh", "shell-specialized")] {
        let path = dir.path().join(name);
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho '{{\"additional_context\":\"{message}\"}}'\n"),
        )
        .expect("script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    let engine = HookEngine::new(HookEnvironment::new("Test", None));
    engine
        .install(config::load(engine.environment(), None, Some(dir.path())))
        .await;
    let context = ToolHookContext {
        event: ToolHookEvent::PreToolUse,
        call_id: "call-1".into(),
        tool_name: "shell".into(),
        arguments: serde_json::json!({"command":"pwd"}),
        success: None,
        duration_ms: None,
        output: None,
        error: None,
        session_id: Some("session".into()),
        agent_id: Some("agent".into()),
        cwd: None,
    };
    let output = engine
        .dispatch_tool_hooks(
            &context,
            crate::command::context::TurnIdentity {
                session_id: Some("session".into()),
                ..Default::default()
            },
            true,
        )
        .await;
    assert_eq!(
        output.additional_context.as_deref(),
        Some("generic\nshell-specialized")
    );
}

#[test]
fn successful_output_summary_keeps_shape_and_discards_contents() {
    let summary = sanitize_tool_output("secret-token-value", "read_file", true);
    assert_eq!(summary, "read_file: ok (18 chars)");
    assert!(!summary.contains("secret-token-value"));
}

#[test]
fn failed_output_summary_keeps_only_a_classification() {
    let summary = sanitize_tool_output("permission denied for /private/path", "shell", false);
    assert_eq!(summary, "shell: failed (permission_denied)");
    assert!(!summary.contains("/private/path"));
}

#[tokio::test]
async fn before_hook_can_rewrite_input_and_append_context_together() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let middleware = middleware(
        HookOutput {
            updated_input: Some(serde_json::json!({"safe":true})),
            additional_context: Some("before context".into()),
            ..Default::default()
        },
        Arc::clone(&seen),
    );
    let mut context = run("one");
    let mut tool_call = call("same");
    Middleware::before_tool(&middleware, &mut context, &(), &mut tool_call)
        .await
        .unwrap();
    assert_eq!(tool_call.arguments, serde_json::json!({"safe":true}));
    let identity = ToolInvocationIdentity::new("same", "read");
    let mut result = ToolResult::success("secret output");
    Middleware::after_tool(&middleware, &mut context, &(), &identity, &mut result)
        .await
        .unwrap();
    let texts: Vec<_> = result
        .content
        .iter()
        .filter_map(|item| match item {
            ToolContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(texts, ["secret output", "before context", "after context"]);
    assert_eq!(
        seen.lock().unwrap()[1].arguments,
        serde_json::json!({"safe":true})
    );
}

#[tokio::test]
async fn deny_and_ask_both_refuse_calls() {
    for decision in [
        HookOutput::deny("blocked"),
        HookOutput {
            permission: Some(crate::command::HookPermission::Ask),
            agent_message: Some("approval".into()),
            ..Default::default()
        },
    ] {
        let middleware = middleware(decision, Arc::default());
        let mut context = run("one");
        assert!(
            Middleware::before_tool(&middleware, &mut context, &(), &mut call("id"))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn duplicate_call_ids_are_isolated_by_run_context() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let middleware = middleware(HookOutput::default(), Arc::clone(&seen));
    let mut first = run("first");
    let mut second = run("second");
    let mut first_call = call("same");
    let mut second_call = call("same");
    Middleware::before_tool(&middleware, &mut first, &(), &mut first_call)
        .await
        .unwrap();
    Middleware::before_tool(&middleware, &mut second, &(), &mut second_call)
        .await
        .unwrap();
    let first_id = ToolInvocationIdentity::new("same", "read");
    let second_id = ToolInvocationIdentity::new("same", "read");
    let mut result = ToolResult::success("ok");
    Middleware::after_tool(&middleware, &mut first, &(), &first_id, &mut result)
        .await
        .unwrap();
    Middleware::after_tool(&middleware, &mut second, &(), &second_id, &mut result)
        .await
        .unwrap();
    let observed = seen.lock().unwrap();
    assert_eq!(observed[2].session_id.as_deref(), Some("first"));
    assert_eq!(observed[3].session_id.as_deref(), Some("second"));
}

#[test]
fn command_decision_vocabulary_keeps_input_rewrite_and_context_together() {
    let decision: ToolHookDecision = crate::command::HookOutput {
        updated_input: Some(serde_json::json!({"safe": true})),
        additional_context: Some("remember this".into()),
        ..Default::default()
    };
    assert_eq!(
        decision.updated_input,
        Some(serde_json::json!({"safe": true}))
    );
    assert_eq!(
        decision.additional_context.as_deref(),
        Some("remember this")
    );
}
