use super::*;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_definition::{AgentDefinition, InMemoryDefinitionRegistry};
use tinyinference_llm::model::ModelResponse;
use tinytools::{Tool, ToolCallOptions, ToolResult, ToolRunContext};

use crate::context::{RunConfig, RunContext};
use crate::host::{
    ContentOrigin, FixedModelResolver, GateDecision, ScreenOutcome, SecurityGate,
    StaticContextComposer, ToolCallRequest,
};
use crate::testkit::ScriptedModel;

struct PreparedTool {
    prepared: Arc<AtomicUsize>,
    executed: Arc<AtomicUsize>,
    fail_preparation: bool,
}

#[async_trait]
impl Tool for PreparedTool {
    fn name(&self) -> &str {
        "prepared"
    }

    fn description(&self) -> &str {
        "A fixture tool with host authorization facts."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object", "properties": {"command": {"type": "string"}}})
    }

    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        if self.fail_preparation {
            return Err(anyhow::anyhow!("private provider diagnostic")
                .context("command analysis is unavailable"));
        }
        Ok(Some(json!({"command": args["command"], "class": "write"})))
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        self.executed.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::success("ran"))
    }
}

#[derive(Default)]
struct RecordingGate(Mutex<Vec<(ToolCallRequest, Option<Value>)>>);

struct NestedCaller {
    failures: Arc<Mutex<Vec<String>>>,
}

struct StalledPreparation {
    started: Arc<tokio::sync::Notify>,
}

struct InjectedPreparedTool {
    prepared: Arc<Mutex<Vec<Value>>>,
    executed: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl Tool for InjectedPreparedTool {
    fn name(&self) -> &str {
        "injected_prepared"
    }

    fn description(&self) -> &str {
        "records effective arguments during preparation and execution"
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": {"type": "string"},
                "call_id": {"type": "string"}
            },
            "required": ["command", "call_id"]
        })
    }

    fn injected_arguments(&self) -> Vec<tinytools::ToolInjectedArgument> {
        vec![tinytools::ToolInjectedArgument::tool_call_id("call_id")]
    }

    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        self.prepared
            .lock()
            .expect("prepared args lock")
            .push(args.clone());
        Ok(Some(args.clone()))
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.executed.lock().expect("executed args lock").push(args);
        Ok(ToolResult::success("ran"))
    }
}

#[async_trait]
impl Tool for StalledPreparation {
    fn name(&self) -> &str {
        "stalled_preparation"
    }

    fn description(&self) -> &str {
        "waits forever while preparing authorization facts"
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }

    async fn prepare_authorization(&self, _args: &Value) -> anyhow::Result<Option<Value>> {
        self.started.notify_one();
        std::future::pending().await
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        panic!("a cancelled authorization preparation cannot execute")
    }
}

#[async_trait]
impl Tool for NestedCaller {
    fn name(&self) -> &str {
        "nested_caller"
    }

    fn description(&self) -> &str {
        "calls the prepared child tool"
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        anyhow::bail!("the harness must supply a nested-call context")
    }

    async fn execute_with_context(
        &self,
        _args: Value,
        _options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let runner = context
            .and_then(ToolRunContext::host_extension)
            .and_then(|value| value.downcast_ref::<crate::tool::ToolExecutionContext>())
            .ok_or_else(|| anyhow::anyhow!("nested-call context is missing"));
        let result = match runner {
            Ok(runner) => runner
                .call_tool("prepared", json!({"command": "nested echo"}))
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = result {
            self.failures
                .lock()
                .expect("nested failure lock")
                .push(error);
        }
        Ok(ToolResult::success("nested call attempted"))
    }
}

#[async_trait]
impl SecurityGate for RecordingGate {
    async fn authorize_tool(&self, call: &ToolCallRequest) -> crate::error::Result<GateDecision> {
        self.0.lock().expect("gate lock").push((call.clone(), None));
        Ok(GateDecision::Allow)
    }

    async fn authorize_tool_with_context(
        &self,
        call: &ToolCallRequest,
        context: Option<&Value>,
    ) -> crate::error::Result<GateDecision> {
        self.0
            .lock()
            .expect("gate lock")
            .push((call.clone(), context.cloned()));
        Ok(GateDecision::Allow)
    }

    async fn screen_input(
        &self,
        _text: &str,
        _origin: ContentOrigin,
    ) -> crate::error::Result<ScreenOutcome> {
        Ok(ScreenOutcome::Pass)
    }
}

#[tokio::test]
async fn prepared_facts_reach_the_host_gate_before_tool_execution() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "prepared",
            json!({"command": "echo ok"}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("done"),
    ]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper").with_tools(["prepared"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let prepared = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicUsize::new(0));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(Arc::new(PreparedTool {
        prepared: prepared.clone(),
        executed: executed.clone(),
        fail_preparation: false,
    }));

    harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("authorization-context"), ()),
            ),
            &(),
        )
        .await
        .expect("host-authorized call completes");

    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    let calls = gate.0.lock().expect("gate lock");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0.arguments, json!({"command": "echo ok"}));
    assert_eq!(
        calls[0]
            .1
            .as_ref()
            .map(|context| context["tool_facts"].clone()),
        Some(json!({"command": "echo ok", "class": "write"}))
    );
    assert_eq!(
        calls[0].1.as_ref().unwrap()["request_arguments"],
        calls[0].0.arguments
    );
    assert_eq!(
        calls[0].1.as_ref().unwrap()["effective_arguments"],
        calls[0].0.arguments
    );
}

#[tokio::test]
async fn preparation_failure_answers_the_call_with_its_reason_and_skips_the_host_gate() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "prepared",
            json!({"command": "echo ok"}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("recovered"),
    ]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper").with_tools(["prepared"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let prepared = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicUsize::new(0));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(Arc::new(PreparedTool {
        prepared: prepared.clone(),
        executed: executed.clone(),
        fail_preparation: true,
    }));

    // A failed preparation used to abort the whole run with a fatal
    // `Tool("tool authorization preparation failed")`.
    let run = harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("authorization-context-failure"), ()),
            ),
            &(),
        )
        .await
        .expect("a preparation failure is a per-call error, not a failed run");

    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(run.executed_tools.is_empty());
    assert!(gate.0.lock().expect("gate lock").is_empty());
    let transcript = serde_json::to_string(&run.messages).expect("transcript serializes");
    assert!(
        transcript
            .contains("tool authorization preparation failed: command analysis is unavailable"),
        "{transcript}"
    );
    assert!(!transcript.contains("private provider diagnostic"));
    // The model saw the refusal and finished its turn.
    assert!(transcript.contains("recovered"), "{transcript}");
}

#[tokio::test]
async fn nested_preparation_failure_refuses_the_child_with_its_reason() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "nested_caller",
            json!({}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("done"),
    ]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper")
                .with_tools(["nested_caller", "prepared"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let prepared = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicUsize::new(0));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_policy(RunPolicy {
        limits: crate::limits::RunLimits::default().with_max_nested_depth(1),
        ..RunPolicy::default()
    });
    let failures = Arc::new(Mutex::new(Vec::new()));
    harness.register_tool(Arc::new(NestedCaller {
        failures: failures.clone(),
    }));
    harness.register_tool(Arc::new(PreparedTool {
        prepared: prepared.clone(),
        executed: executed.clone(),
        fail_preparation: true,
    }));

    harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("authorization-context-nested-failure"), ()),
            ),
            &(),
        )
        .await
        .expect("a nested preparation failure does not fail the run");

    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(
        !gate
            .0
            .lock()
            .expect("gate lock")
            .iter()
            .any(|(request, _)| request.tool_name == "prepared")
    );
    let failures = failures.lock().expect("nested failure lock");
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0].contains(
            "nested call 'prepared' refused: tool authorization preparation failed: command analysis is unavailable"
        ),
        "{failures:?}"
    );
    assert!(!failures[0].contains("private provider diagnostic"));
}

#[tokio::test]
async fn nested_calls_prepare_and_bind_facts_before_host_authorization() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "nested_caller",
            json!({}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("done"),
    ]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper")
                .with_tools(["nested_caller", "prepared"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let prepared = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(AtomicUsize::new(0));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_policy(RunPolicy {
        limits: crate::limits::RunLimits::default().with_max_nested_depth(1),
        ..RunPolicy::default()
    });
    let failures = Arc::new(Mutex::new(Vec::new()));
    harness.register_tool(Arc::new(NestedCaller {
        failures: failures.clone(),
    }));
    harness.register_tool(Arc::new(PreparedTool {
        prepared: prepared.clone(),
        executed: executed.clone(),
        fail_preparation: false,
    }));

    harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("authorization-context-nested"), ()),
            ),
            &(),
        )
        .await
        .expect("nested host-authorized call completes");

    assert_eq!(
        *failures.lock().expect("nested failure lock"),
        Vec::<String>::new(),
        "nested call failures"
    );
    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    let calls = gate.0.lock().expect("gate lock");
    let nested = calls
        .iter()
        .find(|(request, _)| request.tool_name == "prepared")
        .expect("nested child reached the host gate");
    assert_eq!(nested.0.arguments, json!({"command": "nested echo"}));
    assert_eq!(
        nested.1.as_ref().unwrap()["tool_facts"],
        json!({"command": "nested echo", "class": "write"})
    );
}

#[tokio::test]
async fn authorization_preparation_obeys_run_cancellation() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "stalled_preparation",
            json!({}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![response]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper")
                .with_tools(["stalled_preparation"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let started = Arc::new(tokio::sync::Notify::new());
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(Arc::new(StalledPreparation {
        started: started.clone(),
    }));
    let cancellation = crate::cancel::CancellationToken::new();
    let invocation = harness.invoke_agent(
        AgentInvocation::new(
            host,
            AgentTurnRequest::new(
                "helper",
                vec![tinyinference_llm::message::Message::user("go")],
            ),
            RunContext::new(RunConfig::new("authorization-context-cancel"), ())
                .with_cancellation(cancellation.clone()),
        ),
        &(),
    );
    tokio::pin!(invocation);

    let error = tokio::select! {
        _ = started.notified() => {
            cancellation.cancel();
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut invocation)
                .await
                .expect("cancellation interrupts authorization preparation")
                .expect_err("cancelled preparation cannot complete the run")
        }
        result = &mut invocation => panic!("pending preparation unexpectedly finished: {result:?}"),
    };
    assert_eq!(error.kind, crate::runtime::HostedErrorKind::Cancelled);
    assert!(gate.0.lock().expect("gate lock").is_empty());
}

#[tokio::test]
async fn authorization_facts_use_the_effective_arguments_that_execute() {
    let raw_arguments = json!({"command": "echo safe", "call_id": "forged"});
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "injected_prepared",
            raw_arguments.clone(),
        ));
    let model = Arc::new(ScriptedModel::new(vec![
        response,
        ModelResponse::assistant("done"),
    ]));
    let gate = Arc::new(RecordingGate::default());
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper")
                .with_tools(["injected_prepared"]),
        ])),
        gate.clone(),
        Arc::new(FixedModelResolver::new(model)),
    );
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let executed = Arc::new(Mutex::new(Vec::new()));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(Arc::new(InjectedPreparedTool {
        prepared: prepared.clone(),
        executed: executed.clone(),
    }));

    harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("authorization-context-effective-args"), ()),
            ),
            &(),
        )
        .await
        .expect("effective arguments complete the call");

    let prepared = prepared.lock().expect("prepared args lock");
    let executed = executed.lock().expect("executed args lock");
    assert_eq!(prepared.len(), 1);
    assert_eq!(executed.len(), 1);
    assert_eq!(prepared[0], executed[0]);
    assert_eq!(executed[0]["call_id"], "call-1");
    assert_ne!(executed[0]["call_id"], raw_arguments["call_id"]);
    let calls = gate.0.lock().expect("gate lock");
    assert_eq!(calls[0].0.arguments, raw_arguments);
    assert_eq!(
        calls[0].1.as_ref().unwrap()["request_arguments"],
        raw_arguments
    );
    assert_eq!(
        calls[0].1.as_ref().unwrap()["effective_arguments"],
        executed[0]
    );
    assert_eq!(calls[0].1.as_ref().unwrap()["tool_facts"], executed[0]);
}
