//! Tests for tool-prepared host authorization facts.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_definition::{AgentDefinition, InMemoryDefinitionRegistry};
use tinyinference_llm::model::ModelResponse;
use tinytools::{Tool, ToolResult};

use crate::context::{RunConfig, RunContext};
use crate::host::{
    ContentOrigin, FixedModelResolver, GateDecision, ScreenOutcome, SecurityGate,
    StaticContextComposer, ToolCallRequest,
};
use crate::runtime::{AgentHarness, AgentInvocation, AgentTurnRequest};
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
            anyhow::bail!("private provider diagnostic");
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
        calls[0].1,
        Some(json!({"command": "echo ok", "class": "write"}))
    );
}

#[tokio::test]
async fn preparation_failure_blocks_before_host_gate_and_hides_provider_error() {
    let mut response = ModelResponse::assistant("");
    response
        .message
        .tool_calls
        .push(tinyinference_llm::tool::ToolCall::new(
            "call-1",
            "prepared",
            json!({"command": "echo ok"}),
        ));
    let model = Arc::new(ScriptedModel::new(vec![response]));
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

    let error = harness
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
        .expect_err("preparation failure must abort admission");

    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(gate.0.lock().expect("gate lock").is_empty());
    assert!(!error.to_string().contains("private provider diagnostic"));
}
