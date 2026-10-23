use super::*;

use crate::context::RunConfig;
use crate::host::{
    ContentOrigin, FixedModelResolver, GateDecision, HostCapabilities, ScreenOutcome, SecurityGate,
    StaticContextComposer, ToolCallRequest,
};
use crate::runtime::{AgentInvocation, AgentTurnRequest};
use tinyagents_definition::{AgentDefinition, InMemoryDefinitionRegistry};

struct DenyToolCalls;

#[async_trait]
impl SecurityGate for DenyToolCalls {
    async fn authorize_tool(&self, _call: &ToolCallRequest) -> Result<GateDecision> {
        Ok(GateDecision::deny("host denied this execution"))
    }

    async fn screen_input(&self, _text: &str, _origin: ContentOrigin) -> Result<ScreenOutcome> {
        Ok(ScreenOutcome::Pass)
    }
}

#[tokio::test]
async fn host_authorization_denial_creates_no_execution_evidence() {
    let rig = rig(vec![]);
    let mut request = ModelResponse::assistant("");
    request.message.tool_calls = vec![packed("call-1", "patch", json!({}))];
    let model = Arc::new(ScriptedModel::new(vec![
        request,
        ModelResponse::assistant("draft"),
    ]));
    let host = HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![
            AgentDefinition::new("helper", "Helper", "test helper").with_tools([USE_SKILL]),
        ])),
        Arc::new(DenyToolCalls),
        Arc::new(FixedModelResolver::new(model)),
    );
    let run = rig
        .harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new("helper", vec![Message::user("work")]),
                RunContext::new(RunConfig::new("host-denied").with_max_model_calls(10), ()),
            ),
            &(),
        )
        .await
        .unwrap();

    assert_eq!(run.text().as_deref(), Some("draft"));
    assert!(rig.capture.0.lock().unwrap().is_empty());
    assert!(activity(&rig).tools_executed.is_empty());
    assert!(activity(&rig).tools_with_side_effects.is_empty());
    assert!(
        run.messages
            .iter()
            .any(|message| matches!(message, Message::Tool(_))
                && message.text().contains("host denied this execution"))
    );
}

struct NestedCaller;

#[async_trait]
impl Tool for NestedCaller {
    fn name(&self) -> &str {
        "caller"
    }
    fn description(&self) -> &str {
        "nested execution fixture"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        unreachable!("harness supplies the execution context")
    }

    async fn execute_with_context(
        &self,
        _args: Value,
        _options: tinytools::ToolCallOptions,
        context: Option<&dyn tinytools::ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let context = context
            .and_then(tinytools::ToolRunContext::host_extension)
            .and_then(|extension| extension.downcast_ref::<crate::tool::ToolExecutionContext>())
            .expect("harness execution context");
        Ok(
            match context
                .call_tool(
                    USE_SKILL,
                    json!({
                        "skill": "code", "tool": "patch", "args": {}
                    }),
                )
                .await
            {
                Ok(result) => result,
                Err(error) => ToolResult::error(error.to_string()),
            },
        )
    }
}

#[tokio::test]
async fn nested_calls_observe_the_bound_target_only_after_nested_gates() {
    for denied in [false, true] {
        let mut rig = rig(vec![ToolCall::new("call-1", "caller", json!({}))]);
        rig.harness.register_tool(Arc::new(NestedCaller));
        let mut policy = RunPolicy {
            limits: RunLimits::default()
                .with_max_model_calls(10)
                .with_max_nested_depth(3),
            ..RunPolicy::default()
        };
        if denied {
            policy.tool_rules = ToolRulePolicy::new(
                serde_json::from_value::<tinytools::ToolRules>(json!({
                    "rules": [{"effect": "deny", "match": {"name": "patch"}}]
                }))
                .unwrap(),
            );
        }
        rig.harness.with_policy(policy);
        let run = run(&rig).await;
        assert_eq!(
            run.text().as_deref(),
            Some(if denied { "draft" } else { "checked" })
        );
        let observations = rig.capture.0.lock().unwrap();
        assert_eq!(observations.len(), if denied { 1 } else { 2 });
        assert_eq!(observations[0].target, "caller");
        if !denied {
            assert!(observations[1].id.starts_with("call-1/"));
            assert_eq!(observations[1].outer, USE_SKILL);
            assert_eq!(observations[1].target, "patch");
        }
        assert_eq!(activity(&rig).executed_with_side_effects("patch"), !denied);
    }
}
