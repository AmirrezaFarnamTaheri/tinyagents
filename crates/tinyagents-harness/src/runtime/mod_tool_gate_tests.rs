//! Definition tool denials under a hosted wildcard scope.

use std::sync::Arc;

use tinyagents_definition::{AgentDefinition, InMemoryDefinitionRegistry};
use tinyinference_llm::model::ModelResponse;

use crate::context::{RunConfig, RunContext};
use crate::host::{AllowAllSecurityGate, FixedModelResolver, StaticContextComposer};
use crate::runtime::{AgentHarness, AgentInvocation, AgentTurnRequest};
use crate::testkit::{FakeTool, ScriptedModel};

// A glob denial must hold under a wildcard scope, as it does for a Named scope.
#[tokio::test]
async fn hosted_wildcard_glob_denial_hides_the_matching_tool() {
    let model = Arc::new(ScriptedModel::new(vec![ModelResponse::assistant("done")]));
    let mut definition = AgentDefinition::new("helper", "Helper", "test helper");
    definition.disallowed_tools = vec!["ans*".into()];
    let host = crate::host::HostCapabilities::new(
        Arc::new(StaticContextComposer::empty()),
        Arc::new(InMemoryDefinitionRegistry::new(vec![definition])),
        Arc::new(AllowAllSecurityGate),
        Arc::new(FixedModelResolver::new(model.clone())),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_tool(Arc::new(FakeTool::returning("noop", "ok")));
    harness.register_tool(Arc::new(FakeTool::returning("answer", "ok")));

    harness
        .invoke_agent(
            AgentInvocation::new(
                host,
                AgentTurnRequest::new(
                    "helper",
                    vec![tinyinference_llm::message::Message::user("go")],
                ),
                RunContext::new(RunConfig::new("glob-deny"), ()),
            ),
            &(),
        )
        .await
        .expect("run");

    assert_eq!(
        model.requests()[0]
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["noop"],
        "`ans*` denial was ignored under the wildcard scope"
    );
}
