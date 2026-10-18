use super::*;
use serde_json::json;

struct Probe(bool);

#[async_trait]
impl Tool for Probe {
    fn name(&self) -> &str { "probe" }
    fn description(&self) -> &str { "context probe" }
    fn parameters_schema(&self) -> Value { json!({"type":"object"}) }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(self.0.to_string()))
    }
}

#[tokio::test]
async fn each_call_builds_from_its_own_context() {
    let tool = PerCallTool::new(|context| context.is_some(), Probe);
    assert_eq!(tool.execute(json!({})).await.unwrap().text(), "false");

    let run = tinyagents_harness::context::RunContext::new(
        tinyagents_harness::context::RunConfig::new("per-call"),
        (),
    );
    let context = tinyagents_harness::tool::ToolExecutionContext::from_run_context(
        &run,
        tinyagents_harness::ids::CallId::new("per-call-call"),
    );
    assert_eq!(
        tool.execute_with_context(json!({}), ToolCallOptions::default(), Some(&context))
            .await
            .unwrap()
            .text(),
        "true"
    );
}
