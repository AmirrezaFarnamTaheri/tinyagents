use super::*;
use tinytools::ToolResult;

struct Inner;

#[async_trait::async_trait]
impl Tool for Inner {
    fn name(&self) -> &str {
        "inner"
    }
    fn description(&self) -> &str {
        "inner tool"
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({"type": "object"})
    }
    fn exposure(&self) -> ToolExposure {
        ToolExposure::Deferred
    }
    fn external_effect(&self) -> bool {
        true
    }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("ran"))
    }
}

#[tokio::test]
async fn permanent_wrapper_pins_direct_exposure_and_keeps_metadata() {
    let wrapped = PermanentTool(Box::new(Inner));
    assert_eq!(wrapped.exposure(), ToolExposure::Direct);
    assert_eq!(wrapped.name(), "inner");
    assert!(wrapped.external_effect());
    let out = wrapped.execute(serde_json::json!({})).await.expect("ran");
    assert_eq!(out.text(), "ran");
}
