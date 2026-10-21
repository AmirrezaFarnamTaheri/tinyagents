use super::*;
use serde_json::json;

struct Probe(bool);

#[async_trait]
impl Tool for Probe {
    fn name(&self) -> &str {
        "probe"
    }
    fn description(&self) -> &str {
        "context probe"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
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

struct MetadataProbe;

#[async_trait]
impl Tool for MetadataProbe {
    fn name(&self) -> &str {
        "metadata_probe"
    }
    fn description(&self) -> &str {
        "prototype description"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("executed"))
    }
    fn host_call_extension(&self, args: &Value) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        Some(Box::new(args["extension"].as_str()?.to_owned()))
    }
    fn spec(&self) -> tinytools::ToolSpec {
        tinytools::ToolSpec {
            name: "curated_name".into(),
            description: "curated description".into(),
            parameters: json!({"required": ["value"]}),
        }
    }
    fn display_label(&self, args: &Value) -> Option<String> {
        Some(format!("label:{}", args["value"].as_str()?))
    }
    fn display_detail(&self, args: &Value) -> Option<String> {
        Some(format!("detail:{}", args["value"].as_str()?))
    }
    fn return_direct(&self) -> bool {
        true
    }
}

#[test]
fn forwards_host_call_and_presentation_metadata_from_the_prototype() {
    let tool = PerCallTool::new(|_| (), |_| MetadataProbe);
    let args = json!({"extension": "call-marker", "value": "result"});

    let extension = tool.host_call_extension(&args).unwrap();
    assert_eq!(extension.downcast_ref::<String>().unwrap(), "call-marker");
    let spec = tool.spec();
    assert_eq!(spec.name, "curated_name");
    assert_eq!(spec.description, "curated description");
    assert_eq!(spec.parameters, json!({"required": ["value"]}));
    assert_eq!(tool.display_label(&args).as_deref(), Some("label:result"));
    assert_eq!(tool.display_detail(&args).as_deref(), Some("detail:result"));
    assert!(tool.return_direct());
}
