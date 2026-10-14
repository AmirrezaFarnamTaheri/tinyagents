use super::*;

struct Fixed;

impl PayloadSummarizer<()> for Fixed {
    type Prepared = String;
    type Error = String;

    fn prepare(&self, parent_ctx: &RunContext<()>) -> Result<String, String> {
        Ok(parent_ctx.config.run_id.to_string())
    }
}

#[test]
fn summarizer_binds_a_call_to_the_parent_turn() {
    let ctx = RunContext::new(RunConfig::new("parent"), ());
    assert_eq!(Fixed.prepare(&ctx).unwrap(), "parent");
}

#[test]
fn child_config_bounds_the_summary_run() {
    let ctx = RunContext::new(RunConfig::new("parent"), ());
    let config = summary_child_config(&ctx, "summarizer", 3, 512);
    assert_eq!(config.run_id.to_string(), "summarizer-summary");
    assert_eq!(config.max_model_calls, Some(3));
    assert_eq!(config.max_tool_calls, Some(24));
    let tiny = summary_child_config(&ctx, "summarizer", 0, 512);
    assert_eq!(tiny.max_tool_calls, Some(8), "never below eight tool calls");
}

#[tokio::test]
async fn unary_summary_returns_the_models_text() {
    use crate::testkit::ScriptedModel;
    use tinyinference_llm::model::ModelResponse;
    let ctx = RunContext::new(RunConfig::new("summarizer-summary"), ());
    let model: Arc<dyn ChatModel<()>> =
        Arc::new(ScriptedModel::new(vec![ModelResponse::assistant(
            "short summary",
        )]));
    let text = run_unary_summary(ctx, "m", model, 2, "sys".into(), "prompt".into())
        .await
        .unwrap();
    assert_eq!(text, "short summary");
}
