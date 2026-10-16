use super::*;

struct Model(Result<String, String>);

#[async_trait]
impl CapSummaryModel for Model {
    async fn summarize(&self, _max_calls: usize, _digest: &str) -> Result<String, String> {
        self.0.clone()
    }
}

fn results() -> [CapToolResult<'static>; 1] {
    [CapToolResult {
        call_id: "call-1",
        name: "read",
        output: "found a useful file",
        success: true,
    }]
}

#[tokio::test]
async fn cap_summary_uses_model_prose_when_available() {
    let model = Model(Ok("Read the file; inspect its tests next.".into()));
    let summary = summarize_at_cap(Some(&model), 3, &results(), 100).await;
    assert!(summary.model_summarized);
    assert_eq!(summary.text, "Read the file; inspect its tests next.");
    assert!(summary.digest.contains("read [ok]"));
}

#[tokio::test]
async fn cap_summary_has_deterministic_incomplete_fallback_on_failure_or_empty_text() {
    for model in [Model(Err("offline".into())), Model(Ok("  ".into()))] {
        let summary = summarize_at_cap(Some(&model), 3, &results(), 100).await;
        assert!(!summary.model_summarized);
        assert!(summary.text.contains("task is incomplete"));
        assert_eq!(summary.digest, "- read [ok]: found a useful file");
    }
}
