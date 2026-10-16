use super::*;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyinference_llm::model::ModelResponse;
use tinytools_agent::dialect::NativeDialect;

#[derive(Clone)]
struct ScriptedModel {
    answers: Arc<Mutex<VecDeque<String>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

#[async_trait]
impl ChatModel<()> for ScriptedModel {
    async fn invoke(&self, _: &(), request: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        self.requests.lock().unwrap().push(request);
        let answer = self.answers.lock().unwrap().pop_front().expect("scripted answer");
        let mut response = ModelResponse::assistant(answer);
        response.usage = Some(Usage::new(11, 3));
        Ok(response)
    }
}

fn scripted_model(answers: &[&str]) -> ScriptedModel {
    ScriptedModel {
        answers: Arc::new(Mutex::new(answers.iter().map(|s| s.to_string()).collect())),
        requests: Arc::new(Mutex::new(Vec::new())),
    }
}

#[test]
fn render_tool_results_spends_the_budget_on_newest_results() {
    let results = vec![
        CloseToolResult { name: "old".into(), success: true, content: "old output".into() },
        CloseToolResult { name: "new".into(), success: false, content: "new output".into() },
    ];
    let rendered = render_tool_results(&results, 30);
    assert!(rendered.contains("new"));
    assert!(!rendered.contains("old output"));
}

#[test]
fn verdict_parser_accepts_only_an_exact_verdict_line() {
    assert_eq!(parse_close_verdict("ACCEPT\nLooks good"), CloseVerdict::Accept);
    assert_eq!(parse_close_verdict("REJECT"), CloseVerdict::Reject);
    assert_eq!(parse_close_verdict("probably ACCEPT"), CloseVerdict::Unclear);
    assert_eq!(parse_close_verdict(""), CloseVerdict::Unclear);
}

#[test]
fn harness_quote_matching_normalizes_whitespace_but_ignores_short_phrases() {
    assert!(quotes_harness_instruction("Do not call another tool right now", &["Do not call another tool right now"]));
    assert!(!quotes_harness_instruction("The answer is yes", &["yes"]));
}

#[test]
fn repair_prompt_inserts_the_violation_into_host_template() {
    let prompt = close_repair_instruction("{repair_reason}\nTry again", CloseViolation::Unverified);
    assert!(prompt.starts_with("Your previous answer was not verified"));
}

#[tokio::test]
async fn grounded_close_disables_tools_and_accepts_a_verified_answer() {
    let model = scripted_model(&["Found two records.", "ACCEPT"]);
    let output = grounded_close(
        &model,
        &(),
        &NativeDialect,
        &[Message::user("original request")],
        "close prompt",
        |reply| format!("verify {reply}"),
        "Repair: {repair_reason}",
        &[],
        || "fallback".to_owned(),
    )
    .await;
    assert_eq!(output.text, "Found two records.");
    assert_eq!(output.usage.len(), 2);
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.tools.is_empty()));
    assert!(requests.iter().all(|request| request.tool_choice == ToolChoice::None));
}

#[tokio::test]
async fn unclear_verification_and_failed_repair_use_the_deterministic_fallback() {
    let model = scripted_model(&["Initial answer", "unclear", "Repaired answer", "REJECT"]);
    let output = grounded_close(
        &model,
        &(),
        &NativeDialect,
        &[],
        "close prompt",
        |reply| format!("verify {reply}"),
        "Repair: {repair_reason}",
        &[],
        || "deterministic fallback".to_owned(),
    )
    .await;
    assert_eq!(output.text, "deterministic fallback");
    assert_eq!(model.requests.lock().unwrap().len(), 4);
}
