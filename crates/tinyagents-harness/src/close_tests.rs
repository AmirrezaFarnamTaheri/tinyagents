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
    async fn invoke(
        &self,
        _: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.requests.lock().unwrap().push(request);
        let answer = self
            .answers
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted answer");
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
        CloseToolResult {
            name: "old".into(),
            success: true,
            content: "old output".into(),
        },
        CloseToolResult {
            name: "new".into(),
            success: false,
            content: "new output".into(),
        },
    ];
    let rendered = render_tool_results(&results, 30);
    assert!(rendered.contains("new"));
    assert!(!rendered.contains("old output"));
    assert!(rendered.chars().count() <= 30);
}

#[test]
fn verdict_parser_uses_the_last_standalone_verdict_token() {
    assert_eq!(
        parse_close_verdict("reasoning ACCEPT\nREJECT"),
        CloseVerdict::Reject
    );
    assert_eq!(parse_close_verdict("REJECT"), CloseVerdict::Reject);
    assert_eq!(parse_close_verdict("probably ACCEPT"), CloseVerdict::Accept);
    assert_eq!(parse_close_verdict(""), CloseVerdict::Unclear);
}

#[test]
fn harness_quote_matching_normalizes_whitespace_but_ignores_short_phrases() {
    assert!(quotes_harness_instruction(
        "Do not call another tool right now",
        &["Do not call another tool right now"]
    ));
    assert!(!quotes_harness_instruction("The answer is yes", &["yes"]));
}

#[test]
fn result_truncation_respects_unicode_character_boundaries() {
    assert_eq!(truncate_chars("  你好世界  ", 2), "你好… [truncated]");
}

#[test]
fn repair_prompt_inserts_the_violation_into_host_template() {
    let prompt = close_repair_instruction("{repair_reason}\nTry again", "bad evidence");
    assert_eq!(prompt, "bad evidence\nTry again");
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
        |violation| close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}")),
        &[],
        None,
        || "fallback".to_owned(),
    )
    .await;
    assert_eq!(output.text, "Found two records.");
    assert_eq!(output.usage.len(), 2);
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.tools.is_empty()));
    assert!(
        requests
            .iter()
            .all(|request| request.tool_choice == ToolChoice::None)
    );
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
        |violation| close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}")),
        &[],
        None,
        || "deterministic fallback".to_owned(),
    )
    .await;
    assert_eq!(output.text, "deterministic fallback");
    assert_eq!(model.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn an_expired_close_budget_uses_fallback_without_starting_model_calls() {
    let model = scripted_model(&["unconsumed answer"]);
    let output = grounded_close(
        &model,
        &(),
        &NativeDialect,
        &[],
        "close prompt",
        |reply| format!("verify {reply}"),
        |violation| close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}")),
        &[],
        Some(std::time::Duration::ZERO),
        || "budget fallback".to_owned(),
    )
    .await;
    assert_eq!(output.text, "budget fallback");
    assert!(model.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn required_output_repair_replaces_or_appends_and_synthesizes_when_invalid() {
    let contract = crate::config::RequiredOutput::new("summary");
    let model = scripted_model(&["{\"summary\":\"repaired\"}"]);
    let repaired = repair_required_output(
        &model,
        &(),
        &NativeDialect,
        &[],
        "old reply",
        &contract,
        "repair",
        false,
    )
    .await
    .expect("missing output triggers repair");
    assert_eq!(repaired.text, "{\"summary\":\"repaired\"}");
    assert!(repaired.model_repaired);

    let invalid = scripted_model(&["still no JSON"]);
    let appended = repair_required_output(
        &invalid,
        &(),
        &NativeDialect,
        &[],
        "already streamed",
        &contract,
        "repair",
        true,
    )
    .await
    .expect("missing output triggers repair");
    assert_eq!(appended.text, "already streamed\n\n{\"summary\":\"\"}");
    assert!(!appended.model_repaired);
    assert_eq!(
        invalid.requests.lock().unwrap()[0].tool_choice,
        ToolChoice::None
    );
}
