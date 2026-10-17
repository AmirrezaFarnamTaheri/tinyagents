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
        response.usage = Some(Usage {
            charged_amount: Some(tinyinference_llm::usage::ChargedAmount::usd_micros(1_200)),
            ..Usage::new(11, 3)
        });
        response.raw = Some(serde_json::json!({
            "openhuman": {
                "usage": { "cached_input_tokens": 8 },
                "billing": { "charged_amount_usd": 0.0012 }
            }
        }));
        Ok(response)
    }
}

struct StreamScriptedModel {
    responses: Mutex<VecDeque<Vec<ModelStreamItem>>>,
    requests: Mutex<Vec<ModelRequest>>,
}

#[async_trait]
impl ChatModel<()> for StreamScriptedModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        Err(tinyinference_llm::Error::Unsupported(
            "stream-only test model".into(),
        ))
    }

    async fn stream(
        &self,
        _: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<tinyinference_llm::model::ModelStream> {
        self.requests.lock().unwrap().push(request);
        let items = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted stream");
        Ok(tinyinference_llm::model::ModelStream::new(Box::pin(
            futures::stream::iter(items),
        )))
    }
}

fn scripted_model(answers: &[&str]) -> ScriptedModel {
    ScriptedModel {
        answers: Arc::new(Mutex::new(answers.iter().map(|s| s.to_string()).collect())),
        requests: Arc::new(Mutex::new(Vec::new())),
    }
}

#[test]
fn legacy_and_options_close_futures_remain_send() {
    fn assert_send<T: Send>(_: T) {}

    let model = scripted_model(&[]);
    let contract = crate::config::RequiredOutput::new("summary");
    assert_send(grounded_close(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close",
            verification_prompt: |reply: &str| reply.to_owned(),
            repair_instruction: |_| "repair".to_owned(),
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
    ));
    assert_send(grounded_close_with_options(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close",
            verification_prompt: |reply: &str| reply.to_owned(),
            repair_instruction: |_| "repair".to_owned(),
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
        CloseRequestOptions::default(),
    ));
    assert_send(repair_required_output(
        &model,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "reply",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: false,
        },
    ));
    let mut callback = |_kind: CloseRequestKind, _response: Option<&ModelResponse>| {};
    assert_send(repair_required_output_with_options(
        &model,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "reply",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: false,
        },
        CloseRequestOptions {
            model: None,
            temperature: None,
            max_output_tokens: Some(32),
            on_call_completed: Some(&mut callback),
        },
    ));
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
fn configurable_result_renderer_truncates_then_selects_newest_and_preserves_order() {
    let results = vec![
        CloseToolResult {
            name: "old".into(),
            success: true,
            content: "old output".into(),
        },
        CloseToolResult {
            name: "middle".into(),
            success: false,
            content: "middle output".into(),
        },
        CloseToolResult {
            name: "new".into(),
            success: true,
            content: "你好世界".into(),
        },
    ];
    let options = CloseResultRenderOptions {
        per_result_cap: 2,
        truncation_marker: "… [truncated]",
        total_budget: 45,
        render_block: |result: &CloseToolResult| format!("<{}:{}>", result.name, result.content),
        omitted_notice: |count| format!("omitted:{count}|"),
    };

    assert_eq!(
        render_tool_results_with_options(&results, options),
        "omitted:1|<middle:mi… [truncated]><new:你好… [truncated]>"
    );
}

#[test]
fn configurable_result_renderer_always_keeps_an_oversized_newest_block() {
    let results = vec![CloseToolResult {
        name: "newest".into(),
        success: true,
        content: "long result".into(),
    }];
    let rendered = render_tool_results_with_options(
        &results,
        CloseResultRenderOptions {
            per_result_cap: 100,
            truncation_marker: "… [truncated]",
            total_budget: 1,
            render_block: |result: &CloseToolResult| format!("[{}]", result.content),
            omitted_notice: |count| format!("omitted:{count}\n"),
        },
    );

    assert_eq!(rendered, "omitted:0\n[long result]");
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
        GroundedCloseConfig {
            history: &[Message::user("original request")],
            initial_prompt: "close prompt",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| {
                close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}"))
            },
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
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
    assert!(requests.iter().all(|request| request.max_tokens.is_none()));
}

#[tokio::test]
async fn close_options_apply_output_cap_and_report_each_full_response_before_validation() {
    let model = scripted_model(&["Initial answer", "REJECT", "Repaired answer", "ACCEPT"]);
    let mut observed_charges = Vec::new();
    let mut on_response = |kind: CloseRequestKind, response: Option<&ModelResponse>| {
        assert!(matches!(
            kind,
            CloseRequestKind::Generation | CloseRequestKind::Verification
        ));
        let response = response.expect("successful call response");
        let raw_charge = response
            .raw
            .as_ref()
            .and_then(|raw| raw["openhuman"]["billing"]["charged_amount_usd"].as_f64())
            .expect("billing extension is retained");
        let charged_micros = response
            .usage
            .expect("scripted response usage")
            .charged_amount
            .expect("provider charge is retained")
            .micros;
        observed_charges.push((raw_charge, charged_micros));
    };
    let output = grounded_close_with_options(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close prompt",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| format!("repair {violation:?}"),
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
        CloseRequestOptions {
            model: Some("host-close-model"),
            temperature: Some(0.25),
            max_output_tokens: Some(37),
            on_call_completed: Some(&mut on_response),
        },
    )
    .await;

    assert_eq!(output.text, "Repaired answer");
    assert_eq!(output.usage.len(), 4);
    assert_eq!(observed_charges.len(), 4);
    assert!(
        observed_charges
            .iter()
            .all(|(usd, micros)| *usd == 0.0012 && *micros == 1_200)
    );
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| request.max_tokens == Some(37)
        && request.model.as_deref() == Some("host-close-model")
        && request.temperature == Some(0.25)));
}

#[tokio::test]
async fn close_options_use_buffered_stream_text_when_completed_response_text_is_empty() {
    let model = StreamScriptedModel {
        responses: Mutex::new(VecDeque::from([
            vec![
                ModelStreamItem::Started,
                ModelStreamItem::MessageDelta(tinyinference_llm::message::MessageDelta::text(
                    "Buffered answer",
                )),
                ModelStreamItem::Completed(ModelResponse::assistant("")),
            ],
            vec![
                ModelStreamItem::Started,
                ModelStreamItem::MessageDelta(tinyinference_llm::message::MessageDelta::text(
                    "ACCEPT",
                )),
                ModelStreamItem::Completed(ModelResponse::assistant("ACCEPT")),
            ],
        ])),
        requests: Mutex::new(Vec::new()),
    };
    let mut kinds = Vec::new();
    let mut on_call_completed = |kind: CloseRequestKind, response: Option<&ModelResponse>| {
        assert!(response.is_some());
        kinds.push(kind);
    };
    let output = grounded_close_with_options(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| format!("repair {violation:?}"),
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
        CloseRequestOptions {
            model: Some("stream-close-model"),
            temperature: Some(0.4),
            max_output_tokens: Some(33),
            on_call_completed: Some(&mut on_call_completed),
        },
    )
    .await;

    assert_eq!(output.text, "Buffered answer");
    assert_eq!(
        kinds,
        [CloseRequestKind::Generation, CloseRequestKind::Verification]
    );
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.model.as_deref() == Some("stream-close-model")
            && request.temperature == Some(0.4)
            && request.max_tokens == Some(33)
    }));
}

#[tokio::test]
async fn completed_close_response_is_reported_when_a_later_request_fails() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CompleteThenFail {
        calls: AtomicUsize,
        requests: Mutex<Vec<ModelRequest>>,
    }

    #[async_trait]
    impl ChatModel<()> for CompleteThenFail {
        async fn invoke(
            &self,
            _: &(),
            request: ModelRequest,
        ) -> tinyinference_llm::Result<ModelResponse> {
            self.requests.lock().unwrap().push(request);
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut response = ModelResponse::assistant("Completed answer");
                response.usage = Some(Usage {
                    charged_amount: Some(tinyinference_llm::usage::ChargedAmount::usd_micros(987)),
                    ..Usage::new(11, 3)
                });
                response.raw = Some(serde_json::json!({
                    "openhuman": { "billing": { "charged_amount_usd": 0.000987 } }
                }));
                Ok(response)
            } else {
                Err(tinyinference_llm::Error::Model("scripted failure".into()))
            }
        }
    }

    let model = CompleteThenFail::default();
    let mut observed = Vec::new();
    let mut observed_kinds = Vec::new();
    let mut on_response = |kind: CloseRequestKind, response: Option<&ModelResponse>| {
        observed_kinds.push((kind, response.is_some()));
        let Some(response) = response else {
            return;
        };
        observed.push(
            response
                .usage
                .expect("completed response usage")
                .charged_amount
                .expect("charged amount remains on intact response")
                .micros,
        );
        assert_eq!(
            response.raw.as_ref().unwrap()["openhuman"]["billing"]["charged_amount_usd"].as_f64(),
            Some(0.000987)
        );
    };
    let output = grounded_close_with_options(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close prompt",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| format!("repair {violation:?}"),
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
        CloseRequestOptions {
            model: None,
            temperature: None,
            max_output_tokens: Some(21),
            on_call_completed: Some(&mut on_response),
        },
    )
    .await;

    assert_eq!(output.text, "fallback");
    assert_eq!(output.usage.len(), 1);
    assert_eq!(observed, [987]);
    assert_eq!(
        observed_kinds,
        [
            (CloseRequestKind::Generation, true),
            (CloseRequestKind::Verification, false),
            (CloseRequestKind::Generation, false),
        ]
    );
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.max_tokens == Some(21))
    );
}

#[tokio::test(start_paused = true)]
async fn completed_close_usage_survives_a_pending_verifier_timeout() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    struct FirstThenPending {
        calls: AtomicUsize,
        verifier_started: Mutex<Option<oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl ChatModel<()> for FirstThenPending {
        async fn invoke(
            &self,
            _: &(),
            _: ModelRequest,
        ) -> tinyinference_llm::Result<ModelResponse> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut response = ModelResponse::assistant("Completed answer");
                response.usage = Some(Usage::new(11, 3));
                Ok(response)
            } else {
                if let Some(started) = self.verifier_started.lock().unwrap().take() {
                    let _ = started.send(());
                }
                std::future::pending().await
            }
        }
    }

    let (started_tx, started_rx) = oneshot::channel();
    let model = Arc::new(FirstThenPending {
        calls: AtomicUsize::new(0),
        verifier_started: Mutex::new(Some(started_tx)),
    });
    let model_for_task = Arc::clone(&model);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_by_callback = Arc::clone(&observed);
    let task = tokio::spawn(async move {
        let mut on_response = move |kind: CloseRequestKind, response: Option<&ModelResponse>| {
            assert_eq!(kind, CloseRequestKind::Generation);
            let response = response.expect("completed call response");
            observed_by_callback.lock().unwrap().push(
                response
                    .usage
                    .expect("completed response usage")
                    .input_tokens,
            );
        };
        grounded_close_with_options(
            model_for_task.as_ref(),
            &(),
            &NativeDialect,
            GroundedCloseConfig {
                history: &[],
                initial_prompt: "close",
                verification_prompt: |reply: &str| format!("verify {reply}"),
                repair_instruction: |violation| format!("repair {violation:?}"),
                harness_phrases: &[],
                budget: Some(std::time::Duration::from_secs(5)),
                deterministic_fallback: || "fallback".to_owned(),
            },
            CloseRequestOptions {
                model: None,
                temperature: None,
                max_output_tokens: Some(22),
                on_call_completed: Some(&mut on_response),
            },
        )
        .await
    });

    started_rx.await.expect("verifier request started");
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    let output = task.await.expect("close task joins");

    assert_eq!(output.text, "fallback");
    assert_eq!(output.usage.len(), 1);
    assert_eq!(*observed.lock().unwrap(), [11]);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn unclear_verification_and_failed_repair_use_the_deterministic_fallback() {
    let model = scripted_model(&["Initial answer", "unclear", "Repaired answer", "REJECT"]);
    let output = grounded_close(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close prompt",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| {
                close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}"))
            },
            harness_phrases: &[],
            budget: None,
            deterministic_fallback: || "deterministic fallback".to_owned(),
        },
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
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close prompt",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| {
                close_repair_instruction("Repair: {repair_reason}", &format!("{violation:?}"))
            },
            harness_phrases: &[],
            budget: Some(std::time::Duration::ZERO),
            deterministic_fallback: || "budget fallback".to_owned(),
        },
    )
    .await;
    assert_eq!(output.text, "budget fallback");
    assert!(model.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn required_output_repair_replaces_or_appends_and_synthesizes_when_invalid() {
    let contract = crate::config::RequiredOutput::new("summary");
    let model = scripted_model(&["{\"summary\":\"repaired\"}"]);
    let mut observed_responses = 0;
    let mut on_response = |kind: CloseRequestKind, response: Option<&ModelResponse>| {
        assert_eq!(kind, CloseRequestKind::RequiredOutputRepair);
        assert!(response.is_some());
        observed_responses += 1;
    };
    let repaired = repair_required_output_with_options(
        &model,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "old reply",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: false,
        },
        CloseRequestOptions {
            model: None,
            temperature: None,
            max_output_tokens: Some(19),
            on_call_completed: Some(&mut on_response),
        },
    )
    .await
    .expect("missing output triggers repair");
    assert_eq!(repaired.text, "{\"summary\":\"repaired\"}");
    assert!(repaired.model_repaired);
    assert_eq!(observed_responses, 1);
    assert_eq!(model.requests.lock().unwrap()[0].max_tokens, Some(19));

    let invalid = scripted_model(&["still no JSON"]);
    let appended = repair_required_output(
        &invalid,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "already streamed",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: true,
        },
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
