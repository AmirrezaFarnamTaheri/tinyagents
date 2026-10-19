use super::*;

#[test]
fn deterministic_templates_do_not_rescan_inserted_placeholder_text() {
    let checkpoint = deterministic_checkpoint(
        "{steps}|{results}",
        3,
        &[CloseToolResult {
            name: "tool_{steps}".to_owned(),
            success: true,
            content: "result {results}".to_owned(),
        }],
        100,
    );
    assert_eq!(checkpoint, "3|- `tool_{steps}` (ok): result {results}");

    let summary = deterministic_final_summary(
        "{stop_reason}|{results}",
        &[CloseToolResult {
            name: "tool_{results}".to_owned(),
            success: false,
            content: "failed {stop_reason}".to_owned(),
        }],
        Some("halt {results}"),
        100,
    );
    assert_eq!(
        summary,
        "halt {results}|- `tool_{results}` (failed): failed {stop_reason}"
    );
}

#[test]
fn close_copy_prompts_preserve_placeholder_shaped_content() {
    let copy = CloseCopy {
        verification_prompt_template: "request={user_request};records={records};reply={reply}",
        repair_template: "{repair_reason}|{instruction}",
        ..ENGLISH_CLOSE_COPY
    };
    assert_eq!(
        verification_prompt(
            &copy,
            "Explain {records}",
            "record {reply}",
            "reply {user_request}",
            200
        ),
        "request=Explain {records};records=record {reply};reply=reply {user_request}"
    );
    assert_eq!(
        repair_with_copy(&copy, "try {repair_reason}", CloseViolation::NoReply),
        "Your previous reply was empty or tried to call a tool. No tools will run: write the message itself.|try {repair_reason}"
    );
}

#[test]
fn english_verification_prompt_uses_no_tool_fallback_for_empty_records() {
    let expected = "You are checking a reply before it is shown to a user. Below are the user's request, the records of the tool calls made while handling it, and the reply.\n\nAnswer REJECT if any of these is true:\n1. The reply only says what the assistant will do or is about to do, instead of reporting what happened.\n2. The reply states something the tool records contradict, for example that something does not exist or did not work when a record shows it succeeded, or that something succeeded when its record shows it failed.\n3. The request was not completed, a failed record gives the reason, and the reply does not pass that reason on.\n4. The reply narrates the assistant's own deliberation — thinking aloud, correcting itself part way through, or weighing what to do — rather than stating the outcome. Reporting what a tool call returned is not deliberation.\n5. The reply recites or enumerates the tools the assistant has, instead of reporting what this turn's tool calls produced. Naming a tool the reply actually used, or answering a request that asked what the assistant can do, is not a violation.\n\nOtherwise answer ACCEPT. Reply with the single word ACCEPT or REJECT.\n\n<user_request>\nstatus?\n</user_request>\n\n<tool_records>\n(no tool calls completed)\n</tool_records>\n\n<reply>\nNothing ran.\n</reply>";
    assert_eq!(
        verification_prompt(&ENGLISH_CLOSE_COPY, "status?", "", "Nothing ran.", 200),
        expected
    );
    assert_eq!(
        verification_prompt(
            &ENGLISH_CLOSE_COPY,
            "status?",
            " \n\t ",
            "Nothing ran.",
            200
        ),
        expected
    );
}

#[test]
fn close_copy_builders_match_english_checkpoint_and_empty_summary() {
    assert_eq!(
        checkpoint_with_copy(
            &ENGLISH_CLOSE_COPY,
            2,
            "- `read_file` — ok\n  > data\n",
            true
        ),
        "I reached the tool-call limit for this turn (2 steps), so I paused here before I could write up what I found.\n\n**Raw results from this turn**\n- `read_file` — ok\n  > data\n\n**Next steps:** I'll continue from here — just reply (e.g. \"continue\") and I'll pick up where I left off."
    );
    assert_eq!(
        final_summary_with_copy(&ENGLISH_CLOSE_COPY, "", false),
        ENGLISH_CLOSE_COPY.empty_final_summary
    );
}

#[test]
fn close_usage_counts_attempts_and_excludes_verifier_from_context_tokens() {
    let mut usage = CloseUsage::for_model("close-model");
    let generation = Usage {
        input_tokens: 11,
        output_tokens: 4,
        cache_read_tokens: 3,
        ..Default::default()
    };
    usage.record(CloseRequestKind::Generation, Some(&generation), true);
    let verification = Usage {
        input_tokens: 50,
        output_tokens: 2,
        cache_read_tokens: 0,
        ..Default::default()
    };
    usage.record(CloseRequestKind::Verification, Some(&verification), false);
    usage.record(CloseRequestKind::RequiredOutputRepair, None, true);
    assert_eq!(usage.model_calls, 3);
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.cached_input_tokens
        ),
        (61, 6, 3)
    );
    assert_eq!(
        (usage.last_call_input_tokens, usage.last_call_output_tokens),
        (11, 4)
    );
    assert_eq!(usage.pricing_model, "close-model");
}

#[test]
fn close_usage_accepts_host_billing_without_host_types() {
    let mut usage = CloseUsage::for_model("selected-model");
    let mut response = ModelResponse::assistant("closed");
    response.usage = Some(Usage {
        input_tokens: 17,
        output_tokens: 5,
        ..Default::default()
    });

    usage.record_close_call_with_cost(
        CloseRequestKind::Generation,
        Some(&response),
        |response| {
            assert!(response.is_some());
            Some((
                "actual-route-model".into(),
                crate::cost::CallCost::Charged(0.007),
            ))
        },
    );
    usage.record_close_call_with_cost(
        CloseRequestKind::RequiredOutputRepair,
        None,
        |response| {
            assert!(response.is_none());
            None
        },
    );

    assert_eq!(usage.model_calls, 2);
    assert_eq!((usage.input_tokens, usage.output_tokens), (17, 5));
    assert_eq!(usage.pricing_model, "actual-route-model");
    assert_eq!(usage.cost.usd(), Some(0.007));
}

#[test]
fn classified_halt_close_builds_only_for_classified_reason() {
    let mut built = false;
    let result = classified_halt_close(
        Some("halt"),
        |reason| reason == "halt",
        || {
            built = true;
            42
        },
    );
    assert_eq!(result, Some(42));
    assert!(built);
    assert_eq!(
        classified_halt_close::<()>(Some("ordinary"), |_| false, || ()),
        None
    );
    assert_eq!(
        classified_halt_close::<()>(None, |_| panic!("no reason"), || panic!("no close")),
        None
    );
}

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

struct StaticCloseSource(Arc<dyn ChatModel<()>>);

impl CloseModelSource for StaticCloseSource {
    fn build_summarizer(
        &self,
        _model: &str,
        _temperature: f64,
        _thread_id: Option<&str>,
    ) -> Result<Arc<dyn ChatModel<()>>, String> {
        Ok(Arc::clone(&self.0))
    }
}

struct EmptyCloseEnricher;

impl crate::observability::SupplementalEnricher for EmptyCloseEnricher {
    fn enrich(
        &self,
        _model: &str,
        _response: &ModelResponse,
    ) -> (Option<String>, crate::events::GenerationEnrichment) {
        (None, Default::default())
    }
}

struct InvokeFailureModel;

#[async_trait]
impl ChatModel<()> for InvokeFailureModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        Err(tinyinference_llm::Error::Model(
            "synthetic provider failure".into(),
        ))
    }
}

#[tokio::test]
async fn close_source_model_stamps_request_and_traces_completion() {
    use crate::events::{EventListener, EventSink, RecordingListener};
    use crate::ids::RunId;

    let inner = Arc::new(scripted_model(&["answer"]));
    let source = StaticCloseSource(inner.clone());
    let events = EventSink::new();
    let listener = Arc::new(RecordingListener::new());
    events.subscribe(listener.clone() as Arc<dyn EventListener>);
    let trace = crate::observability::SupplementalCallTrace::new(
        events,
        RunId::new("run-close"),
        Arc::new(EmptyCloseEnricher),
    );
    let close = CloseSourceModel {
        source: &source,
        model: "selected",
        temperature: 0.2,
        thread_id: Some("thread"),
        trace: Some(&trace),
    };
    close
        .invoke(&(), ModelRequest::new(Vec::new()))
        .await
        .unwrap();

    let requests = inner.requests.lock().unwrap();
    let correlation = requests[0].correlation.as_ref().unwrap();
    assert_eq!(correlation.run_id, "run-close");
    assert!(correlation.call_id.ends_with("-host-supplemental-1"));
    let events = listener.events();
    assert_eq!(events.len(), 3);
    assert!(matches!(
        events[0].event,
        crate::events::AgentEvent::ModelStarted { .. }
    ));
    assert!(matches!(
        events[1].event,
        crate::events::AgentEvent::UsageRecorded { .. }
    ));
    assert!(matches!(
        events[2].event,
        crate::events::AgentEvent::ModelCompleted { .. }
    ));
}

#[tokio::test]
async fn close_source_model_records_one_failure_when_stream_ends_without_terminal_item() {
    use crate::events::{AgentEvent, EventListener, EventSink, RecordingListener};
    use crate::ids::RunId;
    use futures::StreamExt;

    let inner = Arc::new(StreamScriptedModel {
        responses: Mutex::new(VecDeque::from([Vec::new()])),
        requests: Mutex::new(Vec::new()),
    });
    let source = StaticCloseSource(inner);
    let events = EventSink::new();
    let listener = Arc::new(RecordingListener::new());
    events.subscribe(listener.clone() as Arc<dyn EventListener>);
    let trace = crate::observability::SupplementalCallTrace::new(
        events,
        RunId::new("run-close"),
        Arc::new(EmptyCloseEnricher),
    );
    let close = CloseSourceModel {
        source: &source,
        model: "selected",
        temperature: 0.2,
        thread_id: None,
        trace: Some(&trace),
    };
    let mut stream = close.stream(&(), ModelRequest::default()).await.unwrap();
    assert!(stream.next().await.is_none());
    drop(stream);
    let events = listener.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].event, AgentEvent::ModelStarted { .. }));
    assert!(matches!(events[1].event, AgentEvent::ModelFailed { .. }));
}

#[tokio::test]
async fn close_source_model_records_invoke_and_stream_failures_once() {
    use crate::events::{AgentEvent, EventListener, EventSink, RecordingListener};
    use crate::ids::RunId;
    use futures::StreamExt;

    let events = EventSink::new();
    let listener = Arc::new(RecordingListener::new());
    events.subscribe(listener.clone() as Arc<dyn EventListener>);
    let trace = crate::observability::SupplementalCallTrace::new(
        events,
        RunId::new("run-close-failures"),
        Arc::new(EmptyCloseEnricher),
    );
    let source = StaticCloseSource(Arc::new(InvokeFailureModel));
    let close = CloseSourceModel {
        source: &source,
        model: "selected",
        temperature: 0.0,
        thread_id: None,
        trace: Some(&trace),
    };
    assert!(close.invoke(&(), ModelRequest::default()).await.is_err());
    let events = listener.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].event, AgentEvent::ModelStarted { .. }));
    assert!(matches!(events[1].event, AgentEvent::ModelFailed { .. }));

    let stream_source = StaticCloseSource(Arc::new(StreamScriptedModel {
        responses: Mutex::new(VecDeque::from([vec![ModelStreamItem::Failed(
            "synthetic stream failure".into(),
        )]])),
        requests: Mutex::new(Vec::new()),
    }));
    let stream_close = CloseSourceModel {
        source: &stream_source,
        ..close
    };
    let mut stream = stream_close
        .stream(&(), ModelRequest::default())
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(ModelStreamItem::Failed(_))
    ));
    drop(stream);
    let events = listener.events();
    assert_eq!(events.len(), 4);
    assert!(matches!(events[2].event, AgentEvent::ModelStarted { .. }));
    assert!(matches!(events[3].event, AgentEvent::ModelFailed { .. }));
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
            minimum_harness_phrase_chars: 24,
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
fn harness_needles_split_normalize_and_filter_by_host_word_count() {
    assert_eq!(
        harness_instruction_needles(&["One TWO. short\nThree FOUR Five"], 3),
        ["three four five"]
    );
    let needle = harness_instruction_needles(&["a b c d e f g h i j"], 10);
    assert_eq!(needle, ["a b c d e f g h i j"]);
    assert!(!quotes_harness_instruction(
        "a b c d e f g h i j",
        &["a b c d e f g h i j"]
    ));
    assert!(quotes_harness_instruction_with_minimum(
        "a b c d e f g h i j",
        &needle.iter().map(String::as_str).collect::<Vec<_>>(),
        1,
    ));
}

#[tokio::test]
async fn options_quote_threshold_catches_short_prefiltered_host_needles() {
    let needles = harness_instruction_needles(&["a b c d e f g h i j"], 10);
    let phrase_refs = needles.iter().map(String::as_str).collect::<Vec<_>>();
    let model = scripted_model(&["a b c d e f g h i j", "safe answer", "ACCEPT"]);
    let output = grounded_close_with_options(
        &model,
        &(),
        &NativeDialect,
        GroundedCloseConfig {
            history: &[],
            initial_prompt: "close",
            verification_prompt: |reply: &str| format!("verify {reply}"),
            repair_instruction: |violation| format!("repair {violation:?}"),
            harness_phrases: &phrase_refs,
            budget: None,
            deterministic_fallback: || "fallback".to_owned(),
        },
        CloseRequestOptions {
            minimum_harness_phrase_chars: 1,
            ..CloseRequestOptions::default()
        },
    )
    .await;

    assert_eq!(output.text, "safe answer");
    assert_eq!(model.requests.lock().unwrap().len(), 3);
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
            minimum_harness_phrase_chars: 24,
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
async fn close_options_use_buffered_text_when_completed_response_is_empty_or_whitespace() {
    let model = StreamScriptedModel {
        responses: Mutex::new(VecDeque::from([
            vec![
                ModelStreamItem::Started,
                ModelStreamItem::MessageDelta(tinyinference_llm::message::MessageDelta::text(
                    "Buffered answer",
                )),
                ModelStreamItem::Completed(ModelResponse::assistant(" \n ")),
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
            minimum_harness_phrase_chars: 24,
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
            minimum_harness_phrase_chars: 24,
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
                minimum_harness_phrase_chars: 24,
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
            minimum_harness_phrase_chars: 24,
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

#[tokio::test]
async fn required_output_repair_preserves_host_placement_for_invalid_unstreamed_reply() {
    let contract = crate::config::RequiredOutput::new("summary");
    let model = scripted_model(&["not structured output"]);
    let repaired = repair_required_output_with_repair_options(
        &model,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "original reply",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: false,
        },
        RequiredOutputRepairOptions {
            request: CloseRequestOptions::default(),
            fallback_placement: RequiredOutputFallbackPlacement::Prepend,
        },
    )
    .await
    .expect("missing output triggers repair");

    assert!(!repaired.model_repaired);
    assert_eq!(repaired.text, "{\"summary\":\"\"}\n\noriginal reply");
}

#[tokio::test]
async fn required_output_repair_keeps_valid_streamed_repair_as_suffix() {
    let contract = crate::config::RequiredOutput::new("summary");
    let model = scripted_model(&["{\"summary\":\"repaired\"}"]);
    let repaired = repair_required_output_with_repair_options(
        &model,
        &(),
        &NativeDialect,
        RequiredOutputRepairConfig {
            history: &[],
            reply: "already streamed",
            contract: &contract,
            repair_instruction: "repair",
            reply_already_streamed: true,
        },
        RequiredOutputRepairOptions {
            request: CloseRequestOptions::default(),
            fallback_placement: RequiredOutputFallbackPlacement::Prepend,
        },
    )
    .await
    .expect("missing output triggers repair");

    assert!(repaired.model_repaired);
    assert_eq!(
        repaired.text,
        "already streamed\n\n{\"summary\":\"repaired\"}"
    );
}
