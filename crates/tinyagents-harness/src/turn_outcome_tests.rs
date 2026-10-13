use super::*;
use tinyinference_llm::message::{AssistantMessage, ContentBlock};
use tinyinference_llm::tool::ToolCall;

fn response(finish_reason: &str, text: &str) -> ModelResponse {
    let content = if text.is_empty() {
        vec![ContentBlock::Thinking {
            text: "still weighing the approach".into(),
            signature: None,
        }]
    } else {
        vec![ContentBlock::Text(text.to_string())]
    };
    ModelResponse {
        message: AssistantMessage {
            id: None,
            content,
            tool_calls: Vec::new(),
            usage: None,
            origin: None,
        },
        usage: None,
        finish_reason: Some(finish_reason.to_string()),
        raw: None,
        resolved_model: None,
        continue_turn: None,
        served_from_cache: false,
        correlation: None,
        resolved_route: None,
    }
}

#[test]
fn a_length_stop_with_only_reasoning_ran_out_of_output_budget() {
    assert!(ended_out_of_output_budget(Some(&response("length", ""))));
}

#[test]
fn every_provider_length_stop_spelling_ran_out_of_output_budget() {
    for reason in ["max_tokens", "MAX_TOKENS"] {
        assert!(ended_out_of_output_budget(Some(&response(reason, ""))));
    }
}

#[test]
fn the_halt_summary_survives_a_poisoned_slot() {
    let slot = new_halt_summary_slot();
    *slot.lock().unwrap() = Some("stalled".into());
    let poisoner = Arc::clone(&slot);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.lock().unwrap();
        panic!("poison");
    })
    .join();
    assert_eq!(take_halt_summary(&slot).as_deref(), Some("stalled"));
}

#[test]
fn a_length_stop_with_visible_text_is_a_real_answer() {
    assert!(!ended_out_of_output_budget(Some(&response(
        "length",
        "partial answer"
    ))));
}

#[test]
fn a_normal_blank_stop_or_no_final_response_is_not_truncation() {
    assert!(!ended_out_of_output_budget(Some(&response("stop", ""))));
    assert!(!ended_out_of_output_budget(None));
}

#[test]
fn a_length_stop_that_requests_a_tool_is_not_truncation() {
    let mut r = response("length", "");
    r.message.tool_calls = vec![ToolCall::new("c", "t", serde_json::json!({}))];
    assert!(!ended_out_of_output_budget(Some(&r)));
}

#[test]
fn the_sink_drains_once_and_survives_a_poisoned_lock() {
    let sink = new_tool_outcome_sink();
    sink.lock().expect("lock").push(ToolCallOutcome {
        call_id: "c1".into(),
        name: "t".into(),
        arguments: serde_json::json!({"a": 1}),
        success: true,
        content: "ok".into(),
        duration_ms: 3,
    });
    let poisoner = Arc::clone(&sink);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.lock().expect("lock");
        panic!("poison the sink");
    })
    .join();
    let drained = drain_tool_outcomes(&sink);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].call_id, "c1");
    assert!(drain_tool_outcomes(&sink).is_empty());
}

#[test]
fn the_halt_summary_is_taken_exactly_once() {
    let slot = new_halt_summary_slot();
    assert_eq!(take_halt_summary(&slot), None);
    *slot.lock().expect("lock") = Some("repeated failure".into());
    assert_eq!(
        take_halt_summary(&slot).as_deref(),
        Some("repeated failure")
    );
    assert_eq!(take_halt_summary(&slot), None);
}
