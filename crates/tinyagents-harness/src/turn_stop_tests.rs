use super::*;

fn fallback(note: &str) -> &'static str {
    if note.contains("in a row failed") {
        "turn_stop.varied_failures"
    } else {
        "turn_stop.repeated_failure"
    }
}

const CLASSIFIED_NOTE: &str = "Stopping after 2 attempt(s): failure class \
    `uncertain_side_effect` still blocks operation `web_answer_tool` on \
    `https://example.com/private?q=secret`. Resolve this blocker before retrying.";

#[test]
fn a_typed_breaker_cause_is_independent_of_note_wording() {
    let stop = TurnStop::breaker_with_cause(
        "wording changed: https://example.com/private?q=secret",
        TurnFailureClass::new("uncertain_side_effect"),
        Some("web_answer_tool".into()),
    );
    assert_eq!(stop.kind, TurnStopKind::Breaker);
    assert_eq!(
        stop.failure_class.as_ref().map(TurnFailureClass::as_str),
        Some("uncertain_side_effect")
    );
    assert_eq!(stop.operation.as_deref(), Some("web_answer_tool"));
    let message = stop.status_message();
    assert_eq!(
        message,
        "stopped: breaker uncertain_side_effect on web_answer_tool"
    );
    assert!(!message.contains("example.com"), "the scope never leaks");
}

#[test]
fn legacy_breaker_uses_the_supplied_fallback_without_parsing_the_note() {
    let stop = TurnStop::breaker(
        "The last 6 tool calls in a row failed. Last error: token sk-live-123 rejected",
        fallback,
    );
    assert_eq!(stop.kind, TurnStopKind::Breaker);
    assert_eq!(
        stop.failure_class.as_ref().map(TurnFailureClass::as_str),
        Some("varied_failures")
    );
    assert_eq!(stop.operation, None);
    assert_eq!(stop.status_message(), "stopped: breaker varied_failures");
}

#[test]
fn typed_failure_classes_do_not_include_note_content() {
    let stop = TurnStop::breaker(
        "Stopping after 1 attempt(s): failure class `permission` still blocks operation \
         `rm -rf /home/alice` on `x`.",
        fallback,
    );
    assert_eq!(
        stop.failure_class.as_ref().map(TurnFailureClass::as_str),
        Some("repeated_failure")
    );
    assert_eq!(stop.operation, None, "free text must not reach a trace");
}

#[test]
fn a_classified_stop_preserves_its_cause_without_reading_its_summary() {
    let stop = TurnStop::from_classified_cause(crate::no_progress::ClassifiedStopCause {
        class: "permission".into(),
        operation: "file_write".into(),
        summary: "changed prose without a template".into(),
    });
    assert_eq!(
        stop.failure_class.as_ref().map(TurnFailureClass::as_str),
        Some("permission")
    );
    assert_eq!(stop.operation.as_deref(), Some("file_write"));
    assert_eq!(
        stop.status_message(),
        "stopped: breaker permission on file_write"
    );
}

#[test]
fn typed_stop_identifiers_reject_free_text_and_paths() {
    let stop = TurnStop::breaker_with_cause(
        "ignored",
        TurnFailureClass::new("permission from /home/alice"),
        Some("write_file /home/alice/private".into()),
    );
    assert_eq!(stop.failure_class.as_ref().map(TurnFailureClass::as_str), Some("unknown"));
    assert_eq!(stop.operation, None);
    assert_eq!(stop.status_message(), "stopped: breaker unknown");
}

#[test]
fn classify_prefers_breaker_then_wind_down_then_cap() {
    assert_eq!(TurnStop::classify(None, false, false, fallback), None);
    assert_eq!(
        TurnStop::classify(None, false, true, fallback),
        Some(TurnStop::iteration_cap())
    );
    assert_eq!(
        TurnStop::classify(None, true, true, fallback),
        Some(TurnStop::wind_down()),
        "a wind-down pause can read as a cap; the wind-down is the cause"
    );
    assert_eq!(
        TurnStop::classify(Some(CLASSIFIED_NOTE), true, true, fallback).map(|s| s.kind),
        Some(TurnStopKind::Breaker)
    );
}

#[test]
fn an_incomplete_reason_maps_to_cap_or_breaker() {
    assert_eq!(
        TurnStop::from_incomplete_reason(SUBAGENT_ITERATION_CAP_REASON, fallback),
        TurnStop::iteration_cap()
    );
    assert_eq!(
        TurnStop::from_incomplete_reason(CLASSIFIED_NOTE, fallback)
            .operation
            .as_deref(),
        None
    );
    assert_eq!(TurnStop::wind_down().status_message(), "stopped: wind_down");
    assert_eq!(
        TurnStop::iteration_cap().status_message(),
        "stopped: iteration_cap"
    );
}
