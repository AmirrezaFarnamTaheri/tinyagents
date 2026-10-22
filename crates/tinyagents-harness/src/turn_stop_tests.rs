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
    assert_eq!(
        stop.failure_class.as_ref().map(TurnFailureClass::as_str),
        Some("unknown")
    );
    assert_eq!(stop.operation, None);
    assert_eq!(stop.status_message(), "stopped: breaker unknown");
}

#[test]
fn stop_serde_rejects_or_scrubs_untrusted_identifiers() {
    let secret = "private@example.test/secret";
    let stop = TurnStop {
        kind: TurnStopKind::Breaker,
        failure_class: Some(TurnFailureClass::new(secret)),
        operation: Some(secret.into()),
    };

    let encoded = serde_json::to_string(&stop).expect("serialize sanitized stop");
    assert!(!encoded.contains(secret));
    assert!(encoded.contains("unknown"));
    assert!(!stop.status_message().contains(secret));

    for invalid in [
        format!(r#"{{"kind":"breaker","failure_class":"{secret}"}}"#),
        format!(r#"{{"kind":"breaker","operation":"{secret}"}}"#),
    ] {
        let error = serde_json::from_str::<TurnStop>(&invalid)
            .expect_err("untrusted identifiers must not deserialize");
        assert!(!error.to_string().contains(secret));
    }
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

struct PartialCopyProvider;

impl crate::turn_stop::CopyProvider for PartialCopyProvider {
    fn copy_override(&self, request: &crate::turn_stop::CopyRequest<'_>) -> Option<String> {
        match request {
            crate::turn_stop::CopyRequest::TurnStop {
                key: crate::turn_stop::TurnStopCopyKey::Policy,
                ..
            } => Some("Translated policy copy".into()),
            _ => None,
        }
    }
}

#[test]
fn user_copy_uses_typed_class_mapping_and_partial_provider_fallback() {
    let policy = TurnStop::breaker_with_cause(
        "ignored",
        TurnFailureClass::new("blocked_by_policy"),
        Some("shell".into()),
    );
    assert_eq!(
        policy.user_copy(&PartialCopyProvider),
        "Translated policy copy"
    );
    assert_eq!(policy.failure_class.as_deref(), Some("blocked_by_policy"));

    let unknown =
        TurnStop::breaker_with_cause("ignored", TurnFailureClass::new("new_host_class"), None);
    assert_eq!(
        unknown.user_copy(&PartialCopyProvider),
        "my tool calls were not getting anywhere"
    );
}

#[test]
fn copy_provider_override_cannot_change_machine_class_or_expose_operation() {
    struct UnsafeCopy;
    impl crate::turn_stop::CopyProvider for UnsafeCopy {
        fn copy_override(&self, _request: &crate::turn_stop::CopyRequest<'_>) -> Option<String> {
            Some("secret /home/private".into())
        }
    }

    let stop = TurnStop::breaker_with_cause(
        "ignored",
        TurnFailureClass::new("permission"),
        Some("file_write".into()),
    );
    assert_eq!(stop.user_copy(&UnsafeCopy), "secret /home/private");
    assert_eq!(stop.failure_class.as_deref(), Some("permission"));
    assert_eq!(
        stop.status_message(),
        "stopped: breaker permission on file_write"
    );
}

#[test]
fn user_copy_never_receives_an_untrusted_operation_string() {
    struct OperationCopy;
    impl crate::turn_stop::CopyProvider for OperationCopy {
        fn copy_override(&self, request: &crate::turn_stop::CopyRequest<'_>) -> Option<String> {
            match request {
                crate::turn_stop::CopyRequest::TurnStop { operation, .. } => {
                    assert_eq!(*operation, None);
                    None
                }
                _ => None,
            }
        }
    }

    let stop = TurnStop {
        kind: TurnStopKind::Breaker,
        failure_class: Some(TurnFailureClass::new("permission")),
        operation: Some("shell /home/private".into()),
    };
    let copy = stop.user_copy(&OperationCopy);
    assert!(!copy.contains("/home/private"));
}

#[test]
fn non_failure_stops_use_generic_copy_without_changing_their_wire_kind() {
    let provider = PartialCopyProvider;
    let wind_down = TurnStop::wind_down();
    let cap = TurnStop::iteration_cap();
    assert_eq!(
        wind_down.user_copy(&provider),
        "my tool calls were not getting anywhere"
    );
    assert_eq!(
        cap.user_copy(&provider),
        "my tool calls were not getting anywhere"
    );
    assert_eq!(wind_down.kind, TurnStopKind::WindDown);
    assert_eq!(cap.kind, TurnStopKind::IterationCap);
}

#[test]
fn stable_stop_classes_map_to_all_translation_keys_without_reading_stop_prose() {
    use crate::turn_stop::TurnStopCopyKey as Key;

    let cases = [
        ("transient", Key::Transient),
        ("uncertain_side_effect", Key::UncertainSideEffect),
        ("authentication", Key::Authentication),
        ("permission", Key::Permission),
        ("policy", Key::Policy),
        ("missing_program", Key::MissingProgram),
        ("not_found", Key::NotFound),
        ("invalid_calls", Key::InvalidCalls),
        ("unavailable", Key::Unavailable),
        ("site_refused", Key::SiteRefused),
        ("repeated_failure", Key::RepeatedFailure),
        ("varied_failures", Key::VariedFailures),
        ("repeating_step", Key::RepeatingStep),
        ("out_of_credits", Key::OutOfCredits),
        ("provider_config", Key::ProviderConfig),
        ("missing_connection", Key::MissingConnection),
        ("generic", Key::Generic),
    ];
    for (class, key) in cases {
        assert_eq!(TurnFailureClass::new(class).copy_key(), key, "{class}");
        assert_eq!(key.as_str(), format!("turn_stop.{class}"));
    }
    for (alias, expected) in [
        ("blocked_by_policy", Key::Policy),
        ("unsupported", Key::MissingProgram),
        ("missing_app", Key::MissingProgram),
        ("missing_window", Key::NotFound),
        ("validation", Key::InvalidCalls),
        ("invalid_arguments", Key::InvalidCalls),
        ("service_refused", Key::Unavailable),
    ] {
        assert_eq!(TurnFailureClass::new(alias).copy_key(), expected, "{alias}");
    }

    let stop = TurnStop::breaker(
        "private prose says permission, but class is generic",
        |_| "turn_stop.generic",
    );
    assert_eq!(
        stop.user_copy(&PartialCopyProvider),
        "my tool calls were not getting anywhere"
    );
}
