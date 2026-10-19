use super::*;

#[test]
fn stop_summary_with_copy_preserves_placeholder_shaped_values() {
    let groups = [StopSummaryGroup {
        name: "tool_{count}_{error}".to_owned(),
        success: false,
        error: Some("failed {name}".to_owned()),
        count: 3,
    }];
    let copy = StopCopy {
        opening_template: "reason: {reason}\n",
        failure_template: "- `{name}` failed{count}{error}",
        ..ENGLISH_STOP_COPY
    };
    assert_eq!(
        render_stop_summary_with_copy("reason {count}", &groups, &copy),
        "reason: reason {count}\n\n**What happened**\n- `tool_{count}_{error}` failed 3 times: failed {name}\n\nTell me how you'd like to proceed, or ask me to try again."
    );
}

#[test]
fn stop_copy_keeps_transient_summary_golden() {
    let groups = [
        StopSummaryGroup {
            name: "get_profile".into(),
            success: true,
            error: None,
            count: 1,
        },
        StopSummaryGroup {
            name: "use_skill".into(),
            success: false,
            error: Some("tool `use_skill` timed out after 30000 ms".into()),
            count: 3,
        },
    ];
    assert_eq!(
        render_stop_summary_with_copy(
            "a service I needed kept timing out or was temporarily unavailable",
            &groups,
            &ENGLISH_STOP_COPY
        ),
        "I stopped this turn early because a service I needed kept timing out or was temporarily unavailable.\n\n**What happened**\n- `get_profile` succeeded\n- `use_skill` failed 3 times: tool `use_skill` timed out after 30000 ms\n\nTell me how you'd like to proceed, or ask me to try again."
    );
}

#[test]
fn stop_summary_groups_scrubbed_failures_and_keeps_the_newest_eight_rows() {
    let mut results = vec![CloseToolResult {
        name: "oldest".into(),
        success: true,
        content: "ok".into(),
    }];
    for index in 0..8 {
        results.push(CloseToolResult {
            name: format!("tool-{index}"),
            success: false,
            content: format!("secret-{index}"),
        });
    }
    results.extend([
        CloseToolResult {
            name: "repeat".into(),
            success: false,
            content: "credential: hidden".into(),
        },
        CloseToolResult {
            name: "repeat".into(),
            success: false,
            content: "credential: hidden".into(),
        },
    ]);
    let groups = group_stop_results(&results, |content| {
        content
            .replace("credential: hidden", "credential redacted")
            .replace("secret-", "redacted-")
    });
    assert_eq!(groups.last().unwrap().count, 2);
    assert_eq!(
        groups.last().unwrap().error.as_deref(),
        Some("credential redacted")
    );
    let output = render_stop_summary_with_copy("reason", &groups, &ENGLISH_STOP_COPY);
    assert!(!output.contains("oldest"));
    assert!(!output.contains("secret-"));
    assert!(output.contains("- …and 2 more tool call(s)"));
    assert!(output.contains("- `repeat` failed 2 times: credential redacted"));
}

#[test]
fn stop_summary_reproduces_the_production_repeated_timeout_golden() {
    let results = [
        CloseToolResult {
            name: "get_profile".into(),
            success: true,
            content: "profile loaded".into(),
        },
        CloseToolResult {
            name: "use_skill".into(),
            success: false,
            content: "tool `use_skill` timed out after 30000 ms".into(),
        },
        CloseToolResult {
            name: "use_skill".into(),
            success: false,
            content: "tool `use_skill` timed out after 30000 ms".into(),
        },
        CloseToolResult {
            name: "use_skill".into(),
            success: false,
            content: "tool `use_skill` timed out after 30000 ms".into(),
        },
    ];
    let groups = group_stop_results(&results, str::to_owned);
    assert_eq!(
        render_stop_summary_with_copy(
            "a service I needed kept timing out or was temporarily unavailable",
            &groups,
            &ENGLISH_STOP_COPY,
        ),
        "I stopped this turn early because a service I needed kept timing out or was temporarily unavailable.\n\n**What happened**\n- `get_profile` succeeded\n- `use_skill` failed 3 times: tool `use_skill` timed out after 30000 ms\n\nTell me how you'd like to proceed, or ask me to try again."
    );
}

#[test]
fn successful_repeats_are_reported_without_a_failure_claim() {
    let results = (0..3)
        .map(|_| CloseToolResult {
            name: "list_items".into(),
            success: true,
            content: "three items".into(),
        })
        .collect::<Vec<_>>();
    let groups = group_stop_results(&results, str::to_owned);
    let output =
        render_stop_summary_with_copy("the same step repeated", &groups, &ENGLISH_STOP_COPY);
    assert_eq!(
        output,
        "I stopped this turn early because the same step repeated.\n\n**What happened**\n- `list_items` succeeded 3 times\n\nTell me how you'd like to proceed, or ask me to try again."
    );
    assert!(!output.to_lowercase().contains("failed"));
    assert!(!output.contains("three items"));
}

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
