use super::*;

fn result_with_error(error: Option<&str>) -> ToolResult {
    error.map_or_else(|| ToolResult::success("some content"), ToolResult::error)
}

// ── OutcomeClass invariants ───────────────────────────────────────────────

#[test]
fn default_class_is_success() {
    assert_eq!(OutcomeClass::default(), OutcomeClass::Success);
}

#[test]
fn only_retryable_failure_is_retryable() {
    assert!(!OutcomeClass::Success.is_retryable());
    assert!(OutcomeClass::RetryableFailure.is_retryable());
    assert!(!OutcomeClass::PermanentFailure.is_retryable());
}

#[test]
fn success_and_failure_partition_the_enum() {
    for class in [
        OutcomeClass::Success,
        OutcomeClass::RetryableFailure,
        OutcomeClass::PermanentFailure,
    ] {
        assert_ne!(
            class.is_success(),
            class.is_failure(),
            "{class:?} must be exactly one of success/failure"
        );
    }
}

#[test]
fn class_round_trips_through_serde_as_snake_case() {
    let json = serde_json::to_string(&OutcomeClass::RetryableFailure).expect("serialize");
    assert_eq!(json, "\"retryable_failure\"");
    let back: OutcomeClass = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, OutcomeClass::RetryableFailure);
}

// ── ErrorFieldClassifier behaviour ────────────────────────────────────────

#[test]
fn absent_error_classifies_as_success() {
    let classifier = ErrorFieldClassifier::new();
    assert_eq!(
        classifier.classify("search", &result_with_error(None)),
        OutcomeClass::Success
    );
}

#[test]
fn present_error_classifies_as_permanent_failure() {
    let classifier = ErrorFieldClassifier::new();
    assert_eq!(
        classifier.classify("search", &result_with_error(Some("boom"))),
        OutcomeClass::PermanentFailure
    );
}

#[test]
fn empty_error_string_still_counts_as_a_failure() {
    // `Some("")` is a tool reporting failure without a message; the field's
    // presence is the signal, so it must not be mistaken for success.
    let classifier = ErrorFieldClassifier::new();
    assert_eq!(
        classifier.classify("search", &result_with_error(Some(""))),
        OutcomeClass::PermanentFailure
    );
}

#[test]
fn never_returns_retryable() {
    // Pinned deliberately: the safe-by-default choice is load-bearing, and
    // a host relying on it must not have retries appear under it silently.
    let classifier = ErrorFieldClassifier::new();
    for error in [None, Some("timed out"), Some("429 rate limited")] {
        assert!(
            !classifier
                .classify("any_tool", &result_with_error(error))
                .is_retryable()
        );
    }
}

#[test]
fn tool_name_does_not_change_the_verdict() {
    let classifier = ErrorFieldClassifier::new();
    let failed = result_with_error(Some("boom"));
    assert_eq!(
        classifier.classify("alpha", &failed),
        classifier.classify("omega", &failed)
    );
}

#[test]
fn content_blocks_are_ignored() {
    let classifier = ErrorFieldClassifier::new();
    let mut noisy = result_with_error(None);
    noisy.content = vec![tinytools::ToolContent::Json {
        data: serde_json::json!({ "status": 500, "message": "Error: everything is on fire" }),
    }];
    assert_eq!(
        classifier.classify("search", &noisy),
        OutcomeClass::Success,
        "only the reported-error flag is consulted"
    );
}

#[test]
fn usable_as_a_trait_object() {
    // The runtime holds this capability as `Option<Arc<dyn …>>`, so the
    // trait must stay object-safe.
    let classifier: std::sync::Arc<dyn ToolOutcomeClassifier> =
        std::sync::Arc::new(ErrorFieldClassifier);
    assert_eq!(
        classifier.classify("search", &result_with_error(Some("nope"))),
        OutcomeClass::PermanentFailure
    );
}

#[test]
fn status_classifier_uses_typed_status_precedence_and_conservative_retryability() {
    let classifier = ToolStatusClassifier;
    assert_eq!(
        classifier.classify(
            "tool",
            &ToolResult::error("[policy-denied] approval timed out")
        ),
        OutcomeClass::PermanentFailure,
        "a refusal marker must win over timeout wording"
    );
    assert_eq!(
        classifier.classify("tool", &ToolResult::error("service unavailable")),
        OutcomeClass::RetryableFailure
    );
    assert_eq!(
        classifier.classify(
            "tool",
            &ToolResult::error("could not reach the model endpoint")
        ),
        OutcomeClass::RetryableFailure
    );
    assert_eq!(
        classifier.classify("tool", &ToolResult::error("timed out")),
        OutcomeClass::PermanentFailure,
        "the classifier cannot know whether repeating a timed-out effect is safe"
    );
    assert_eq!(
        classifier.classify("tool", &ToolResult::error("unclassified failure")),
        OutcomeClass::PermanentFailure
    );
    assert_eq!(
        classifier.classify("tool", &ToolResult::success("an error was found")),
        OutcomeClass::Success
    );
}

#[test]
fn status_classifier_can_retry_timeouts_only_for_host_approved_tools() {
    let classifier = ToolStatusClassifier.with_retry_safe_timeouts(|name| name == "safe_read");

    assert_eq!(
        classifier.classify("safe_read", &ToolResult::error("timed out")),
        OutcomeClass::RetryableFailure
    );
    assert_eq!(
        classifier.classify("unsafe_write", &ToolResult::error("timed out")),
        OutcomeClass::PermanentFailure
    );
    assert_eq!(
        ToolStatusClassifier.classify("safe_read", &ToolResult::error("timed out")),
        OutcomeClass::PermanentFailure,
        "the existing unit classifier keeps its conservative default"
    );
}

#[test]
fn status_classifier_uses_the_dispatched_name_and_only_consults_timeout_policy_for_timeouts() {
    let consulted = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let consulted_by_callback = consulted.clone();
    let classifier = ToolStatusClassifier.with_retry_safe_timeouts(move |name| {
        consulted_by_callback
            .lock()
            .expect("callback log")
            .push(name.to_owned());
        name == "dispatched_safe_read"
    });

    assert_eq!(
        classifier.classify("dispatched_safe_read", &ToolResult::error("timed out")),
        OutcomeClass::RetryableFailure
    );
    assert_eq!(
        classifier.classify("service", &ToolResult::error("service unavailable")),
        OutcomeClass::RetryableFailure
    );
    assert_eq!(
        classifier.classify("success", &ToolResult::success("timed out")),
        OutcomeClass::Success
    );
    assert_eq!(
        classifier.classify(
            "policy",
            &ToolResult::error("[policy-denied] approval timed out")
        ),
        OutcomeClass::PermanentFailure
    );
    assert_eq!(
        *consulted.lock().expect("callback log"),
        ["dispatched_safe_read"]
    );
}
