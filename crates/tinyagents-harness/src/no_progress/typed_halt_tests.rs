use super::*;

fn failure<'a>(tool: &'a str, fp: &'a str, error: &'a str) -> ToolAttempt<'a> {
    ToolAttempt::failure(tool, fp, error)
}

#[test]
fn typed_halts_preserve_classes_operations_and_legacy_verdicts() {
    let legacy_policy = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    let typed_policy = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    let hard_reject = failure("shell", "same", "blocked").hard_reject();
    assert_eq!(
        typed_policy.record_with_cause(1, &hard_reject).verdict,
        legacy_policy.record(1, &hard_reject)
    );
    let legacy = legacy_policy.record(2, &hard_reject);
    let typed = typed_policy.record_with_cause(2, &hard_reject);
    assert_eq!(typed.verdict, legacy);
    assert_eq!(
        typed.failure_cause.as_ref().map(|cause| (
            cause.class.as_str(),
            cause.operation.as_str(),
            cause.summary.as_str()
        )),
        match &legacy {
            NoProgress::Halt(summary) => Some(("policy", "shell", summary.as_str())),
            other => panic!("expected policy halt, got {other:?}"),
        }
    );

    let legacy_identical = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    let typed_identical = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    for step in 1..DEFAULT_IDENTICAL_HALT_THRESHOLD {
        let attempt = failure("read_file", "a", "missing");
        assert_eq!(
            typed_identical.record_with_cause(step, &attempt).verdict,
            legacy_identical.record(step, &attempt)
        );
    }
    let attempt = failure("read_file", "a", "missing");
    let legacy = legacy_identical.record(DEFAULT_IDENTICAL_HALT_THRESHOLD, &attempt);
    let typed = typed_identical.record_with_cause(DEFAULT_IDENTICAL_HALT_THRESHOLD, &attempt);
    assert_eq!(typed.verdict, legacy);
    assert_eq!(
        typed
            .failure_cause
            .as_ref()
            .map(|cause| (cause.class.as_str(), cause.operation.as_str())),
        Some(("repeated_failure", "read_file"))
    );

    let legacy_varied = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    let typed_varied = NoProgressTracker::new(DEFAULT_IDENTICAL_HALT_THRESHOLD);
    for step in 1..=NO_PROGRESS_HALT_THRESHOLD {
        let fp = step.to_string();
        let attempt = failure("tool", &fp, "unavailable");
        let legacy = legacy_varied.record(step, &attempt);
        let typed = typed_varied.record_with_cause(step, &attempt);
        assert_eq!(typed.verdict, legacy);
        if step == NO_PROGRESS_HALT_THRESHOLD {
            assert_eq!(
                typed
                    .failure_cause
                    .as_ref()
                    .map(|cause| (cause.class.as_str(), cause.operation.as_str())),
                Some(("varied_failures", "tool"))
            );
        }
    }
}
