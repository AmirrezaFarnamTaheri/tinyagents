use super::*;

#[test]
fn only_an_identical_re_issue_counts_as_a_repeated_failure() {
    let witness = RepeatLoopWitness::default();
    assert!(!witness.record_failure("file_read", "a", "No such file"));
    assert!(!witness.record_failure("file_read", "b", "No such file"));
    assert!(!witness.record_failure("grep", "a", "No such file"));
    assert!(witness.record_failure("file_read", "a", "No such file"));
}

#[test]
fn a_retry_that_fails_differently_is_not_a_repeat() {
    let witness = RepeatLoopWitness::default();
    assert!(!witness.record_failure("browser_task", "a", "Task ended at step 2"));
    assert!(!witness.record_failure("browser_task", "a", "Task ended at step 5"));
    // Only volatile spans differ: still the same outcome.
    assert!(!witness.record_failure("fetch", "u", "timed out after 31s"));
    assert!(witness.record_failure("fetch", "u", "timed out after 47s"));
}

#[test]
fn a_success_clears_the_refusal_count() {
    let witness = RepeatLoopWitness::default();
    assert_eq!(witness.record_refusal(), 1);
    assert_eq!(witness.record_refusal(), 2);
    witness.record_success();
    assert_eq!(witness.refusals(), 0);
}

#[test]
fn security_and_side_effect_classes_keep_their_own_wording() {
    for class in [
        "not_found",
        "missing_window",
        "validation",
        "invalid_arguments",
    ] {
        assert!(is_correctable_class(class), "{class}");
    }
    for class in [
        "policy",
        "blocked_by_policy",
        "authentication",
        "permission",
        "uncertain_side_effect",
        "transient",
        "unavailable",
    ] {
        assert!(!is_correctable_class(class), "{class}");
    }
}

#[test]
fn the_repeat_loop_note_names_the_repeating_and_the_last_error() {
    let note = repeat_loop_halt_summary(
        "file_read",
        2,
        "Failed to resolve path 'src/lbi.rs': No such file or directory (os error 2)",
    );
    assert!(note.contains("kept re-issuing tool calls"), "{note}");
    assert!(note.contains("refused 2 identical"), "{note}");
    assert!(note.contains("`file_read`"), "{note}");
    assert!(note.contains("src/lbi.rs"), "{note}");
}
