use super::*;

struct Copy;
impl FailurePolicy for Copy {
    fn identical_halt_summary(&self, tool: &str, count: u32, _t: &str) -> String {
        format!("identical:{tool}:{count}")
    }
    fn no_progress_halt_summary(&self, consecutive: u32, tool: &str, _t: &str) -> String {
        format!("streak:{tool}:{consecutive}")
    }
}

#[test]
fn identical_failures_halt_at_the_repeat_threshold() {
    let ladder = RecoverableFailureLadder::default();
    for _ in 0..7 {
        assert!(ladder.record(&Copy, "web_fetch", "a", "timeout").is_none());
    }
    assert_eq!(
        ladder.record(&Copy, "web_fetch", "a", "timeout").as_deref(),
        Some("identical:web_fetch:8")
    );
}

#[test]
fn typed_recoverable_halt_preserves_transient_class_and_operation() {
    let legacy = RecoverableFailureLadder::default();
    let typed = RecoverableFailureLadder::default();
    for _ in 0..7 {
        let _ = legacy.record(&Copy, "web_fetch", "a", "timeout");
        let _ = typed.record_with_cause(&Copy, "web_fetch", "a", "timeout");
    }
    let old_summary = legacy.record(&Copy, "web_fetch", "a", "timeout").unwrap();
    let cause = typed
        .record_with_cause(&Copy, "web_fetch", "a", "timeout")
        .unwrap();
    assert_eq!(cause.summary, old_summary);
    assert_eq!(cause.class, "transient");
    assert_eq!(cause.operation, "web_fetch");
}

#[test]
fn varied_failures_halt_at_the_streak_threshold() {
    let ladder = RecoverableFailureLadder::default();
    for i in 0..11 {
        assert!(ladder.record(&Copy, "t", &i.to_string(), "x").is_none());
    }
    assert_eq!(
        ladder.record(&Copy, "t", "last", "x").as_deref(),
        Some("streak:t:12")
    );
}

#[test]
fn reset_streak_keeps_identical_counts() {
    let ladder = RecoverableFailureLadder::default();
    for _ in 0..7 {
        ladder.record(&Copy, "t", "a", "x");
    }
    ladder.reset_streak();
    assert!(ladder.record(&Copy, "t", "a", "x").is_some());
}

#[test]
fn custom_thresholds_are_honoured() {
    let ladder = RecoverableFailureLadder::new(LoopGuardThresholds {
        recoverable_repeat_failure: 2,
        recoverable_no_progress_failure: 99,
    });
    assert!(ladder.record(&Copy, "t", "a", "x").is_none());
    assert!(ladder.record(&Copy, "t", "a", "x").is_some());
}

#[test]
fn full_reset_clears_identical_counts() {
    let ladder = RecoverableFailureLadder::new(LoopGuardThresholds {
        recoverable_repeat_failure: 2,
        recoverable_no_progress_failure: 99,
    });
    ladder.record(&Copy, "t", "a", "x");
    ladder.reset();
    assert!(ladder.record(&Copy, "t", "a", "x").is_none());
}

#[test]
fn text_predicates() {
    assert!(is_command_exit_report("  Command failed (exit 1)\nboom"));
    assert!(!is_command_exit_report("timeout"));
    assert!(is_pre_execution_failure("Failed to spawn process"));
    assert!(is_argument_rejection("Invalid arguments for tool"));
    assert!(!is_argument_rejection("connection reset"));
}

#[test]
fn first_error_line_adds_stderr_and_bounds_length() {
    assert_eq!(
        first_error_line("Command failed (exit 2)\n[stderr]\nno such program"),
        "Command failed (exit 2) — no such program"
    );
    assert_eq!(first_error_line("\n\n  boom \nmore"), "boom");
    assert_eq!(first_error_line(&"x".repeat(500)).chars().count(), 200);
}

#[test]
fn recovery_decision_is_const_constructible() {
    const D: RecoveryDecision = RecoveryDecision::new("transient", 2);
    assert_eq!(D.class, "transient");
    assert_eq!(D.budget, 2);
    assert_eq!(ARGUMENT_SCHEMA_RECOVERY, 3);
}
