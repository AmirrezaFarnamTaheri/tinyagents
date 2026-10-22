use super::*;

#[test]
fn polling_a_running_subagent_is_exempt_from_repeat_breakers() {
    assert!(is_repeat_call_exempt("wait_subagent"));
    assert!(!is_repeat_call_exempt("file_read"));
}

#[test]
fn default_thresholds_give_recoverable_failures_more_headroom() {
    let t = LoopGuardThresholds::default();
    assert_eq!(t.recoverable_repeat_failure, 8);
    assert_eq!(t.recoverable_no_progress_failure, 12);
}
