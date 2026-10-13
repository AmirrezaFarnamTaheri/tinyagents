use super::*;

#[test]
fn the_model_call_cap_is_not_a_provider_failure() {
    let error = TinyAgentsError::LimitExceeded("model call limit 10 reached".into());
    assert_eq!(classify_run_failure(&error), RunFailureKind::ModelCallCap);
}

#[test]
fn other_limits_are_not_the_model_call_cap() {
    let error = TinyAgentsError::LimitExceeded("tool call limit".into());
    assert_eq!(classify_run_failure(&error), RunFailureKind::Other);
}

#[test]
fn depth_and_recursion_limits_carry_the_maximum() {
    assert_eq!(
        classify_run_failure(&TinyAgentsError::SubAgentDepth(3)),
        RunFailureKind::DepthLimit { max_depth: 3 }
    );
    assert_eq!(
        classify_run_failure(&TinyAgentsError::RecursionLimit(5)),
        RunFailureKind::DepthLimit { max_depth: 5 }
    );
}

/// A provider error from an earlier attempt must not be re-surfaced as the
/// cause of a run that timed out or was cancelled.
#[test]
fn a_timeout_or_cancellation_is_the_runs_own_failure() {
    for error in [
        TinyAgentsError::CallTimeout("model call".to_string()),
        TinyAgentsError::Timeout("run".to_string()),
        TinyAgentsError::Cancelled,
    ] {
        assert_eq!(
            classify_run_failure(&error),
            RunFailureKind::TimeoutOrCancelled,
            "{error}"
        );
    }
}

#[test]
fn provider_failures_fall_through_to_the_recorded_error() {
    let error = TinyAgentsError::Validation("bad key".into());
    assert_eq!(classify_run_failure(&error), RunFailureKind::Other);
}
