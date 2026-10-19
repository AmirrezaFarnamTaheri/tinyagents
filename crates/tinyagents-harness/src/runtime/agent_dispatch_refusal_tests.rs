//! Regression tests for preserving physical dispatch refusals across the
//! hosted error boundary.

use super::{HostedError, HostedErrorKind, TimeoutBound, hosted_error};
use crate::cost::{DispatchRefusal, DispatchRefusalReason};
use crate::error::TinyAgentsError;
use crate::middleware::AgentRun;

#[test]
fn hosted_error_retains_content_free_dispatch_refusal() {
    let refusal = DispatchRefusal::new(DispatchRefusalReason::CallLimit);
    let hosted = hosted_error(
        &TinyAgentsError::PhysicalDispatchAdmission(refusal),
        AgentRun::new(),
    );

    assert_eq!(hosted.kind, HostedErrorKind::LimitExceeded);
    assert_eq!(hosted.dispatch_refusal, Some(refusal));
    assert_eq!(hosted.message, "hosted agent invocation exceeded a configured limit");
}

#[test]
fn hosted_error_round_trip_preserves_dispatch_refusal() {
    let refusal = DispatchRefusal::new(DispatchRefusalReason::Deadline);
    let hosted = HostedError {
        kind: HostedErrorKind::LimitExceeded,
        message: "sanitized limit failure".to_owned(),
        timeout_bound: None::<TimeoutBound>,
        dispatch_refusal: Some(refusal),
        run: None,
    };

    assert!(matches!(
        TinyAgentsError::from(hosted),
        TinyAgentsError::PhysicalDispatchAdmission(actual)
            if actual.reason() == DispatchRefusalReason::Deadline
    ));
}
