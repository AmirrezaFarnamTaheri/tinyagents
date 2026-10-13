//! Classify a failed harness run so the host can map it onto its own typed
//! error.
//!
//! The run's *own* definitively-non-provider failures (the model-call cap, a
//! spawn-depth limit, a timeout, a cancellation) must be decided **before** the
//! host consults the last provider error its model adapter saw. A run that
//! failed on the cap is not a provider error, so it must not be re-surfaced as
//! a stale one (wrong classification, wrong reporting suppression, wrong user
//! message). The host keeps the mapping onto its error type; this module owns
//! the ordering-sensitive decision.

use crate::error::TinyAgentsError;

/// How a failed run should be attributed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunFailureKind {
    /// The model-call cap was hit.
    ModelCallCap,
    /// A sub-agent depth or recursion limit was hit. Carries the limit.
    DepthLimit {
        /// The configured maximum depth.
        max_depth: usize,
    },
    /// A timeout or cancellation: the run's own failure. A provider error left
    /// over from an earlier attempt must be discarded, not reported as cause.
    TimeoutOrCancelled,
    /// Anything else: prefer the original typed provider error when one was
    /// recorded, else wrap this error.
    Other,
}

/// Classify the harness's run failure `error`.
#[must_use]
pub fn classify_run_failure(error: &TinyAgentsError) -> RunFailureKind {
    match error {
        TinyAgentsError::LimitExceeded(msg) if msg.contains("model call") => {
            RunFailureKind::ModelCallCap
        }
        TinyAgentsError::SubAgentDepth(max_depth) | TinyAgentsError::RecursionLimit(max_depth) => {
            RunFailureKind::DepthLimit {
                max_depth: *max_depth,
            }
        }
        TinyAgentsError::Timeout(_)
        | TinyAgentsError::CallTimeout(_)
        | TinyAgentsError::Cancelled => RunFailureKind::TimeoutOrCancelled,
        _ => RunFailureKind::Other,
    }
}

#[cfg(test)]
#[path = "turn_run_error_tests.rs"]
mod tests;
