//! Host-owned admission for physical model calls.
//!
//! This API stays independent of repository, request, and provider details so
//! a host can share one policy across model wrappers and child agents.

use std::sync::Arc;

/// A reason a host declined to admit or dispatch a physical model call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchRefusalReason {
    /// The host's absolute dispatch deadline has elapsed.
    Deadline,
    /// The host has no remaining physical call slots.
    CallLimit,
    /// The owning operation was cancelled.
    Cancelled,
}

/// A content-free refusal from a host's physical dispatch policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchRefusal {
    reason: DispatchRefusalReason,
}

impl DispatchRefusal {
    /// Creates a refusal with the supplied policy reason.
    pub const fn new(reason: DispatchRefusalReason) -> Self {
        Self { reason }
    }

    /// Returns the policy reason without exposing request content.
    pub const fn reason(&self) -> DispatchRefusalReason {
        self.reason
    }
}

impl std::fmt::Display for DispatchRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self.reason {
            DispatchRefusalReason::Deadline => "physical dispatch deadline elapsed",
            DispatchRefusalReason::CallLimit => "physical dispatch call limit reached",
            DispatchRefusalReason::Cancelled => "physical dispatch cancelled",
        })
    }
}

impl std::error::Error for DispatchRefusal {}

/// Shared host policy that reserves a physical call slot before model work.
#[async_trait::async_trait]
pub trait PhysicalDispatchAdmission: Send + Sync {
    /// Reserves a slot, waiting only as permitted by the host policy.
    async fn acquire(&self) -> Result<Arc<dyn PhysicalDispatchPermit>, DispatchRefusal>;
}

/// A reservation for one prospective physical model call.
pub trait PhysicalDispatchPermit: Send + Sync {
    /// Rechecks host policy and commits the reservation immediately before dispatch.
    fn mark_dispatched(&self) -> Result<(), DispatchRefusal>;
}
