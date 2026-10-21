//! Host-owned admission for physical model calls.
//!
//! This API stays independent of repository, request, and provider details so
//! a host can share one policy across model wrappers and child agents.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use super::ModelBudget;

tokio::task_local! {
    static INVOCATION_DISPATCH_OUTCOME: Arc<Mutex<Option<DispatchRefusal>>>;
}

/// Runs one ChatModel invocation with its own typed admission outcome slot.
pub(crate) async fn with_dispatch_outcome_scope<F: Future>(
    future: F,
) -> (F::Output, Option<DispatchRefusal>) {
    let outcome = Arc::new(Mutex::new(None));
    let value = INVOCATION_DISPATCH_OUTCOME
        .scope(outcome.clone(), future)
        .await;
    let refusal = outcome
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    (value, refusal)
}

pub(super) fn record_invocation_refusal(refusal: DispatchRefusal) {
    let _ = INVOCATION_DISPATCH_OUTCOME.try_with(|outcome| {
        *outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(refusal);
    });
}

/// Optional policies attached to one physical model wrapper.
///
/// Admission remains independent of monetary accounting. `None` preserves
/// the existing unbounded dispatch behavior; an observer records only calls
/// that pass the final admission check.
#[derive(Clone, Default)]
pub struct DispatchModelOptions {
    /// Optional existing shared monetary budget.
    pub monetary_budget: Option<ModelBudget>,
    /// Optional terminal observer for admitted physical provider calls.
    pub observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
    /// Optional host-owned physical dispatch policy.
    pub admission: Option<Arc<dyn PhysicalDispatchAdmission>>,
}

impl DispatchModelOptions {
    /// Creates options with no monetary or physical dispatch policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the shared monetary budget while preserving the other policies.
    pub fn monetary_budget(mut self, budget: Option<ModelBudget>) -> Self {
        self.monetary_budget = budget;
        self
    }

    /// Sets the observer while preserving the other policies.
    pub fn observer(
        mut self,
        observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
    ) -> Self {
        self.observer = observer;
        self
    }

    /// Sets the host admission policy while preserving the other policies.
    pub fn admission(mut self, admission: Option<Arc<dyn PhysicalDispatchAdmission>>) -> Self {
        self.admission = admission;
        self
    }
}

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

/// An admitted model call's policy refusal or underlying inference error.
#[derive(Debug)]
pub enum ControlledDispatchError {
    /// Host policy declined the physical dispatch.
    Admission(DispatchRefusal),
    /// The dispatched model call failed.
    Model(tinyinference_llm::Error),
}

impl std::fmt::Display for ControlledDispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => std::fmt::Display::fmt(error, formatter),
            Self::Model(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for ControlledDispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Model(error) => Some(error),
        }
    }
}

impl From<DispatchRefusal> for ControlledDispatchError {
    fn from(refusal: DispatchRefusal) -> Self {
        Self::Admission(refusal)
    }
}

impl From<ControlledDispatchError> for tinyinference_llm::Error {
    fn from(error: ControlledDispatchError) -> Self {
        match error {
            ControlledDispatchError::Admission(refusal) => Self::Validation(refusal.to_string()),
            ControlledDispatchError::Model(error) => error,
        }
    }
}

/// Shared host policy that reserves a physical call slot before model work.
#[async_trait::async_trait]
pub trait PhysicalDispatchAdmission: Send + Sync {
    /// Reserves a prospective call slot, waiting only as permitted by the host
    /// policy. Acquiring a permit does not commit a physical call or dispatch
    /// to a provider.
    async fn acquire(&self) -> Result<Arc<dyn PhysicalDispatchPermit>, DispatchRefusal>;
}

/// A reservation for one prospective physical model call.
pub trait PhysicalDispatchPermit: Send + Sync {
    /// Rechecks host clock/cancellation and commits the reservation immediately
    /// before dispatch. This operation must be idempotent and must never count
    /// one permit twice. Dropping an uncommitted permit releases its reserved
    /// count slot; a committed permit remains held through call completion or
    /// stream completion, failure, EOF, or drop. Cloning the `Arc` does not
    /// release the slot until the final clone is dropped.
    fn mark_dispatched(&self) -> Result<(), DispatchRefusal>;

    /// Accounts a concrete physical provider send after `mark_dispatched`.
    ///
    /// This is called once for each native HTTP attempt, including provider
    /// warm-up and retry sends. The first send is covered by the committed
    /// invocation slot; hosts that enforce a physical-call ceiling should
    /// charge each later send here. The default preserves existing permit
    /// implementations that only limit logical invocations.
    fn mark_physical_attempt(&self) -> Result<(), DispatchRefusal> {
        Ok(())
    }
}
