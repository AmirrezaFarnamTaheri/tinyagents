//! Explicit asynchronous workflow persistence over an already-bound ledger.
//!
//! This contract does not choose a workspace, scope, backend, or fallback.
//! The legacy engine is not switched by constructing this adapter.

use super::{OrchestrationError, WorkflowChildRegistration};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tinyagents_session::run_ledger::{
    LedgerError, RunLedgerStore, WorkflowLeaseClaim, WorkflowRun, WorkflowRunUpsert,
};

/// Typed persistence or registration failure, retained until the host boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowPersistenceError {
    /// The authoritative ledger refused or failed an operation.
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    /// A legacy callback failed.
    #[error(transparent)]
    Legacy(#[from] OrchestrationError),
    /// A std duration cannot be represented by the ledger.
    #[error(transparent)]
    LeaseDuration(#[from] chrono::OutOfRangeError),
    /// Synchronous registration cannot persist to an asynchronous authority.
    #[error("authoritative workflow registration requires register_async")]
    AsyncRegistrationRequired,
}

/// Result returned by the authoritative asynchronous contract.
pub type WorkflowPersistenceResult<T> = Result<T, WorkflowPersistenceError>;

/// Minimal object-safe asynchronous workflow persistence required by an engine.
#[async_trait]
pub trait AsyncWorkflowStore: Send + Sync {
    /// Read one workflow from the admitted authority.
    async fn load(&self, id: &str) -> WorkflowPersistenceResult<Option<WorkflowRun>>;
    /// Create or merge a workflow, preserving ledger revision semantics.
    async fn upsert(&self, row: WorkflowRunUpsert) -> WorkflowPersistenceResult<WorkflowRun>;
    /// Acquire the durable lease or return its current claim outcome.
    async fn claim(
        &self,
        id: &str,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<WorkflowLeaseClaim>;
    /// Persist only while the expected revision and live owner still match.
    async fn compare_and_swap(
        &self,
        row: WorkflowRunUpsert,
        expected_revision: u64,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<Option<WorkflowRun>>;
    /// Renew only the matching unexpired owner lease.
    async fn renew(
        &self,
        id: &str,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<bool>;
}

/// Workflow adapter over a caller-admitted ledger handle.
///
/// Every operation uses that handle; there is no SQLite fallback, runtime
/// blocking bridge or ambient scope lookup. Engine wiring is a separate step.
pub struct LedgerWorkflowStore {
    ledger: Arc<dyn RunLedgerStore>,
}

impl LedgerWorkflowStore {
    /// Retain the explicit authoritative handle selected by the session owner.
    pub fn new(ledger: Arc<dyn RunLedgerStore>) -> Self {
        Self { ledger }
    }
}

#[async_trait]
impl AsyncWorkflowStore for LedgerWorkflowStore {
    async fn load(&self, id: &str) -> WorkflowPersistenceResult<Option<WorkflowRun>> {
        Ok(self.ledger.get_workflow_run(id).await?)
    }

    async fn upsert(&self, row: WorkflowRunUpsert) -> WorkflowPersistenceResult<WorkflowRun> {
        Ok(self.ledger.upsert_workflow_run(row).await?)
    }

    async fn claim(
        &self,
        id: &str,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<WorkflowLeaseClaim> {
        Ok(self
            .ledger
            .try_claim_workflow_run(id, owner, chrono::Duration::from_std(lease_for)?)
            .await?)
    }

    async fn compare_and_swap(
        &self,
        row: WorkflowRunUpsert,
        expected_revision: u64,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<Option<WorkflowRun>> {
        Ok(self
            .ledger
            .compare_and_swap_workflow_run(
                row,
                expected_revision,
                owner,
                chrono::Duration::from_std(lease_for)?,
            )
            .await?)
    }

    async fn renew(
        &self,
        id: &str,
        owner: &str,
        lease_for: Duration,
    ) -> WorkflowPersistenceResult<bool> {
        Ok(self
            .ledger
            .renew_workflow_run_lease(id, owner, chrono::Duration::from_std(lease_for)?)
            .await?)
    }
}

/// Authoritative registration that an async child executor must await.
///
/// The synchronous entrypoint refuses by default; a caller cannot mistake a
/// queued or omitted registration for a committed child identity.
#[async_trait]
pub trait AsyncWorkflowChildRegistration: Send + Sync {
    /// Commit the child identity before the executor proceeds to waiting.
    async fn register_async(&self, child_id: String) -> WorkflowPersistenceResult<()>;

    /// Refuse synchronous use of an asynchronous authority.
    fn register(&self, _child_id: String) -> WorkflowPersistenceResult<()> {
        Err(WorkflowPersistenceError::AsyncRegistrationRequired)
    }
}

/// Explicit adapter for an existing synchronous desktop registration callback.
pub struct LegacyWorkflowRegistration {
    inner: Arc<dyn WorkflowChildRegistration>,
}

impl LegacyWorkflowRegistration {
    /// Preserve a supplied legacy callback without choosing storage.
    pub fn new(inner: Arc<dyn WorkflowChildRegistration>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl AsyncWorkflowChildRegistration for LegacyWorkflowRegistration {
    async fn register_async(&self, child_id: String) -> WorkflowPersistenceResult<()> {
        Ok(self.inner.register(child_id)?)
    }

    fn register(&self, child_id: String) -> WorkflowPersistenceResult<()> {
        Ok(self.inner.register(child_id)?)
    }
}

#[cfg(test)]
#[path = "persistence_tests.rs"]
mod tests;
