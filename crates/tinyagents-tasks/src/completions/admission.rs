//! Turn-queue admission tied to a durable parent lifecycle generation.

use std::sync::{Arc, Mutex};

use tinyagents_harness::run_queue::{QueueAdmission, QueueAdmissionError, QueueAdmissionPermit};

use super::CompletionStore;
use super::{CompletionRecord, CompletionRouter};

/// Retained admission capability for one queued completion message.
///
/// The harness checks this capability when the queue consumer applies the
/// message to a model turn. Parent stop, deletion, or reopen changes the
/// durable generation and rejects messages queued under an older generation.
#[derive(Clone)]
pub struct CompletionAdmission {
    store: Arc<Mutex<super::router::AdmissionStoreSlot>>,
    parent_key: String,
    parent_epoch: u64,
}

impl std::fmt::Debug for CompletionAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompletionAdmission")
            .field("parent_key", &self.parent_key)
            .field("parent_epoch", &self.parent_epoch)
            .finish_non_exhaustive()
    }
}

impl CompletionAdmission {
    /// Binds a queued completion to the lifecycle generation that recorded it.
    pub fn for_record(router: Arc<CompletionRouter>, record: &CompletionRecord) -> Self {
        Self::for_router(&router, record)
    }

    /// Binds a queued completion directly to its durable store and lifecycle.
    pub fn for_store(store: Arc<dyn CompletionStore>, record: &CompletionRecord) -> Self {
        Self {
            store: Arc::new(Mutex::new(super::router::AdmissionStoreSlot {
                store,
                migrating: false,
            })),
            parent_key: record.parent_key.clone(),
            parent_epoch: record.parent_epoch,
        }
    }

    /// Binds the admission to the router's live store slot so a queued item
    /// follows a successful durable-store migration.
    pub(crate) fn for_router(router: &CompletionRouter, record: &CompletionRecord) -> Self {
        Self {
            store: router.admission_store_handle(),
            parent_key: record.parent_key.clone(),
            parent_epoch: record.parent_epoch,
        }
    }

    fn key_for(&self, store: &Arc<dyn CompletionStore>) -> String {
        let identity = store
            .coordination_identity()
            .unwrap_or_else(|| format!("store:{:p}", Arc::as_ptr(store)));
        serde_json::to_string(&(identity, self.parent_key.as_str(), self.parent_epoch))
            .expect("serializing a completion admission batch key cannot fail")
    }
}

impl QueueAdmission for CompletionAdmission {
    fn acquire(&self) -> Result<Arc<dyn QueueAdmissionPermit>, QueueAdmissionError> {
        self.acquire_for(self.batch_key().as_deref())
    }

    fn acquire_for(
        &self,
        expected_key: Option<&str>,
    ) -> Result<Arc<dyn QueueAdmissionPermit>, QueueAdmissionError> {
        let store =
            CompletionRouter::admission_store_snapshot(&self.store).ok_or(QueueAdmissionError)?;
        if expected_key.is_some_and(|expected| self.key_for(&store) != expected) {
            return Err(QueueAdmissionError);
        }
        let permit = super::router::ParentAdmissionPermit::acquire(
            Arc::clone(&store),
            &self.parent_key,
            self.parent_epoch,
        )
        .map_err(|_| QueueAdmissionError)?
        .ok_or(QueueAdmissionError)?;
        Ok(Arc::new(permit))
    }

    fn retry_after_rejection(&self, expected_key: Option<&str>) -> bool {
        let slot = self
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slot.migrating || expected_key.is_some_and(|expected| self.key_for(&slot.store) != expected)
    }

    fn batch_key(&self) -> Option<String> {
        let store = CompletionRouter::admission_store_snapshot(&self.store)?;
        // Keep each component framed: scope and parent keys are caller-owned
        // strings and may contain any punctuation, including colons.
        Some(self.key_for(&store))
    }
}
