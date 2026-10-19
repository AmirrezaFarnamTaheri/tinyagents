//! Turn-queue admission tied to a durable parent lifecycle generation.

use std::sync::Arc;

use tinyagents_harness::run_queue::{
    QueueAdmission, QueueAdmissionError, QueueAdmissionPermit,
};

use super::{CompletionRecord, CompletionRouter};
use super::CompletionStore;

/// Retained admission capability for one queued completion message.
///
/// The harness checks this capability when the queue consumer applies the
/// message to a model turn. Parent stop, deletion, or reopen changes the
/// durable generation and rejects messages queued under an older generation.
#[derive(Clone)]
pub struct CompletionAdmission {
    store: Arc<dyn CompletionStore>,
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
        Self::for_store(router.store(), record)
    }

    /// Binds a queued completion directly to its durable store and lifecycle.
    pub fn for_store(store: Arc<dyn CompletionStore>, record: &CompletionRecord) -> Self {
        Self {
            store,
            parent_key: record.parent_key.clone(),
            parent_epoch: record.parent_epoch,
        }
    }
}

impl QueueAdmission for CompletionAdmission {
    fn acquire(&self) -> Result<Arc<dyn QueueAdmissionPermit>, QueueAdmissionError> {
        super::router::ParentAdmissionPermit::acquire(
            Arc::clone(&self.store),
            &self.parent_key,
            self.parent_epoch,
        )
            .ok()
            .flatten()
            .map(|permit| Arc::new(permit) as Arc<dyn QueueAdmissionPermit>)
            .ok_or(QueueAdmissionError)
    }

    fn batch_key(&self) -> Option<String> {
        let identity = self.store.coordination_identity().unwrap_or_else(|| {
            format!("store:{:p}", Arc::as_ptr(&self.store))
        });
        // Keep each component framed: scope and parent keys are caller-owned
        // strings and may contain any punctuation, including colons.
        Some(
            serde_json::to_string(&(identity, self.parent_key.as_str(), self.parent_epoch))
                .expect("serializing a completion admission batch key cannot fail"),
        )
    }
}
