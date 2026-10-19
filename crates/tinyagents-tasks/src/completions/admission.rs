//! Turn-queue admission tied to a durable parent lifecycle generation.

use std::sync::Arc;

use tinyagents_harness::run_queue::{
    QueueAdmission, QueueAdmissionError, QueueAdmissionPermit,
};

use super::{CompletionRecord, CompletionRouter};

/// Retained admission capability for one queued completion message.
///
/// The harness checks this capability when the queue consumer applies the
/// message to a model turn. Parent stop, deletion, or reopen changes the
/// durable generation and rejects messages queued under an older generation.
#[derive(Clone)]
pub struct CompletionAdmission {
    router: Arc<CompletionRouter>,
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
        Self {
            router,
            parent_key: record.parent_key.clone(),
            parent_epoch: record.parent_epoch,
        }
    }
}

impl QueueAdmission for CompletionAdmission {
    fn acquire(&self) -> Result<Arc<dyn QueueAdmissionPermit>, QueueAdmissionError> {
        self.router
            .acquire_parent_generation(&self.parent_key, self.parent_epoch)
            .ok()
            .flatten()
            .ok_or(QueueAdmissionError)
    }

    fn batch_key(&self) -> Option<String> {
        self.router
            .parent_admission_batch_key(&self.parent_key, self.parent_epoch)
            .ok()
    }
}
