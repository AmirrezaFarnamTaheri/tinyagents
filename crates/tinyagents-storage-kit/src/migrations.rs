//! Per-scope progress markers for owner migrations and legacy importers.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::{Precondition, Result, StorageError};

use crate::{DocumentStoreExt, Key, ScopedStorage};

/// Collection holding owner-local migration progress.
pub const MIGRATIONS_COLLECTION: &str = "tsd.migrations";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Progress {
    owner: String,
    step: String,
    done: bool,
}

/// Completion markers for owner migrations and one-time importers.
#[derive(Debug, Clone)]
pub struct Migrations {
    docs: Arc<dyn DocumentStore>,
}

impl Migrations {
    /// Bind migration markers to the supplied storage scope.
    pub fn new(storage: &ScopedStorage) -> Self {
        Self {
            docs: Arc::clone(storage.documents()),
        }
    }

    /// Check whether a named owner step already completed.
    pub async fn done(&self, owner: &str, step: &str) -> Result<bool> {
        let id = Key::of([owner, step])?;
        Ok(self
            .docs
            .get_as::<Progress>(MIGRATIONS_COLLECTION, id.as_str())
            .await?
            .is_some_and(|found| found.doc.done))
    }

    /// Mark a named owner step complete. Repeating the call is safe.
    pub async fn mark_done(&self, owner: &str, step: &str) -> Result<()> {
        let id = Key::of([owner, step])?;
        let progress = Progress {
            owner: owner.to_owned(),
            step: step.to_owned(),
            done: true,
        };
        self.docs
            .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new(
                MIGRATIONS_COLLECTION,
            ))
            .await?;
        let body = serde_json::to_value(progress).map_err(StorageError::from)?;
        self.docs
            .put(MIGRATIONS_COLLECTION, id.as_str(), body, Precondition::None)
            .await?;
        Ok(())
    }
}
