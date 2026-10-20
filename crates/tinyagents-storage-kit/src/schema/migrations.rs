//! `tsd.migrations`: where the runner records step progress and where hosts
//! record the completion of legacy-file importers.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::backend::ScopedStorage;
use crate::document::{CollectionSpec, DocumentStore, DocumentStoreExt, Precondition};
use crate::error::Result;
use crate::key::Key;

/// The collection that holds one [`Progress`] document per step.
///
/// It lives in whatever scope the [`ScopedStorage`] given to
/// [`Registry::migrate`](super::Registry::migrate) or [`Migrations::new`] is
/// bound to, so progress is kept per scope: a tenant's scope records the
/// steps run over that tenant's data.
pub const MIGRATIONS_COLLECTION: &str = "tsd.migrations";

pub(super) fn migrations_spec() -> CollectionSpec {
    CollectionSpec::new(MIGRATIONS_COLLECTION)
}

/// The body of a `tsd.migrations` document: `{owner, step, from, to, cursor,
/// written, done}`. A runner step has the id `Key::of([owner, item, step])`;
/// an importer marked with [`Migrations::mark_done`] has `Key::of([owner,
/// step])` and `from` and `to` of 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// The owner of the step.
    pub owner: String,
    /// The step id.
    pub step: String,
    /// The version the step reads.
    pub from: u32,
    /// The version the step writes.
    pub to: u32,
    /// The keyset cursor after the last finished batch; `None` before the
    /// first batch and once the step is done.
    pub cursor: Option<String>,
    /// Records the step has written (or found already written).
    pub written: u64,
    /// Whether the step has completed.
    pub done: bool,
}

/// A handle on `tsd.migrations` in one scope.
///
/// Hosts use it for legacy-file importers, which have no source collection
/// for the runner to page: the importer runs, then calls
/// [`mark_done`](Self::mark_done); on the next start [`done`](Self::done)
/// says to skip it. This replaces the per-domain `*_import` marker
/// collections.
///
/// ```
/// use tinyagents_storage_kit::{Migrations, Scope, StorageBackend};
/// use tinystoragedrivers_testkit::memory_backend;
///
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// let storage = memory_backend().for_scope(&Scope::new("local")?)?;
/// let migrations = Migrations::new(&storage);
/// assert!(!migrations.done("threads", "import_v1").await?);
/// migrations.mark_done("threads", "import_v1").await?;
/// assert!(migrations.done("threads", "import_v1").await?);
/// # Ok::<_, tinyagents_storage_kit::error::StorageError>(())
/// # }).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct Migrations {
    docs: Arc<dyn DocumentStore>,
}

impl Migrations {
    /// A handle over `storage`'s scope. The collection must have been
    /// declared by [`Registry::prepare`](super::Registry::prepare) or
    /// [`Registry::migrate`](super::Registry::migrate).
    #[must_use]
    pub fn new(storage: &ScopedStorage) -> Self {
        Self {
            docs: Arc::clone(storage.documents()),
        }
    }

    /// Whether the importer `step` of `owner` has been marked done.
    ///
    /// # Errors
    ///
    /// Whatever the document store returns.
    pub async fn done(&self, owner: &str, step: &str) -> Result<bool> {
        let id = importer_key(owner, step)?;
        Ok(self.load(&id).await?.is_some_and(|found| found.done))
    }

    /// Record the importer `step` of `owner` as complete. Marking twice is
    /// fine.
    ///
    /// # Errors
    ///
    /// Whatever the document store returns.
    pub async fn mark_done(&self, owner: &str, step: &str) -> Result<()> {
        let id = importer_key(owner, step)?;
        self.save(
            &id,
            &Progress {
                owner: owner.to_owned(),
                step: step.to_owned(),
                from: 0,
                to: 0,
                cursor: None,
                written: 0,
                done: true,
            },
        )
        .await
    }

    pub(super) async fn load(&self, id: &Key) -> Result<Option<Progress>> {
        Ok(self
            .docs
            .get_as::<Progress>(MIGRATIONS_COLLECTION, id.as_str())
            .await?
            .map(|found| found.doc))
    }

    pub(super) async fn save(&self, id: &Key, progress: &Progress) -> Result<()> {
        self.docs
            .put_as(
                MIGRATIONS_COLLECTION,
                id.as_str(),
                progress,
                Precondition::None,
            )
            .await
            .map(|_| ())
    }
}

fn importer_key(owner: &str, step: &str) -> Result<Key> {
    Key::of([owner, step])
}

/// The id of a runner step's progress document.
pub(super) fn step_key(owner: &str, item: &str, step: &str) -> Result<Key> {
    Key::of([owner, item, step])
}
