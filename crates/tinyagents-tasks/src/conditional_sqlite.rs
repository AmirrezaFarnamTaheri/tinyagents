//! SQLite-backed conditional storage for lease-fencing regressions.

use std::sync::Arc;

use tempfile::TempDir;
use tinyagents_storage_kit::{
    DocumentStore, Scope, ScopedStorage, Storage, StorageBackend, StorageError, WriteCondition,
};
use tinystoragedrivers_connect::sqlite::SqliteConnection;
use tinystoragedrivers_document::sqlite::SqliteDocuments;

#[derive(Clone)]
pub(crate) struct MemoryStorage {
    _directory: Arc<TempDir>,
    connection: SqliteConnection,
}

impl MemoryStorage {
    pub(crate) fn new() -> Self {
        Self::with_clock(Arc::new(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        }))
    }

    pub(crate) fn with_clock(clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        let directory = Arc::new(tempfile::tempdir().expect("temporary storage directory"));
        let connection =
            SqliteConnection::open_with_clock(directory.path().join("storage.db"), clock)
                .expect("temporary sqlite storage opens");
        Self {
            _directory: directory,
            connection,
        }
    }

    pub(crate) fn open(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            _directory: Arc::new(tempfile::tempdir().expect("temporary storage directory")),
            connection: SqliteConnection::open(path).expect("sqlite storage opens"),
        }
    }
}

impl StorageBackend for MemoryStorage {
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage, StorageError> {
        let docs = Arc::new(SqliteDocuments::new(self.connection.clone(), scope.clone()))
            as Arc<dyn DocumentStore>;
        Ok(ScopedStorage::new(storage_with(docs), scope.clone()))
    }

    fn for_scope_conditioned(
        &self,
        scope: &Scope,
        condition: &WriteCondition,
    ) -> Result<ScopedStorage, StorageError> {
        let docs = Arc::new(SqliteDocuments::conditioned(
            self.connection.clone(),
            scope.clone(),
            condition.clone(),
        )) as Arc<dyn DocumentStore>;
        Ok(ScopedStorage::new(storage_with(docs), scope.clone()))
    }
}

fn storage_with(documents: Arc<dyn DocumentStore>) -> Storage {
    let unsupported = Storage::unsupported();
    Storage::new(
        documents,
        unsupported.streams().clone(),
        unsupported.kv().clone(),
        unsupported.blobs().clone(),
        unsupported.queues().clone(),
        unsupported.secrets().clone(),
    )
}
