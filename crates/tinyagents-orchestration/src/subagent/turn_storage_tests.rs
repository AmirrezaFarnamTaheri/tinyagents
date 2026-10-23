use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tinyagents_storage_kit::{
    CollectionSpec, DocumentStore, Leases, Precondition, Scope, Storage,
    StorageBackend as ScopedBackend, WriteCondition,
};
use tinystoragedrivers_connect::sqlite::SqliteConnection;
use tinystoragedrivers_document::sqlite::SqliteDocuments;

#[derive(Clone)]
struct SqliteBackend {
    connection: SqliteConnection,
}

impl ScopedBackend for SqliteBackend {
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

#[tokio::test]
async fn retained_session_documents_reject_expired_lease_after_takeover() {
    let directory = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(100));
    let clock = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst)) as Arc<dyn Fn() -> u64 + Send + Sync>
    };
    let connection =
        SqliteConnection::open_with_clock(directory.path().join("session.db"), clock).unwrap();
    let backend: Arc<dyn ScopedBackend> = Arc::new(SqliteBackend { connection });
    let lease_scope = Scope::new("cluster").unwrap();

    let lease_docs = backend.for_scope(&lease_scope).unwrap().documents().clone();
    lease_docs
        .ensure_collection(&CollectionSpec::new("session_probe"))
        .await
        .unwrap();
    let leases = Leases::new(&lease_docs).with_clock(Arc::new({
        let now = Arc::clone(&now);
        move || now.load(Ordering::SeqCst)
    }));
    let lease_a = leases
        .acquire("turn", "thread", "holder-a", 10)
        .await
        .unwrap()
        .unwrap();

    let adapter = Arc::new(
        FencedSessionBackend::new(
            Arc::clone(&backend),
            Scope::new("u:alice").unwrap(),
            lease_a.write_condition(lease_scope.clone()).unwrap(),
        )
        .unwrap(),
    );
    let stale_docs = adapter.for_agent("worker").unwrap().documents().clone();
    stale_docs
        .ensure_collection(&CollectionSpec::new("session_probe"))
        .await
        .unwrap();
    stale_docs
        .put(
            "session_probe",
            "stale",
            serde_json::json!({"holder":"a"}),
            Precondition::None,
        )
        .await
        .unwrap();

    now.store(110, Ordering::SeqCst);
    let lease_b = leases
        .acquire("turn", "thread", "holder-b", 30)
        .await
        .unwrap()
        .unwrap();
    let stale_write = stale_docs
        .put(
            "session_probe",
            "stale",
            serde_json::json!({"holder":"stale"}),
            Precondition::None,
        )
        .await;
    assert!(
        stale_write.is_err(),
        "expired holder's retained document handle must fail closed"
    );

    let current = FencedSessionBackend::new(
        backend,
        Scope::new("u:alice").unwrap(),
        lease_b.write_condition(lease_scope).unwrap(),
    )
    .unwrap();
    let current_docs = current.for_agent("worker").unwrap().documents().clone();
    current_docs
        .put(
            "session_probe",
            "current",
            serde_json::json!({"holder":"b"}),
            Precondition::None,
        )
        .await
        .unwrap();
}
