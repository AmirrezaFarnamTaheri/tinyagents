use super::*;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use tinystoragedrivers_core::{DocumentStore, CollectionSpec, Version, Versioned, Precondition, Page, ReadCheck, WriteOp, WriteResult};

#[derive(Debug)]
struct NoPostCommitReads { inner: Arc<dyn DocumentStore>, committed: AtomicBool }
type NativeResult<T> = std::result::Result<T, StorageError>;

#[async_trait::async_trait]
impl DocumentStore for NoPostCommitReads {
    async fn ensure_collection(&self, spec: &CollectionSpec) -> NativeResult<()> { self.inner.ensure_collection(spec).await }
    async fn get(&self, collection: &str, id: &str) -> NativeResult<Option<Versioned<Value>>> {
        if collection == storage::TELEMETRY && self.committed.load(Ordering::SeqCst) {
            return Err(StorageError::fenced("fixture expires reads immediately after committed write"));
        }
        self.inner.get(collection, id).await
    }
    async fn put(&self, collection: &str, id: &str, doc: Value, precondition: Precondition) -> NativeResult<Version> {
        let result = self.inner.put(collection, id, doc, precondition).await?;
        if collection == storage::AGENTS { self.committed.store(true, Ordering::SeqCst); } Ok(result)
    }
    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> NativeResult<bool> { self.inner.delete(collection, id, precondition).await }
    async fn query(&self, collection: &str, query: &Query) -> NativeResult<Page<Versioned<Value>>> { self.inner.query(collection, query).await }
    async fn count(&self, collection: &str, filter: &Filter) -> NativeResult<u64> { self.inner.count(collection, filter).await }
    async fn delete_where(&self, collection: &str, filter: &Filter) -> NativeResult<u64> { self.inner.delete_where(collection, filter).await }
    async fn claim(&self, collection: &str, filter: &Filter, sort: &[Sort], patch: &Value) -> NativeResult<Option<Versioned<Value>>> { self.inner.claim(collection, filter, sort, patch).await }
    async fn drop_collection(&self, collection: &str) -> NativeResult<()> { self.inner.drop_collection(collection).await }
    async fn atomic_batch_checked(&self, checks: Vec<ReadCheck>, writes: Vec<WriteOp>) -> NativeResult<Vec<WriteResult>> {
        let changed = writes.iter().any(|write| matches!(write, WriteOp::Put { collection, .. } if collection == storage::AGENTS));
        let result = self.inner.atomic_batch_checked(checks, writes).await?;
        if changed { self.committed.store(true, Ordering::SeqCst); } Ok(result)
    }
}

fn wrapped(native: &ScopedStorage) -> DriverRunLedger {
    DriverRunLedger::new(ScopedStorage::new(native.scope().clone(), "memory",
        Arc::new(NoPostCommitReads { inner: native.documents().clone(), committed: AtomicBool::new(false) }),
        native.streams().clone(), native.blobs().clone()))
}

#[tokio::test]
async fn agent_upsert_returns_its_commit_without_a_fallible_postcommit_telemetry_read() {
    let backend = MemoryStorage::new(); let native = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let ledger = wrapped(&native);
    assert!(ledger.upsert_agent_run(run("run")).await.is_ok());
    assert!(DriverRunLedger::new(native).get_agent_run("run").await.unwrap().is_some());
}

#[tokio::test]
async fn status_and_event_return_their_commit_without_a_fallible_postcommit_telemetry_read() {
    let backend = MemoryStorage::new(); let native = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    DriverRunLedger::new(native.clone()).upsert_agent_run(run("run")).await.unwrap();
    let ledger = wrapped(&native);
    assert!(ledger.transition_agent_run_status_from("run", AgentRunStatus::Running, AgentRunStatus::Completed, None, None,
        Some(RunEventAppend { run_id: "run".into(), event_type: "done".into(), payload: serde_json::json!({}) })).await.is_ok());
    assert_eq!(DriverRunLedger::new(native).get_agent_run("run").await.unwrap().unwrap().status, AgentRunStatus::Completed);
}
