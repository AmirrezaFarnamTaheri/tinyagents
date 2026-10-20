//! Opt-in actual Mongo replica-set proof in an excluded standalone workspace.
//! Ordinary standalone runs ignore these cases; ignored results are not native
//! Mongo acceptance evidence. The root CI graph excludes this workspace.
//!
//! Set TINYAGENTS_TEST_MONGO_URL to an unauthenticated literal-loopback URI with
//! an explicit port, then enable mongodb-storage-tests and run this target with
//! --ignored. Every case uses a unique database; remove its owned test container
//! after the run. No production endpoint or credential is accepted.
#![cfg(feature = "mongodb-storage-tests")]

use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use tinyagents_session::run_ledger::{
    AgentRunKind, AgentRunStatus, AgentRunUpsert, DriverRunLedger, LedgerError, RunEventAppend,
    RunEventListRequest, RunLedgerStore,
    drivers::{QuiescentSource, SourceOwner},
};

use tinyagents_storage_kit::ScopedStorage;
use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend, StreamStore};
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_document::mongodb::{MongoDocuments, MongoStreams};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::{ErrorKind, Scope};

/// Validate the explicitly opted-in unauthenticated loopback Mongo endpoint.
fn endpoint() -> String {
    let value = std::env::var("TINYAGENTS_TEST_MONGO_URL")
        .expect("opt-in local Mongo tests require TINYAGENTS_TEST_MONGO_URL");
    let url = reqwest::Url::parse(&value).expect("local Mongo URI must be valid");
    assert_eq!(
        url.scheme(),
        "mongodb",
        "SRV and non-Mongo endpoints are forbidden"
    );
    assert!(
        matches!(url.host_str(), Some("127.0.0.1" | "[::1]")) && url.port().is_some(),
        "local Mongo requires literal loopback and an explicit port"
    );
    assert!(
        url.username().is_empty() && url.password().is_none(),
        "credentials are forbidden"
    );
    value
}
/// Create a UUID database and prepare its native Mongo schema.
async fn backend(scope: &Scope) -> (String, String, ScopedStorage) {
    let url = endpoint();
    let database = format!("tinyagents_ledger_{}", uuid::Uuid::new_v4().simple());
    let connection = MongoConnection::connect(&url, &database).await.unwrap();
    let documents =
        Arc::new(MongoDocuments::new(connection.clone(), scope.clone())) as Arc<dyn DocumentStore>;
    let streams = Arc::new(MongoStreams::new(connection, scope.clone())) as Arc<dyn StreamStore>;
    let memory = MemoryBackend::new().for_scope(scope).unwrap();
    let storage = Storage::new(
        documents,
        streams,
        memory.kv().clone(),
        memory.blobs().clone(),
        memory.queues().clone(),
        memory.secrets().clone(),
    );
    (url, database, ScopedStorage::new(storage, scope.clone()))
}
/// Build the event shared by the sequence-boundary import and append cases.
fn event() -> RunEventAppend {
    RunEventAppend {
        run_id: "run".into(),
        event_type: "tick".into(),
        payload: json!({}),
    }
}
/// Snapshot classic SQLite state at the requested sequence without mutating the snapshot.
fn source(ledger: &DriverRunLedger, root: &tempfile::TempDir, sequence: i64) -> QuiescentSource {
    tinyagents_session::run_ledger::upsert_agent_run(
        root.path(),
        AgentRunUpsert {
            id: "run".into(),
            kind: AgentRunKind::WorkerThread,
            parent_run_id: None,
            parent_thread_id: None,
            agent_id: Some("agent".into()),
            status: AgentRunStatus::Running,
            prompt_ref: None,
            worker_thread_id: None,
            checkpoint_path: None,
            checkpoint: None,
            summary: None,
            error: None,
            metadata: json!({}),
            started_at: Some(Utc::now()),
            completed_at: None,
        },
    )
    .unwrap();
    tinyagents_session::run_ledger::append_run_event(root.path(), event()).unwrap();
    let db = rusqlite::Connection::open(tinyagents_session::db_path(root.path())).unwrap();
    db.execute("UPDATE run_events SET sequence = ?1", [sequence])
        .unwrap();
    let snapshot = root.path().join("immutable-source.db");
    db.execute("VACUUM INTO ?1", [snapshot.to_str().unwrap()])
        .unwrap();
    drop(db);
    QuiescentSource::asserted_by_trusted_host(
        snapshot,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    )
}

#[tokio::test]
#[ignore = "local Mongo replica set: explicit loopback URL and --ignored required"]
async fn terminal_event_append_uses_the_actual_mongo_bson_converter() {
    let scope = Scope::new("alice").unwrap();
    let (_, _, storage) = backend(&scope).await;
    let ledger = DriverRunLedger::new(storage);
    let root = tempfile::tempdir().unwrap();
    let source = source(&ledger, &root, i64::MAX - 1);
    let bytes = std::fs::read(source.path()).unwrap();
    ledger
        .import_agent_family(&source, "append-boundary")
        .await
        .unwrap();
    assert_eq!(
        ledger.append_run_event(event()).await.unwrap().sequence,
        i64::MAX as u64
    );
    assert!(
        matches!(ledger.append_run_event(event()).await.unwrap_err(), LedgerError::Driver(error) if error.kind() == ErrorKind::Serialization)
    );
    let events = ledger
        .list_recent_run_events(&RunEventListRequest {
            run_id: "run".into(),
            after_sequence: Some(i64::MAX as u64 - 1),
            limit: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].sequence, i64::MAX as u64);
    assert_eq!(std::fs::read(source.path()).unwrap(), bytes);
    eprintln!("Actual Mongo terminal event proof completed");
}
#[tokio::test]
#[ignore = "local Mongo replica set: explicit loopback URL and --ignored required"]
async fn terminal_event_import_uses_the_actual_mongo_bson_converter() {
    let scope = Scope::new("alice").unwrap();
    let (url, database, storage) = backend(&scope).await;
    let ledger = DriverRunLedger::new(storage);
    let root = tempfile::tempdir().unwrap();
    let source = source(&ledger, &root, i64::MAX);
    let bytes = std::fs::read(source.path()).unwrap();
    ledger
        .import_agent_family(&source, "import-boundary")
        .await
        .unwrap();
    let reopened = MongoConnection::connect(&url, &database).await.unwrap();
    let documents =
        Arc::new(MongoDocuments::new(reopened.clone(), scope.clone())) as Arc<dyn DocumentStore>;
    let streams = Arc::new(MongoStreams::new(reopened, scope.clone())) as Arc<dyn StreamStore>;
    let memory = MemoryBackend::new().for_scope(&scope).unwrap();
    let peer_storage = ScopedStorage::new(
        Storage::new(
            documents,
            streams,
            memory.kv().clone(),
            memory.blobs().clone(),
            memory.queues().clone(),
            memory.secrets().clone(),
        ),
        scope.clone(),
    );
    let peer = DriverRunLedger::new(peer_storage);
    assert!(
        matches!(peer.append_run_event(event()).await.unwrap_err(), LedgerError::Driver(error) if error.kind() == ErrorKind::Serialization)
    );
    let events = peer
        .list_recent_run_events(&RunEventListRequest {
            run_id: "run".into(),
            after_sequence: Some(i64::MAX as u64 - 1),
            limit: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].sequence, i64::MAX as u64);
    assert_eq!(std::fs::read(source.path()).unwrap(), bytes);
    eprintln!("Actual Mongo terminal event proof completed");
}
