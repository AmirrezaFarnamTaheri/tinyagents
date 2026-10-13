//! Tests for [`DriverStatusStore`], run-id minting and the env redaction seed.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use super::*;
use crate::events::{AgentEvent, EventSink};
use crate::ids::{ComponentId, HarnessPhase, ThreadId};
use crate::observability::{
    AgentObservation, FanOutSink, HarnessEventJournal, JournalSink, RedactingSink,
    StoreEventJournal,
};
use crate::store::{AppendStore, DriverAppendStore};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn scoped(storage: &dyn StorageBackend, scope: &str) -> tinystoragedrivers_core::ScopedStorage {
    storage.for_scope(&Scope::new(scope).unwrap()).unwrap()
}

fn status_store() -> DriverStatusStore {
    DriverStatusStore::new(Arc::clone(scoped(&MemoryStorage::new(), "local").documents()))
}

fn journal_store(storage: &dyn StorageBackend) -> Arc<dyn AppendStore> {
    Arc::new(DriverAppendStore::new(Arc::clone(
        scoped(storage, "local").streams(),
    )))
}

#[test]
fn mint_run_id_is_slash_free_and_unique() {
    let a = mint_run_id();
    let b = mint_run_id();
    assert_ne!(a.as_str(), b.as_str());
    let hex = a.as_str().strip_prefix("run.").expect("run. prefix");
    assert_eq!(hex.len(), 32);
    assert!(hex.bytes().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn secrets_from_vars_masks_only_long_secretish_values() {
    let secrets = secrets_from_vars([
        ("OPENAI_API_KEY".to_string(), "sk-0123456789".to_string()),
        ("my_bearer".to_string(), "  abcdefgh  ".to_string()),
        ("SHORT_TOKEN".to_string(), "short".to_string()),
        ("HOME".to_string(), "/home/someone-long-path".to_string()),
    ]);
    assert_eq!(
        secrets,
        vec!["sk-0123456789".to_string(), "  abcdefgh  ".to_string()]
    );
}

/// A status written through the store reads back field for field, including
/// the timestamps and the optional thread link.
#[tokio::test]
async fn status_round_trips_field_for_field() {
    let store = status_store();
    let mut status = HarnessRunStatus::new(
        RunId::new("run.fixture"),
        ComponentId::new("mock-model".to_string()),
    )
    .with_thread(ThreadId::new("thread-1"));
    status.started_at = UNIX_EPOCH + Duration::from_secs(100);
    status.updated_at = UNIX_EPOCH + Duration::from_secs(100);
    store.put_status(status.clone()).await.unwrap();

    let back = store.get_status("run.fixture").await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&back).unwrap(),
        serde_json::to_value(&status).unwrap()
    );
    assert_eq!(back.thread_id.as_ref().unwrap().as_str(), "thread-1");
    assert_eq!(back.status, ExecutionStatus::Pending);
    assert!(store.get_status("run.other").await.unwrap().is_none());
}

#[tokio::test]
async fn status_store_round_trips_and_answers_queries() {
    let store = status_store();
    let run_id = mint_run_id();
    let mut status =
        HarnessRunStatus::new(run_id.clone(), ComponentId::new("mock-model".to_string()))
            .with_thread(ThreadId::new("thread-42"));
    status.mark_running(HarnessPhase::Model);
    store.put_status(status.clone()).await.unwrap();

    let active = store.list_active().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].status, ExecutionStatus::Running);
    assert_eq!(store.list_by_thread("thread-42").await.unwrap().len(), 1);
    assert!(store.list_by_thread("nope").await.unwrap().is_empty());

    status.mark_completed();
    store.put_status(status).await.unwrap();
    let by_root = store.list_by_root(run_id.as_str()).await.unwrap();
    assert_eq!(by_root.len(), 1);
    assert_eq!(by_root[0].status, ExecutionStatus::Completed);
    assert!(store.list_active().await.unwrap().is_empty());
}

#[tokio::test]
async fn undecodable_record_is_skipped_and_does_not_hide_valid_runs() {
    use tinystoragedrivers_core::Precondition;
    let storage = MemoryStorage::new();
    let docs = Arc::clone(scoped(&storage, "local").documents());
    let store = DriverStatusStore::new(docs.clone());
    let mut status = HarnessRunStatus::new(mint_run_id(), ComponentId::new("m".to_string()));
    status.mark_running(HarnessPhase::Model);
    store.put_status(status).await.unwrap();
    // A record from an incompatible build: right projections, wrong body.
    docs.put(
        DriverStatusStore::DEFAULT_COLLECTION,
        "bad",
        serde_json::json!({"runId": "bad", "rootRunId": "bad", "threadId": null, "active": true, "record": {"nope": 1}}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(store.list_active().await.unwrap().len(), 1);
}

#[tokio::test]
async fn runs_of_different_tenants_and_collections_never_meet() {
    let storage = MemoryStorage::new();
    let alice = DriverStatusStore::new(Arc::clone(scoped(&storage, "alice").documents()));
    let bob = DriverStatusStore::new(Arc::clone(scoped(&storage, "bob").documents()));
    let other = DriverStatusStore::with_collection(
        Arc::clone(scoped(&storage, "alice").documents()),
        "other_status",
    );
    let mut status = HarnessRunStatus::new(RunId::new("run.shared"), ComponentId::new("m"));
    status.mark_running(HarnessPhase::Model);
    alice.put_status(status).await.unwrap();
    assert!(bob.get_status("run.shared").await.unwrap().is_none());
    assert!(bob.list_active().await.unwrap().is_empty());
    assert!(other.get_status("run.shared").await.unwrap().is_none());
    assert_eq!(alice.list_active().await.unwrap().len(), 1);
}

#[tokio::test]
async fn any_run_id_is_a_valid_key_and_overwrites_in_place() {
    let store = status_store();
    for id in ["legacy/run", "with space", "ünï", "..", &"x".repeat(5000)] {
        let mut status = HarnessRunStatus::new(RunId::new(id), ComponentId::new("m"));
        store.put_status(status.clone()).await.unwrap();
        status.mark_running(HarnessPhase::Model);
        store.put_status(status).await.unwrap();
        let read = store.get_status(id).await.unwrap().expect("written");
        assert_eq!(read.status, ExecutionStatus::Running);
    }
    assert_eq!(store.list_active().await.unwrap().len(), 5, "one record each");
}

async fn read_all(
    storage: &dyn StorageBackend,
    run_id: &str,
    from: u64,
) -> Vec<AgentObservation> {
    StoreEventJournal::new(journal_store(storage))
        .read_from(run_id, from)
        .await
        .unwrap()
}

/// Journal persists, masks secrets and replays with stable ids.
#[tokio::test]
async fn journal_persists_masks_and_replays_run() {
    let storage = MemoryStorage::new();
    let run_id = mint_run_id();
    let journal: Arc<dyn HarnessEventJournal> =
        Arc::new(StoreEventJournal::new(journal_store(&storage)));
    let sink = EventSink::with_stream_id(run_id.as_str());
    let journal_sink = Arc::new(JournalSink::new(journal, run_id.clone()));
    let redacting = RedactingSink::new(journal_sink.clone(), vec!["sk-super-secret".into()]);
    sink.subscribe(Arc::new(FanOutSink::new().with(Arc::new(redacting))));

    sink.emit(AgentEvent::ModelStarted {
        call_id: "c1".into(),
        model: "sk-super-secret leaked here".to_string(),
    });
    sink.emit(AgentEvent::ToolStarted {
        parent_call_id: None,
        call_id: "c1".into(),
        tool_name: "echo".to_string(),
        input: None,
    });
    journal_sink.flush();

    let replayed = read_all(&storage, run_id.as_str(), 0).await;
    assert_eq!(replayed.len(), 2);
    for (offset, obs) in replayed.iter().enumerate() {
        assert_eq!(obs.offset, offset as u64);
        assert_eq!(
            obs.event_id.as_str(),
            format!("{}-evt-{offset}", run_id.as_str())
        );
    }
    if let AgentEvent::ModelStarted { model, .. } = &replayed[0].event {
        assert!(!model.contains("sk-super-secret"));
        assert!(model.contains("[REDACTED]"));
    } else {
        panic!("expected ModelStarted first");
    }
    let tail = read_all(&storage, run_id.as_str(), 1).await;
    assert_eq!(tail.len(), 1);
    assert!(matches!(tail[0].event, AgentEvent::ToolStarted { .. }));
}

/// Multi-byte UTF-8 payloads round-trip through the driver journal at every
/// padding around the old 4096-byte tail-read window (#5599), and offsets stay
/// dense.
#[tokio::test]
async fn journal_sink_handles_multibyte_utf8_payloads_at_every_padding() {
    for pad in 0..20usize {
        let storage = MemoryStorage::new();
        let run_id = mint_run_id();
        let journal: Arc<dyn HarnessEventJournal> =
            Arc::new(StoreEventJournal::new(journal_store(&storage)));
        let sink = EventSink::with_stream_id(run_id.as_str());
        let journal_sink = Arc::new(JournalSink::new(journal, run_id.clone()));
        sink.subscribe(Arc::new(FanOutSink::new().with(journal_sink.clone())));
        for i in 0..10 {
            sink.emit(AgentEvent::ModelStarted {
                call_id: format!("call-{i}").into(),
                model: "🦀".repeat(50),
            });
        }
        sink.emit(AgentEvent::ModelStarted {
            call_id: "call-10".into(),
            model: "x".repeat(pad),
        });
        sink.emit(AgentEvent::ToolStarted {
            parent_call_id: None,
            call_id: "call-11".into(),
            tool_name: "test_tool".to_string(),
            input: None,
        });
        journal_sink.flush();

        let replayed = read_all(&storage, run_id.as_str(), 0).await;
        assert_eq!(replayed.len(), 12, "pad {pad}");
        for (i, obs) in replayed.iter().enumerate() {
            assert_eq!(obs.offset, i as u64, "pad {pad}");
        }
        if let AgentEvent::ModelStarted { model, .. } = &replayed[0].event {
            assert_eq!(model, &"🦀".repeat(50), "pad {pad}");
        } else {
            panic!("expected ModelStarted first");
        }
    }
}

#[tokio::test]
async fn a_status_store_survives_a_reopened_sqlite_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("status.sqlite");
    {
        let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
        let store = DriverStatusStore::new(Arc::clone(scoped(&storage, "local").documents()));
        let mut status = HarnessRunStatus::new(RunId::new("run.durable"), ComponentId::new("m"))
            .with_thread(ThreadId::new("thread-d"));
        status.mark_running(HarnessPhase::Model);
        store.put_status(status).await.unwrap();
    }
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
    let store = DriverStatusStore::new(Arc::clone(scoped(&storage, "local").documents()));
    assert_eq!(store.list_by_thread("thread-d").await.unwrap().len(), 1);
    assert_eq!(store.list_active().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_journal_over_a_shared_append_store_handle_is_a_journal() {
    let shared: Arc<dyn crate::store::AppendStore> =
        Arc::new(crate::store::InMemoryAppendStore::new());
    let journal = StoreEventJournal::new(shared.clone());
    assert!(journal.read_from("run.x", 0).await.unwrap().is_empty());
    shared.append("s", serde_json::json!(1)).await.unwrap();
    assert_eq!(shared.read_window("s", 0, 5).await.unwrap().len(), 1);
    assert_eq!(shared.len("s").await.unwrap(), 1);
}
