use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::*;
use crate::completions::{CompletionResult, RecordOutcome};
use crate::completions::{CompletionRouter, CompletionStatus, InMemoryCompletionStore, NotifyMode};
use crate::storage::{COMPLETIONS, task_id_key};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn record(task: &str, parent: &str) -> CompletionRecord {
    CompletionRecord::new(
        task,
        parent,
        "worker",
        CompletionStatus::Success,
        CompletionResult::text(format!("result of {task}")),
    )
}

fn docs_of(storage: &dyn StorageBackend, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// A router over the SQLite file at `path`, opened afresh (a simulated restart).
fn open_router(path: &std::path::Path) -> CompletionRouter {
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(path).unwrap();
    let store = DriverCompletionStore::new(docs_of(&storage, "local")).unwrap();
    CompletionRouter::new(Arc::new(store))
}

#[tokio::test]
async fn pending_completions_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path);
        router.record(record("t1", "p")).await.unwrap();
        router
            .record(record("t2", "p").with_notify_mode(NotifyMode::Off))
            .await
            .unwrap();
        router.record(record("t3", "other")).await.unwrap();
    }
    let router = open_router(&path);
    let mut pending = router.pending_for("p");
    pending.sort_by(|a, b| a.task_id.cmp(&b.task_id));
    assert_eq!(
        pending
            .iter()
            .map(|r| r.task_id.as_str())
            .collect::<Vec<_>>(),
        ["t1", "t2"]
    );
    assert_eq!(pending[0].result.text, "result of t1");
    let batch = router.claim_pending("p", 10).unwrap();
    assert_eq!(batch.len(), 1, "the redelivery path finds t1 again");
}

#[tokio::test]
async fn attempts_and_give_up_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path).with_max_attempts(2);
        router.record(record("t1", "p")).await.unwrap();
        router.claim_pending("p", 1).unwrap();
        router.mark_failed(&["t1"]).unwrap();
    }
    let router = open_router(&path).with_max_attempts(2);
    let batch = router.claim_pending("p", 1).unwrap();
    assert_eq!(batch[0].attempts, 2, "the earlier attempt was kept");
    assert_eq!(router.mark_failed(&["t1"]).unwrap().len(), 1);
    drop(router);
    let router = open_router(&path);
    assert!(router.pending_for("p").is_empty());
    assert!(router.claim_pending("p", 1).unwrap().is_empty());
}

#[tokio::test]
async fn delivered_and_tombstoned_stay_deduped_after_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path);
        router.record(record("done", "p")).await.unwrap();
        router.claim_pending("p", 1).unwrap();
        router.mark_delivered(&["done"]).unwrap();
        router.record(record("waited", "p")).await.unwrap();
        router.tombstone("waited").unwrap();
        router.tombstone("early").unwrap();
    }
    let router = open_router(&path);
    for task in ["done", "waited", "early"] {
        let outcome = router.record(record(task, "p")).await.unwrap();
        assert!(
            matches!(
                outcome,
                RecordOutcome::Duplicate | RecordOutcome::Suppressed
            ),
            "{task}: {outcome:?}"
        );
    }
    assert!(router.pending_for("p").is_empty());
}

#[tokio::test]
async fn a_settled_record_is_persisted_without_its_result() {
    let storage = MemoryStorage::new();
    let docs = docs_of(&storage, "local");
    let store = Arc::new(DriverCompletionStore::new(docs.clone()).unwrap());
    let router = CompletionRouter::new(store.clone()).with_max_attempts(1);
    let doc_of = |task: &str| {
        let docs = docs.clone();
        let id = task_id_key(task).unwrap();
        async move { docs.get(COMPLETIONS, &id).await.unwrap().unwrap().doc }
    };
    for (index, task) in ["delivered", "tombstoned", "gave-up", "cancelled"]
        .into_iter()
        .enumerate()
    {
        let mut record = record(task, "p");
        record.finished_at = UNIX_EPOCH + Duration::from_secs(index as u64 + 1);
        record.result.omitted_chars = 7;
        record.result.artifact = Some(crate::completions::CompletionArtifact {
            id: format!("art-{task}"),
            ..Default::default()
        });
        router.record(record).await.unwrap();
        let persisted = doc_of(task).await;
        assert_eq!(persisted["result"]["text"], format!("result of {task}"));
        assert_eq!(persisted["result"]["artifact"]["id"], format!("art-{task}"));
    }
    // Oldest first: the claim leases the first three; `cancelled` stays
    // unclaimed for `cancel_parent` to withdraw.
    let claimed = router.claim_pending("p", 3).unwrap();
    assert_eq!(claimed.len(), 3);
    router.mark_delivered(&["delivered"]).unwrap();
    assert_eq!(router.mark_failed(&["gave-up"]).unwrap().len(), 1);
    router.tombstone("tombstoned").unwrap();
    assert_eq!(router.cancel_parent("p").unwrap(), 1);
    for (task, state) in [
        ("delivered", "delivered"),
        ("tombstoned", "tombstoned"),
        ("gave-up", "gave_up"),
        ("cancelled", "tombstoned"),
    ] {
        let persisted = doc_of(task).await;
        assert_eq!(persisted["state"], state, "{task}");
        assert_eq!(persisted["taskId"], task, "{task}");
        assert_eq!(persisted["parentKey"], "p", "{task}");
        assert_eq!(persisted["result"]["text"], "", "{task}");
        assert_eq!(persisted["result"]["omitted_chars"], 0, "{task}");
        assert!(
            persisted["result"].get("artifact").is_none(),
            "{task}: {}",
            persisted["result"]
        );
        let read = store.get(task).unwrap();
        assert_eq!(read.result, CompletionResult::default(), "{task}");
        assert_eq!(read.task_id, task, "{task}");
    }
}

#[tokio::test]
async fn settled_records_reopen_without_their_result_and_stay_deduped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path).with_max_attempts(1);
        router.record(record("delivered", "p")).await.unwrap();
        router.record(record("gave-up", "p")).await.unwrap();
        router.record(record("waited", "p")).await.unwrap();
        router.record(record("live", "p")).await.unwrap();
        router.claim_pending("p", 2).unwrap();
        router.mark_delivered(&["delivered"]).unwrap();
        let gave_up = router.mark_failed(&["gave-up"]).unwrap();
        assert_eq!(gave_up[0].result.text, "result of gave-up");
        router.tombstone("waited").unwrap();
    }
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
    let store = Arc::new(DriverCompletionStore::new(docs_of(&storage, "local")).unwrap());
    let router = CompletionRouter::new(store.clone());
    let pending = router.pending_for("p");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].task_id, "live");
    assert_eq!(pending[0].result.text, "result of live");
    for (task, state) in [
        ("delivered", CompletionState::Delivered),
        ("gave-up", CompletionState::GaveUp),
        ("waited", CompletionState::Tombstoned),
    ] {
        let settled = store.get(task).unwrap();
        assert_eq!(settled.state, state, "{task}");
        assert_eq!(settled.result, CompletionResult::default(), "{task}");
        assert_eq!(
            settled.attempts,
            u32::from(state != CompletionState::Tombstoned),
            "{task}"
        );
        let outcome = router.record(record(task, "p")).await.unwrap();
        let expected = if state == CompletionState::Tombstoned {
            RecordOutcome::Suppressed
        } else {
            RecordOutcome::Duplicate
        };
        assert_eq!(outcome, expected, "{task}");
    }
    assert_eq!(router.claim_pending("p", 10).unwrap().len(), 1);
    assert_eq!(router.cancel_parent("p").unwrap(), 1);
    drop(router);
    let router = open_router(&path);
    assert!(router.pending_for("p").is_empty());
    assert_eq!(
        router.record(record("live", "p")).await.unwrap(),
        RecordOutcome::Suppressed,
        "a withdrawn record stays tombstoned across a reopen"
    );
}

#[tokio::test]
async fn the_store_keeps_the_latest_record_per_task() {
    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(docs_of(&storage, "local")).unwrap();
    let mut first = record("t1", "p");
    store.put(&first).unwrap();
    first.state = CompletionState::Delivered;
    first.attempts = 3;
    store.put(&first).unwrap();
    assert_eq!(store.list(None).len(), 1);
    let read = store.get("t1").unwrap();
    assert_eq!(read.state, CompletionState::Delivered);
    assert_eq!(read.attempts, 3);
    assert!(store.get("missing").is_none());
    store.put(&record("t2", "q")).unwrap();
    assert_eq!(store.list(Some("p")).len(), 1);
    assert_eq!(store.list(Some("nobody")).len(), 0);
    assert_eq!(store.list(None).len(), 2);
}

#[tokio::test]
async fn migration_listing_fails_on_corrupt_records_while_legacy_listing_skips_them() {
    use serde_json::json;
    use tinystoragedrivers_core::Precondition;

    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(docs_of(&storage, "local")).unwrap();
    store.put(&record("corrupt", "p")).unwrap();
    store
        .docs
        .put(
            COMPLETIONS,
            &task_id_key("corrupt").unwrap(),
            json!({
                "task_id": "corrupt",
                "parent_key": "p",
                "state": "pending",
                "updatedAtMs": 1,
                "record": {"taskId": "corrupt"}
            }),
            Precondition::None,
        )
        .await
        .unwrap();

    assert!(store.try_list(None).is_err());
    assert!(store.list(None).is_empty(), "legacy reads remain lenient");
}

#[tokio::test]
async fn compaction_drops_settled_records_only_and_keeps_pending_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    let router = open_router(&path);
    router.record(record("old", "p")).await.unwrap();
    router.claim_pending("p", 1).unwrap();
    router.mark_delivered(&["old"]).unwrap();
    router.record(record("live", "p")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(router.compact(Duration::from_millis(1)).unwrap(), 1);
    router.record(record("after", "p")).await.unwrap();
    drop(router);
    let router = open_router(&path);
    let mut ids: Vec<_> = router
        .pending_for("p")
        .into_iter()
        .map(|r| r.task_id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["after", "live"]);
    // A compacted record is gone, dedupe included.
    assert!(matches!(
        router.record(record("old", "p")).await.unwrap(),
        RecordOutcome::Recorded { .. }
    ));
}

/// A backend whose clock the test moves, since the update time is the driver's.
fn clocked_store() -> (DriverCompletionStore, Arc<std::sync::atomic::AtomicU64>) {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_700_000_000_000));
    let clock = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(std::sync::atomic::Ordering::SeqCst))
    };
    let storage = MemoryStorage::with_clock(clock);
    let store = DriverCompletionStore::new(docs_of(&storage, "local")).unwrap();
    (store, now)
}

#[test]
fn compaction_drops_settled_records_older_than_the_window_like_the_in_memory_store() {
    let (driver, now) = clocked_store();
    let memory = InMemoryCompletionStore::new();
    let real_now = SystemTime::now();
    let ms = |t: SystemTime| t.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let old = real_now - Duration::from_secs(60);
    let seed = |store: &dyn CompletionStore, at: SystemTime| {
        for (task, state) in [
            ("done", CompletionState::Delivered),
            ("gave-up", CompletionState::GaveUp),
            ("pending", CompletionState::Pending),
        ] {
            let mut r = record(task, "p");
            r.state = state;
            r.updated_at = at;
            store.put(&r).unwrap();
        }
    };
    now.store(ms(old), std::sync::atomic::Ordering::SeqCst);
    seed(&driver, old);
    seed(&memory, old);
    now.store(ms(real_now), std::sync::atomic::Ordering::SeqCst);
    let mut recent = record("recent", "p");
    recent.state = CompletionState::Delivered;
    driver.put(&recent).unwrap();
    memory.put(&recent).unwrap();
    assert_eq!(memory.compact(Duration::from_secs(1)).unwrap(), 2);
    assert_eq!(driver.compact(Duration::from_secs(1)).unwrap(), 2);
    for id in ["pending", "recent"] {
        assert!(memory.get(id).is_some() && driver.get(id).is_some(), "{id}");
    }
    for id in ["done", "gave-up"] {
        assert!(memory.get(id).is_none() && driver.get(id).is_none(), "{id}");
    }
}

#[tokio::test]
async fn a_cancelled_parent_stays_cancelled_after_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path);
        router.cancel_parent("p").unwrap();
    }
    let router = open_router(&path);
    assert_eq!(
        router.record(record("late", "p")).await.unwrap(),
        RecordOutcome::Suppressed
    );
    router.resume_parent("p");
    drop(router);
    let router = open_router(&path);
    assert!(matches!(
        router.record(record("later", "p")).await.unwrap(),
        RecordOutcome::Recorded { .. }
    ));
}

#[tokio::test]
async fn an_unreadable_record_is_skipped_without_hiding_the_rest() {
    use tinystoragedrivers_core::Precondition;
    let storage = MemoryStorage::new();
    let docs = docs_of(&storage, "local");
    let store = DriverCompletionStore::new(docs.clone()).unwrap();
    store.put(&record("good", "p")).unwrap();
    docs.put(
        "tasks.completions",
        "corrupt",
        serde_json::json!({"task_id": "bad", "parent_key": "p", "state": "pending", "nope": 1}),
        Precondition::None,
    )
    .await
    .unwrap();
    let listed = store.list(Some("p"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].task_id, "good");
}

#[tokio::test]
async fn scopes_keep_completions_apart() {
    let storage = MemoryStorage::new();
    let alice = DriverCompletionStore::new(docs_of(&storage, "alice")).unwrap();
    let bob = DriverCompletionStore::new(docs_of(&storage, "bob")).unwrap();
    alice.put(&record("t", "p")).unwrap();
    assert!(bob.get("t").is_none());
    assert!(bob.list(None).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sync_trait_works_from_a_multi_thread_runtime() {
    let store = DriverCompletionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    store.put(&record("mt", "p")).unwrap();
    assert!(store.get("mt").is_some());
}

#[tokio::test]
async fn completions_are_v2_documents_with_key_ids_and_metadata_update_times() {
    let storage = MemoryStorage::new();
    let docs = docs_of(&storage, "local");
    let store = DriverCompletionStore::new(docs.clone()).unwrap();
    store.put(&record("c/1", "p")).unwrap();
    let row = docs
        .get("tasks.completions", "c%2F1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.doc["task_id"], "c/1");
    assert_eq!(row.doc["parent_key"], "p");
    assert_eq!(row.doc["state"], "pending");
    assert!(row.doc["finished_ms"].as_i64().unwrap() > 1_600_000_000_000);
    for old in ["record", "updatedAtMs", "updated_at", "taskId"] {
        assert!(row.doc.get(old).is_none(), "{old}");
    }
    assert_eq!(row.meta.schema, 2);
    let read = store.get("c/1").unwrap();
    assert_eq!(
        read.updated_at,
        UNIX_EPOCH + Duration::from_millis(row.meta.updated_ms as u64)
    );
}
