use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::completions::{CompletionResult, RecordOutcome};
use crate::completions::{CompletionRouter, CompletionStatus, InMemoryCompletionStore, NotifyMode};
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
    use tinyagents_harness::driver_ids::hashed_id;
    use tinystoragedrivers_core::Precondition;

    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(docs_of(&storage, "local")).unwrap();
    store.put(&record("corrupt", "p")).unwrap();
    store
        .docs
        .put(
            &store.collection,
            &hashed_id(&["corrupt"]),
            json!({
                "taskId": "corrupt",
                "parentKey": "p",
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

#[test]
fn compaction_matches_the_in_memory_store() {
    fn seed(store: &dyn CompletionStore) {
        let mut done = record("done", "p");
        done.state = CompletionState::Delivered;
        done.updated_at -= Duration::from_secs(60);
        let mut gave_up = record("gave-up", "p");
        gave_up.state = CompletionState::GaveUp;
        gave_up.updated_at -= Duration::from_secs(60);
        let mut pending = record("pending", "p");
        pending.updated_at -= Duration::from_secs(60);
        let mut recent = record("recent", "p");
        recent.state = CompletionState::Delivered;
        for r in [&done, &gave_up, &pending, &recent] {
            store.put(r).unwrap();
        }
    }
    let memory = InMemoryCompletionStore::new();
    let driver = DriverCompletionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    seed(&memory);
    seed(&driver);
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
        DriverCompletionStore::DEFAULT_COLLECTION,
        "corrupt",
        serde_json::json!({"taskId": "bad", "parentKey": "p", "state": "pending", "updatedAtMs": 0, "record": {"nope": 1}}),
        Precondition::None,
    )
    .await
    .unwrap();
    let listed = store.list(Some("p"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].task_id, "good");
}

#[tokio::test]
async fn scopes_and_collections_keep_completions_apart() {
    let storage = MemoryStorage::new();
    let alice = DriverCompletionStore::new(docs_of(&storage, "alice")).unwrap();
    let bob = DriverCompletionStore::new(docs_of(&storage, "bob")).unwrap();
    alice.put(&record("t", "p")).unwrap();
    assert!(bob.get("t").is_none());
    assert!(bob.list(None).is_empty());
    let other = DriverCompletionStore::with_collection(
        docs_of(&storage, "alice"),
        Blocking::new().unwrap(),
        "another_completions",
    );
    assert!(other.get("t").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sync_trait_works_from_a_multi_thread_runtime() {
    let store = DriverCompletionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    store.put(&record("mt", "p")).unwrap();
    assert!(store.get("mt").is_some());
}
