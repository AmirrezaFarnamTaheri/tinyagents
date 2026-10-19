use super::*;
use crate::completions::CompletionReceipt;
use crate::completions::{CompletionResult, RecordOutcome};
use crate::completions::{CompletionRouter, CompletionStatus, InMemoryCompletionStore, NotifyMode};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

fn receipts(records: &[CompletionRecord]) -> Vec<CompletionReceipt> {
    records
        .iter()
        .filter_map(CompletionRecord::receipt)
        .collect()
}

#[test]
fn stale_parent_admission_cannot_create_a_completion_after_takeover() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let store = DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap())
        .unwrap();
    let stale = store
        .acquire_parent_gate("parent", "router-a", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(store
        .release_parent_gate("parent", &stale)
        .unwrap());
    let current = store
        .acquire_parent_gate("parent", "router-b", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(current.epoch > stale.epoch);

    assert!(store
        .insert_if_absent(&record("late", "parent"), &stale)
        .is_err());
    assert!(store.get("late").is_none());
}

/// A router over the SQLite file at `path`, opened afresh (a simulated restart).
fn open_router(path: &std::path::Path) -> CompletionRouter {
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(path).unwrap();
    let store = DriverCompletionStore::new(
        Arc::new(storage),
        Scope::new("local").unwrap(),
    )
    .unwrap();
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
async fn two_routers_on_one_backend_do_not_claim_the_same_completion() {
    let storage = MemoryStorage::new();
    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    let first = CompletionRouter::new(Arc::new(
        DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap()).unwrap(),
    ));
    let second = CompletionRouter::new(Arc::new(
        DriverCompletionStore::new(backend, Scope::new("shared").unwrap()).unwrap(),
    ));
    first.record(record("shared-task", "parent")).await.unwrap();

    let first_claim = first.claim_pending("parent", 1).unwrap();
    let second_claim = second.claim_pending("parent", 1).unwrap();

    assert_eq!(first_claim.len(), 1);
    assert!(
        second_claim.is_empty(),
        "another router already owns this visible claim"
    );
}

#[tokio::test]
async fn a_live_push_claim_blocks_a_second_driver_router() {
    let storage = MemoryStorage::new();
    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    let first = CompletionRouter::new(Arc::new(
        DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap()).unwrap(),
    ));
    let second = CompletionRouter::new(Arc::new(
        DriverCompletionStore::new(backend, Scope::new("shared").unwrap()).unwrap(),
    ));
    first.attach_parent(
        "parent",
        Arc::new(tinyagents_harness::run_queue::RunQueue::<
            tinyinference_llm::message::Message,
        >::new()),
    );
    first.record(record("live-push", "parent")).await.unwrap();

    assert!(second.claim_pending("parent", 1).unwrap().is_empty());
}

#[test]
fn driver_claims_choose_oldest_before_task_id_tiebreaking() {
    use crate::completions::CompletionClaimMode;

    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let now = SystemTime::now();
    store
        .put(&record("z-older", "p").with_finished_at(now))
        .unwrap();
    store
        .put(&record("a-newer", "p").with_finished_at(now + Duration::from_secs(1)))
        .unwrap();

    let claimed = store
        .claim_pending(
            "p",
            CompletionClaimMode::Followup,
            1,
            "owner",
            now + Duration::from_secs(30),
        )
        .unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|record| record.task_id.as_str())
            .collect::<Vec<_>>(),
        ["z-older"]
    );
}

#[test]
fn conditional_driver_create_preserves_an_existing_claim() {
    use crate::completions::{ClaimResolution, CompletionClaimMode};

    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let original = record("claim-owner", "parent");
    assert!(store.insert_if_absent(&original).unwrap());
    let claimed = store
        .claim_task(
            "claim-owner",
            "parent",
            CompletionClaimMode::Followup,
            "owner-token",
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap()
        .unwrap();

    assert!(
        !store
            .insert_if_absent(&record("claim-owner", "other-parent"))
            .unwrap()
    );
    let persisted = store.get("claim-owner").unwrap();
    assert_eq!(persisted.parent_key, claimed.parent_key);
    assert_eq!(persisted.attempts, claimed.attempts);
    assert_eq!(persisted.receipt(), claimed.receipt());
    assert_eq!(persisted.result, claimed.result);
    let receipt = claimed.receipt().unwrap();
    assert_eq!(
        store
            .resolve_claim(&receipt.task_id, &receipt.token, ClaimResolution::Delivered)
            .unwrap()
            .unwrap()
            .state,
        CompletionState::Delivered
    );
}

#[test]
fn conditional_driver_create_does_not_replace_a_tombstone() {
    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let tombstone = CompletionRecord::tombstone_stub("deleted");
    store.put(&tombstone).unwrap();

    assert!(
        !store
            .insert_if_absent(&record("deleted", "parent"))
            .unwrap()
    );
    let persisted = store.get("deleted").unwrap();
    assert_eq!(persisted.task_id, tombstone.task_id);
    assert_eq!(persisted.state, CompletionState::Tombstoned);
    assert!(persisted.parent_key.is_empty());
}

#[tokio::test]
async fn expired_claim_can_be_reclaimed_and_stale_owner_cannot_settle_it() {
    use crate::completions::{ClaimResolution, CompletionClaimMode};

    let storage = MemoryStorage::new();
    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    let first = DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap()).unwrap();
    let second = DriverCompletionStore::new(backend, Scope::new("shared").unwrap()).unwrap();
    let record = record("lease-task", "parent");
    first.put(&record).unwrap();
    let now = SystemTime::now();
    let first_claim = first
        .claim_pending(
            "parent",
            CompletionClaimMode::Followup,
            1,
            "first",
            now - Duration::from_secs(1),
        )
        .unwrap()
        .pop()
        .unwrap();
    let first_token = first_claim.claim.unwrap().token;
    assert!(
        first
            .resolve_claim("lease-task", &first_token, ClaimResolution::Delivered)
            .unwrap()
            .is_none(),
        "an expired owner cannot settle before another router reclaims"
    );
    let second_claim = second
        .claim_pending(
            "parent",
            CompletionClaimMode::Followup,
            1,
            "second",
            now + Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(
        second_claim.len(),
        1,
        "the expired visibility lease is reclaimable"
    );
    let second_token = second_claim[0].claim.as_ref().unwrap().token.clone();

    assert!(
        first
            .resolve_claim("lease-task", &first_token, ClaimResolution::Delivered)
            .unwrap()
            .is_none()
    );
    let settled = second
        .resolve_claim("lease-task", &second_token, ClaimResolution::Delivered)
        .unwrap()
        .unwrap();
    assert_eq!(settled.state, CompletionState::Delivered);
}

#[tokio::test]
async fn attempts_and_give_up_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("completions.sqlite");
    {
        let router = open_router(&path).with_max_attempts(2);
        router.record(record("t1", "p")).await.unwrap();
        let batch = router.claim_pending("p", 1).unwrap();
        router.mark_failed(&receipts(&batch)).unwrap();
    }
    let router = open_router(&path).with_max_attempts(2);
    let batch = router.claim_pending("p", 1).unwrap();
    assert_eq!(batch[0].attempts, 2, "the earlier attempt was kept");
    assert_eq!(router.mark_failed(&receipts(&batch)).unwrap().len(), 1);
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
        let batch = router.claim_pending("p", 1).unwrap();
        router.mark_delivered(&receipts(&batch)).unwrap();
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
    let store = DriverCompletionStore::new(Arc::new(storage), Scope::new("local").unwrap()).unwrap();
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
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap()).unwrap();
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
    let batch = router.claim_pending("p", 1).unwrap();
    router.mark_delivered(&receipts(&batch)).unwrap();
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
    let store = DriverCompletionStore::new(Arc::new(storage), Scope::new("local").unwrap()).unwrap();
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
    let docs = Arc::clone(storage.for_scope(&Scope::new("local").unwrap()).unwrap().documents());
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap()).unwrap();
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
    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    let alice = DriverCompletionStore::new(Arc::clone(&backend), Scope::new("alice").unwrap()).unwrap();
    let bob = DriverCompletionStore::new(backend, Scope::new("bob").unwrap()).unwrap();
    alice.put(&record("t", "p")).unwrap();
    assert!(bob.get("t").is_none());
    assert!(bob.list(None).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sync_trait_works_from_a_multi_thread_runtime() {
    let store = DriverCompletionStore::new(Arc::new(MemoryStorage::new()), Scope::new("local").unwrap()).unwrap();
    store.put(&record("mt", "p")).unwrap();
    assert!(store.get("mt").is_some());
}

#[tokio::test]
async fn completions_are_v2_documents_with_key_ids_and_metadata_update_times() {
    let storage = MemoryStorage::new();
    let docs = Arc::clone(storage.for_scope(&Scope::new("local").unwrap()).unwrap().documents());
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap()).unwrap();
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
