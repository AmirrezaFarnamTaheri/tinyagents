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

fn parent_gate(
    store: &dyn CompletionStore,
    parent_key: &str,
) -> crate::completions::ParentGateClaim {
    store
        .acquire_parent_gate(parent_key, "driver-test-owner", Duration::from_secs(30))
        .unwrap()
        .unwrap()
}

fn put_record(store: &dyn CompletionStore, record: &CompletionRecord) {
    let parent_key = if record.parent_key.is_empty() {
        format!("completion-task:{}", record.task_id)
    } else {
        record.parent_key.clone()
    };
    let gate = parent_gate(store, &parent_key);
    store.put_fenced(record, &parent_key, &gate).unwrap();
}

#[test]
fn stale_parent_admission_cannot_create_a_completion_after_takeover() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let store =
        DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap()).unwrap();
    let stale = store
        .acquire_parent_gate("parent", "router-a", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(store.release_parent_gate("parent", &stale).unwrap());
    let current = store
        .acquire_parent_gate("parent", "router-b", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(current.epoch > stale.epoch);

    assert!(store
        .insert_if_absent(&record("late", "parent"), &current)
        .unwrap());
    assert!(store
        .insert_if_absent(&record("late", "other-parent"), &stale)
        .is_err());
    let preserved = store.get("late").unwrap();
    assert_eq!(preserved.parent_key, "parent");

    assert!(store
        .insert_if_absent(&record("stale-create", "parent"), &stale)
        .is_err());
    assert!(store.get("stale-create").is_none());
}

#[test]
fn in_memory_store_rejects_mutations_from_a_released_parent_gate() {
    let store = InMemoryCompletionStore::new();
    let stale = store
        .acquire_parent_gate("parent", "router-a", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(store.release_parent_gate("parent", &stale).unwrap());
    let current = store
        .acquire_parent_gate("parent", "router-b", Duration::from_secs(30))
        .unwrap()
        .unwrap();

    let mut accepted = record("same-task", "parent");
    accepted.result = CompletionResult::text("current owner");
    store.put_fenced(&accepted, "parent", &current).unwrap();

    let mut stale_update = accepted.clone();
    stale_update.result = CompletionResult::text("stale owner");
    assert!(store.put_fenced(&stale_update, "parent", &stale).is_err());
    assert_eq!(
        store.get("same-task").unwrap().result,
        CompletionResult::text("current owner")
    );
    assert!(store
        .insert_if_absent(&record("stale-create", "parent"), &stale)
        .is_err());
    assert!(store.get("stale-create").is_none());
}

#[tokio::test]
async fn a_claim_prepared_before_takeover_cannot_land_after_the_new_epoch() {
    let store = DriverCompletionStore::new(
        Arc::new(MemoryStorage::new()),
        Scope::new("shared").unwrap(),
    )
    .unwrap();
    let stale = store
        .acquire_parent_gate("parent", "router-a", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    let stale_docs = store.fenced_docs("parent", &stale).await.unwrap();

    assert!(store.release_parent_gate("parent", &stale).unwrap());
    let current = store
        .acquire_parent_gate("parent", "router-b", Duration::from_secs(30))
        .unwrap()
        .unwrap();
    let pending = record("late-claim", "parent");
    assert!(store.insert_if_absent(&pending, &current).unwrap());

    let id = task_id_key(&pending.task_id).unwrap();
    let row = stale_docs.get(COMPLETIONS, &id).await.unwrap().unwrap();
    let mut claimed = decode_strict(row.clone()).unwrap();
    claimed.attempts = 1;
    claimed.claim = Some(crate::completions::CompletionClaim {
        token: "router-a:late-claim".into(),
        visible_after: SystemTime::now() + Duration::from_secs(30),
    });
    let error = stale_docs
        .put_with(
            COMPLETIONS,
            &id,
            serde_json::to_value(CompletionDoc::from_record(&claimed)).unwrap(),
            Precondition::Version(row.version),
            WriteOptions::new().schema(SCHEMA_VERSION),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Fenced);
    let persisted = store.get(&pending.task_id).unwrap();
    assert_eq!(persisted.attempts, 0);
    assert!(persisted.claim.is_none());
}

#[tokio::test]
async fn closed_parent_markers_survive_compaction_and_a_router_restart() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let open = || {
        Arc::new(
            DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap())
                .unwrap(),
        ) as Arc<dyn CompletionStore>
    };
    let first = CompletionRouter::new(open());
    first.cancel_parent("cancelled-thread").unwrap();
    first.delete_parent("deleted-thread").unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(first.compact(Duration::ZERO).unwrap(), 0);

    let restarted = CompletionRouter::new(open());
    assert_eq!(
        restarted
            .record(record("after-delete", "deleted-thread"))
            .await
            .unwrap(),
        RecordOutcome::Suppressed
    );
    assert_eq!(
        restarted
            .record(record("after-stop", "cancelled-thread"))
            .await
            .unwrap(),
        RecordOutcome::Suppressed
    );
    assert!(restarted.pending_for("deleted-thread").is_empty());
}

#[tokio::test]
async fn resuming_an_open_parent_preserves_its_generation_and_pending_work() {
    let store = Arc::new(InMemoryCompletionStore::new());
    let router = CompletionRouter::new(store.clone());
    router.record(record("pending", "parent")).await.unwrap();
    let before = store.get("pending").unwrap().parent_epoch;

    router.resume_parent("parent");

    assert!(router.parent_generation_admitted("parent", before).unwrap());
    assert_eq!(router.claim_pending("parent", 1).unwrap().len(), 1);
}

#[tokio::test]
async fn deleted_parent_cannot_be_reopened_by_resume() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    router.delete_parent("deleted").unwrap();

    router.resume_parent("deleted");

    assert!(!router.admit_child("deleted").unwrap());
    assert_eq!(
        router
            .record(record("after-delete", "deleted"))
            .await
            .unwrap(),
        RecordOutcome::Suppressed
    );
}

#[tokio::test]
async fn queued_completion_generation_is_rejected_after_cross_router_close_and_reopen() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let open = || {
        Arc::new(
            DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap())
                .unwrap(),
        ) as Arc<dyn CompletionStore>
    };
    let delivery_router = CompletionRouter::new(open());
    let lifecycle_router = CompletionRouter::new(open());
    delivery_router
        .record(record("queued", "parent"))
        .await
        .unwrap();
    let claimed = delivery_router.claim_pending("parent", 1).unwrap();
    let stale_receipt = claimed[0].receipt().unwrap();

    lifecycle_router.cancel_parent("parent").unwrap();
    lifecycle_router.resume_parent("parent");
    assert!(!delivery_router
        .parent_generation_admitted("parent", stale_receipt.parent_epoch)
        .unwrap());
    assert_eq!(
        delivery_router
            .mark_delivered(std::slice::from_ref(&stale_receipt))
            .unwrap(),
        0
    );
    assert_eq!(
        delivery_router
            .record(record("new-generation", "parent"))
            .await
            .unwrap(),
        RecordOutcome::Recorded {
            lane: None,
            receipt: None
        }
    );
    assert!(delivery_router.pending_for("parent").iter().any(|item| {
        item.task_id == "new-generation" && item.parent_epoch > stale_receipt.parent_epoch
    }));
}

/// A router over the SQLite file at `path`, opened afresh (a simulated restart).
fn open_router(path: &std::path::Path) -> CompletionRouter {
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(path).unwrap();
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("local").unwrap()).unwrap();
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
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let now = SystemTime::now();
    put_record(&store, &record("z-older", "p").with_finished_at(now));
    put_record(
        &store,
        &record("a-newer", "p").with_finished_at(now + Duration::from_secs(1)),
    );

    let gate = parent_gate(&store, "p");
    let claimed = store
        .claim_pending(
            "p",
            0,
            &gate,
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
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let original = record("claim-owner", "parent");
    let gate = parent_gate(&store, "parent");
    assert!(store.insert_if_absent(&original, &gate).unwrap());
    let claimed = store
        .claim_task(
            "claim-owner",
            "parent",
            0,
            &gate,
            CompletionClaimMode::Followup,
            "owner-token",
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap()
        .unwrap();

    let other_parent_gate = parent_gate(&store, "other-parent");
    assert!(!store
        .insert_if_absent(&record("claim-owner", "other-parent"), &other_parent_gate,)
        .unwrap());
    let persisted = store.get("claim-owner").unwrap();
    assert_eq!(persisted.parent_key, claimed.parent_key);
    assert_eq!(persisted.attempts, claimed.attempts);
    assert_eq!(persisted.receipt(), claimed.receipt());
    assert_eq!(persisted.result, claimed.result);
    let receipt = claimed.receipt().unwrap();
    assert_eq!(
        store
            .resolve_claim(
                &receipt.task_id,
                "parent",
                receipt.parent_epoch,
                &gate,
                &receipt.token,
                ClaimResolution::Delivered,
            )
            .unwrap()
            .unwrap()
            .state,
        CompletionState::Delivered
    );
}

#[test]
fn conditional_driver_create_does_not_replace_a_tombstone() {
    let storage = MemoryStorage::new();
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("shared").unwrap()).unwrap();
    let tombstone = CompletionRecord::tombstone_stub("deleted");
    put_record(&store, &tombstone);
    let gate = parent_gate(&store, "parent");

    assert!(!store
        .insert_if_absent(&record("deleted", "parent"), &gate)
        .unwrap());
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
    let first =
        DriverCompletionStore::new(Arc::clone(&backend), Scope::new("shared").unwrap()).unwrap();
    let second = DriverCompletionStore::new(backend, Scope::new("shared").unwrap()).unwrap();
    let record = record("lease-task", "parent");
    put_record(&first, &record);
    let now = SystemTime::now();
    let first_gate = parent_gate(&first, "parent");
    let first_claim = first
        .claim_pending(
            "parent",
            0,
            &first_gate,
            CompletionClaimMode::Followup,
            1,
            "first",
            now - Duration::from_secs(1),
        )
        .unwrap()
        .pop()
        .unwrap();
    first.release_parent_gate("parent", &first_gate).unwrap();
    let first_token = first_claim.claim.unwrap().token;
    let second_gate = parent_gate(&second, "parent");
    assert!(
        first
            .resolve_claim(
                "lease-task",
                "parent",
                first_claim.parent_epoch,
                &second_gate,
                &first_token,
                ClaimResolution::Delivered,
            )
            .unwrap()
            .is_none(),
        "an expired owner cannot settle before another router reclaims"
    );
    let second_claim = second
        .claim_pending(
            "parent",
            0,
            &second_gate,
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

    assert!(first
        .resolve_claim(
            "lease-task",
            "parent",
            first_claim.parent_epoch,
            &second_gate,
            &first_token,
            ClaimResolution::Delivered,
        )
        .unwrap()
        .is_none());
    let settled = second
        .resolve_claim(
            "lease-task",
            "parent",
            second_claim[0].parent_epoch,
            &second_gate,
            &second_token,
            ClaimResolution::Delivered,
        )
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
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("local").unwrap()).unwrap();
    let mut first = record("t1", "p");
    put_record(&store, &first);
    first.state = CompletionState::Delivered;
    first.attempts = 3;
    put_record(&store, &first);
    assert_eq!(store.list(None).len(), 1);
    let read = store.get("t1").unwrap();
    assert_eq!(read.state, CompletionState::Delivered);
    assert_eq!(read.attempts, 3);
    assert!(store.get("missing").is_none());
    put_record(&store, &record("t2", "q"));
    assert_eq!(store.list(Some("p")).len(), 1);
    assert_eq!(store.list(Some("nobody")).len(), 0);
    assert_eq!(store.list(None).len(), 2);
}

#[tokio::test]
async fn migration_listing_fails_on_corrupt_records_while_legacy_listing_skips_them() {
    use serde_json::json;
    use tinystoragedrivers_core::Precondition;

    let storage = MemoryStorage::new();
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap())
        .unwrap();
    put_record(&store, &record("corrupt", "p"));
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
    let store =
        DriverCompletionStore::new(Arc::new(storage), Scope::new("local").unwrap()).unwrap();
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
            put_record(store, &r);
        }
    };
    now.store(ms(old), std::sync::atomic::Ordering::SeqCst);
    seed(&driver, old);
    seed(&memory, old);
    now.store(ms(real_now), std::sync::atomic::Ordering::SeqCst);
    let mut recent = record("recent", "p");
    recent.state = CompletionState::Delivered;
    put_record(&driver, &recent);
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
    let docs = Arc::clone(
        storage
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap())
        .unwrap();
    put_record(&store, &record("good", "p"));
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
    let alice =
        DriverCompletionStore::new(Arc::clone(&backend), Scope::new("alice").unwrap()).unwrap();
    let bob = DriverCompletionStore::new(backend, Scope::new("bob").unwrap()).unwrap();
    put_record(&alice, &record("t", "p"));
    assert!(bob.get("t").is_none());
    assert!(bob.list(None).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_sync_trait_works_from_a_multi_thread_runtime() {
    let store =
        DriverCompletionStore::new(Arc::new(MemoryStorage::new()), Scope::new("local").unwrap())
            .unwrap();
    put_record(&store, &record("mt", "p"));
    assert!(store.get("mt").is_some());
}

#[tokio::test]
async fn completions_are_v2_documents_with_key_ids_and_metadata_update_times() {
    let storage = MemoryStorage::new();
    let docs = Arc::clone(
        storage
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let store = DriverCompletionStore::new(Arc::new(storage.clone()), Scope::new("local").unwrap())
        .unwrap();
    put_record(&store, &record("c/1", "p"));
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
