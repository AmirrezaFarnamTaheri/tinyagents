use super::*;
use std::sync::Arc;

use crate::completions::{CompletionRouter, InMemoryCompletionStore};

#[derive(Debug)]
struct UnfencedBackend(tinystoragedrivers_core::MemoryStorage);

#[async_trait::async_trait]
impl tinystoragedrivers_core::StorageBackend for UnfencedBackend {
    fn driver(&self) -> &'static str {
        "unfenced-test"
    }

    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        tinystoragedrivers_core::Capabilities::none()
    }

    fn for_scope(
        &self,
        scope: &tinystoragedrivers_core::Scope,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::ScopedStorage> {
        self.0.for_scope(scope)
    }
}

#[tokio::test]
async fn durable_completion_open_fails_closed_while_explicit_local_fallback_remains_usable() {
    let backend: Arc<dyn tinystoragedrivers_core::StorageBackend> = Arc::new(UnfencedBackend(
        tinystoragedrivers_core::MemoryStorage::new(),
    ));
    let scope = tinystoragedrivers_core::Scope::new("shared").unwrap();
    assert!(
        open_driver_completion_store_or_memory(
            Arc::clone(&backend),
            scope.clone(),
            CompletionStoreFallback::FailClosed,
        )
        .is_err()
    );

    let local = open_driver_completion_store_or_memory(
        backend,
        scope,
        CompletionStoreFallback::LocalMemory,
    )
    .unwrap();
    let router = CompletionRouter::new(local);
    let outcome = router
        .record(crate::completions::CompletionRecord::new(
            "local-task",
            "local-parent",
            "worker",
            crate::completions::CompletionStatus::Success,
            crate::completions::CompletionResult::default(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        crate::completions::RecordOutcome::Recorded { .. }
    ));
}

#[test]
fn router_pool_opens_once_per_key_and_reuses_the_router() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let pool = CompletionRouterPool::new(move |_| {
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let first = pool.get_or_open(&"workspace").unwrap();
    let second = pool.get_or_open(&"workspace").unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn router_pool_can_drop_cached_routers_for_restart_recovery() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let first = pool.get_or_open(&"workspace").unwrap();
    pool.clear().unwrap();
    let second = pool.get_or_open(&"workspace").unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
}

#[test]
fn router_pool_replaces_only_the_expected_cached_instance() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let failed = pool.get_or_open(&"workspace").unwrap();
    let fallback = Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )));
    let winner = match pool
        .replace_if_current(&"workspace", &failed, Arc::clone(&fallback))
        .unwrap()
    {
        CompletionRouterReplaceOutcome::Replaced(router) => router,
        CompletionRouterReplaceOutcome::Current(_) | CompletionRouterReplaceOutcome::Missing => {
            panic!("expected replacement to win")
        }
    };
    assert!(Arc::ptr_eq(&winner, &fallback));
    match pool
        .replace_if_current(&"workspace", &failed, Arc::clone(&failed))
        .unwrap()
    {
        CompletionRouterReplaceOutcome::Current(router) => assert!(Arc::ptr_eq(&router, &fallback)),
        CompletionRouterReplaceOutcome::Replaced(_) | CompletionRouterReplaceOutcome::Missing => {
            panic!("stale router must not replace the installed fallback")
        }
    }
}

#[test]
fn router_pool_does_not_resurrect_a_router_after_clear() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let stale = pool.get_or_open(&"workspace").unwrap();
    pool.clear().unwrap();
    let stale_fallback = Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )));

    assert!(matches!(
        pool.replace_if_current(&"workspace", &stale, stale_fallback)
            .unwrap(),
        CompletionRouterReplaceOutcome::Missing
    ));
    let reopened = pool.get_or_open(&"workspace").unwrap();
    assert!(!Arc::ptr_eq(&reopened, &stale));
}

#[test]
fn stopped_parent_remains_closed_after_pool_clear_and_reopen() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let first = pool.get_or_open(&"workspace").unwrap();
    assert_eq!(first.admit_child_generation("parent").unwrap(), Some(1));
    let mut queued = CompletionRecord::new(
        "queued-child",
        "parent",
        "worker",
        crate::completions::CompletionStatus::Success,
        crate::completions::CompletionResult::default(),
    );
    queued.parent_epoch = 1;
    let old_admission = CompletionAdmission::for_router(&first, &queued);
    let old_store_alias = CompletionAdmission::for_store(first.store(), &queued);
    assert_eq!(
        tinyagents_harness::run_queue::QueueAdmission::batch_key(&old_admission),
        tinyagents_harness::run_queue::QueueAdmission::batch_key(&old_store_alias),
        "for_store aliases must use the pool-bound lifecycle coordination key"
    );
    let permit = tinyagents_harness::run_queue::QueueAdmission::acquire(&old_admission)
        .expect("the current lifecycle admits its queued child");
    drop(permit);
    first.cancel_parent("parent").unwrap();
    assert!(tinyagents_harness::run_queue::QueueAdmission::acquire(&old_admission).is_err());

    pool.clear().unwrap();
    let reopened = pool.get_or_open(&"workspace").unwrap();
    let reopened_store_alias = CompletionAdmission::for_store(reopened.store(), &queued);
    assert_eq!(
        tinyagents_harness::run_queue::QueueAdmission::batch_key(&old_admission),
        tinyagents_harness::run_queue::QueueAdmission::batch_key(&reopened_store_alias),
        "queued handles across clear must preserve the same scoped lifecycle key"
    );

    assert!(
        !reopened.admit_child("parent").unwrap(),
        "clearing the router cache must not implicitly reopen a stopped parent"
    );

    reopened.resume_parent("parent");
    assert_eq!(
        reopened.admit_child_generation("parent").unwrap(),
        Some(3),
        "only explicit resume opens a new generation"
    );
    assert!(tinyagents_harness::run_queue::QueueAdmission::acquire(&old_admission).is_err());
    assert_eq!(
        first.admit_child_generation("parent").unwrap(),
        Some(3),
        "an old router must reconcile its stale marker with the retained newer generation"
    );
    assert!(tinyagents_harness::run_queue::QueueAdmission::acquire(&old_admission).is_err());
    let mut current = queued;
    current.parent_epoch = 3;
    let current_admission = CompletionAdmission::for_router(&reopened, &current);
    assert!(tinyagents_harness::run_queue::QueueAdmission::acquire(&current_admission).is_ok());
}

#[test]
fn deleted_parent_stays_deleted_across_pool_clear() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let first = pool.get_or_open(&"workspace").unwrap();
    assert_eq!(first.admit_child_generation("parent").unwrap(), Some(1));
    first.delete_parent("parent").unwrap();

    pool.clear().unwrap();
    let reopened = pool.get_or_open(&"workspace").unwrap();
    reopened.resume_parent("parent");

    assert_eq!(first.admit_child_generation("parent").unwrap(), None);
    let mut old_record = CompletionRecord::new(
        "queued-child",
        "parent",
        "worker",
        crate::completions::CompletionStatus::Success,
        crate::completions::CompletionResult::default(),
    );
    old_record.parent_epoch = 1;
    let old_admission = CompletionAdmission::for_router(&first, &old_record);
    assert!(tinyagents_harness::run_queue::QueueAdmission::acquire(&old_admission).is_err());

    assert!(
        !reopened.admit_child("parent").unwrap(),
        "a deleted parent cannot be reopened after its router is evicted"
    );
}

#[test]
fn parent_lifecycle_isolated_between_pool_scope_keys() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let first_scope = pool.get_or_open(&"workspace-a").unwrap();
    let second_scope = pool.get_or_open(&"workspace-b").unwrap();
    assert_eq!(
        first_scope.admit_child_generation("same-parent").unwrap(),
        Some(1)
    );
    assert_eq!(
        second_scope.admit_child_generation("same-parent").unwrap(),
        Some(1)
    );

    first_scope.cancel_parent("same-parent").unwrap();

    assert!(!first_scope.admit_child("same-parent").unwrap());
    assert!(second_scope.admit_child("same-parent").unwrap());
}

#[tokio::test]
async fn evicting_one_key_preserves_other_cached_routers_and_held_claims() {
    let pool = CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    });
    let removed = pool.get_or_open(&"workspace-a").unwrap();
    let unrelated = pool.get_or_open(&"workspace-b").unwrap();
    removed
        .record(
            CompletionRecord::new(
                "held-claim",
                "parent",
                "worker",
                CompletionStatus::Success,
                CompletionResult::default(),
            )
            .with_notify_mode(NotifyMode::Off),
        )
        .await
        .unwrap();
    let claimed = removed.pull("parent", 1).unwrap();
    assert_eq!(claimed.len(), 1);

    let evicted = pool.remove(&"workspace-a").unwrap().unwrap();
    assert!(Arc::ptr_eq(&evicted, &removed));
    assert!(pool.get(&"workspace-a").unwrap().is_none());
    let still_cached = pool.get(&"workspace-b").unwrap().unwrap();
    assert!(Arc::ptr_eq(&still_cached, &unrelated));
    assert_eq!(
        removed
            .mark_delivered(&claimed[0].receipt().into_iter().collect::<Vec<_>>())
            .unwrap(),
        1
    );
    assert_eq!(
        removed.store().get("held-claim").unwrap().state,
        crate::completions::CompletionState::Delivered
    );
}

#[tokio::test]
async fn pool_release_all_releases_claims_from_every_cached_router() {
    let store = Arc::new(InMemoryCompletionStore::new());
    let shared = Arc::clone(&store);
    let pool = CompletionRouterPool::new(move |_| Arc::new(CompletionRouter::new(shared.clone())));
    let first = pool.get_or_open(&"workspace-a").unwrap();
    let second = pool.get_or_open(&"workspace-b").unwrap();
    let ambiguous_alias = CompletionAdmission::for_store(
        store.clone(),
        &CompletionRecord::new(
            "ambiguous",
            "parent-a",
            "worker",
            crate::completions::CompletionStatus::Success,
            crate::completions::CompletionResult::default(),
        ),
    );
    assert!(tinyagents_harness::run_queue::QueueAdmission::batch_key(&ambiguous_alias).is_none());
    assert!(matches!(
        tinyagents_harness::run_queue::QueueAdmission::acquire(&ambiguous_alias),
        Err(tinyagents_harness::run_queue::QueueAdmissionError::Retry)
    ));
    first
        .record(CompletionRecord::new(
            "first",
            "parent-a",
            "worker",
            crate::completions::CompletionStatus::Success,
            crate::completions::CompletionResult::text("one"),
        ))
        .await
        .unwrap();
    second
        .record(CompletionRecord::new(
            "second",
            "parent-b",
            "worker",
            crate::completions::CompletionStatus::Success,
            crate::completions::CompletionResult::text("two"),
        ))
        .await
        .unwrap();
    assert_eq!(first.claim_pending("parent-a", 1).unwrap().len(), 1);
    assert_eq!(second.claim_pending("parent-b", 1).unwrap().len(), 1);

    assert_eq!(pool.release_all().unwrap(), 2);

    let restarted = CompletionRouter::new(store);
    assert_eq!(restarted.claim_pending("parent-a", 1).unwrap().len(), 1);
    assert_eq!(restarted.claim_pending("parent-b", 1).unwrap().len(), 1);
}

#[test]
fn concurrent_fallback_replacements_have_one_winner_and_keep_other_keys_isolated() {
    let pool = Arc::new(CompletionRouterPool::new(|_| {
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        )))
    }));
    let expected = pool.get_or_open(&"workspace-a").unwrap();
    let other_key = pool.get_or_open(&"workspace-b").unwrap();
    let candidates = [
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        ))),
        Arc::new(CompletionRouter::new(Arc::new(
            InMemoryCompletionStore::new(),
        ))),
    ];
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = candidates
        .iter()
        .map(|candidate| {
            let pool = Arc::clone(&pool);
            let expected = Arc::clone(&expected);
            let candidate = Arc::clone(candidate);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                pool.replace_if_current(&"workspace-a", &expected, candidate)
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();

    let mut winner = None;
    let mut replaced = 0;
    let mut current = 0;
    for outcome in outcomes {
        let router = match outcome {
            CompletionRouterReplaceOutcome::Replaced(router) => {
                replaced += 1;
                router
            }
            CompletionRouterReplaceOutcome::Current(router) => {
                current += 1;
                router
            }
            CompletionRouterReplaceOutcome::Missing => panic!("cached key unexpectedly missing"),
        };
        if let Some(winner) = &winner {
            assert!(Arc::ptr_eq(winner, &router));
        } else {
            winner = Some(router);
        }
    }
    assert_eq!((replaced, current), (1, 1));
    assert!(Arc::ptr_eq(
        &winner.unwrap(),
        &pool.get(&"workspace-a").unwrap().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &other_key,
        &pool.get(&"workspace-b").unwrap().unwrap()
    ));
}
