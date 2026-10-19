use super::*;
use std::sync::Arc;

use crate::completions::{CompletionRouter, InMemoryCompletionStore};

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
    assert_eq!(removed.mark_delivered(&claimed[0].receipt().into_iter().collect::<Vec<_>>()).unwrap(), 1);
    assert_eq!(
        removed.store().get("held-claim").unwrap().state,
        crate::completions::CompletionState::Delivered
    );
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
