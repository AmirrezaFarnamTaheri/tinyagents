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
