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
