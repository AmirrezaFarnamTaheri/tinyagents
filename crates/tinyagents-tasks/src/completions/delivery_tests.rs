use super::*;
use std::sync::Arc;
use std::time::Duration;

use crate::completions::{CompletionRecord, CompletionResult, CompletionStatus, InMemoryCompletionStore};

#[tokio::test]
async fn delivery_claims_a_batch_and_acknowledges_it() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    router.record(CompletionRecord::new("a", "parent", "worker", CompletionStatus::Success, CompletionResult::text("A"))).await.unwrap();
    router.record(CompletionRecord::new("b", "parent", "worker", CompletionStatus::Success, CompletionResult::text("B"))).await.unwrap();
    let delivery = CompletionDelivery::new(CompletionDeliveryConfig {
        debounce: Duration::ZERO,
        retry_backoff: Duration::ZERO,
        busy_poll: Duration::ZERO,
        max_busy_polls: 0,
        batch_size: 8,
    });
    let mut delivered = Vec::new();
    let outcome = delivery.deliver_once(&router, "parent", || false, || true, |batch| {
        delivered = batch.iter().map(|record| record.task_id.clone()).collect();
        async { Ok::<(), &'static str>(()) }
    }, |_| {}).await;
    assert_eq!(outcome, DeliveryOutcome::Delivered { count: 2 });
    assert_eq!(delivered.len(), 2);
    assert!(router.pending_for("parent").is_empty());
}

#[tokio::test]
async fn failed_delivery_releases_for_retry_and_reports_give_up() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new())).with_max_attempts(1);
    router.record(CompletionRecord::new("a", "parent", "worker", CompletionStatus::Failed, CompletionResult::default())).await.unwrap();
    let delivery = CompletionDelivery::new(CompletionDeliveryConfig {
        debounce: Duration::ZERO,
        retry_backoff: Duration::ZERO,
        busy_poll: Duration::ZERO,
        max_busy_polls: 0,
        batch_size: 8,
    });
    let mut gave_up = Vec::new();
    let outcome = delivery.deliver_once(&router, "parent", || false, || true, |_| async { Err::<(), _>("turn failed") }, |records| gave_up = records.to_vec()).await;
    assert_eq!(outcome, DeliveryOutcome::Failed { count: 1, gave_up: 1 });
    assert_eq!(gave_up[0].task_id, "a");
}

#[test]
fn busy_gate_and_delivery_lease_are_raii_and_exclusive() {
    let busy = ParentBusyGate::default();
    let first = busy.enter("parent").unwrap();
    assert!(busy.is_busy("parent"));
    assert!(busy.enter("parent").is_none());
    drop(first);
    assert!(!busy.is_busy("parent"));

    let leases = ThreadDeliveryLeases::default();
    let lease = leases.try_acquire("thread").unwrap();
    assert!(leases.try_acquire("thread").is_none());
    drop(lease);
    assert!(leases.try_acquire("thread").is_some());
}

#[tokio::test]
async fn recovery_is_fenced_before_claiming_records() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    router.record(CompletionRecord::new("a", "parent", "worker", CompletionStatus::Success, CompletionResult::default())).await.unwrap();
    let delivery = CompletionDelivery::new(CompletionDeliveryConfig { debounce: Duration::ZERO, retry_backoff: Duration::ZERO, busy_poll: Duration::ZERO, max_busy_polls: 0, batch_size: 8 });
    let outcome = delivery.recover_on_boot(&router, "parent", || false, || false, |_| async { Ok::<(), &'static str>(()) }, |_| {}).await;
    assert_eq!(outcome, DeliveryOutcome::Fenced);
    assert_eq!(router.pending_for("parent").len(), 1);
}
