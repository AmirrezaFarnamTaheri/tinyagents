use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::completions::{
    CompletionRecord, CompletionResult, CompletionStatus, DeliveryTarget, InMemoryCompletionStore,
};

fn target(router: &CompletionRouter) -> DeliveryTarget<'_> {
    DeliveryTarget {
        router,
        parent_key: "parent",
        thread_key: "thread",
    }
}

#[tokio::test]
async fn delivery_claims_a_batch_and_acknowledges_it() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    router
        .record(CompletionRecord::new(
            "a",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::text("A"),
        ))
        .await
        .unwrap();
    router
        .record(CompletionRecord::new(
            "b",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::text("B"),
        ))
        .await
        .unwrap();
    let delivery = CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 8,
        },
        ThreadDeliveryLeases::default(),
    );
    let mut delivered = Vec::new();
    let outcome = delivery
        .deliver_once(
            target(&router),
            || false,
            || true,
            |batch| {
                delivered = batch.iter().map(|record| record.task_id.clone()).collect();
                async { Ok::<(), &'static str>(()) }
            },
            |_| {},
        )
        .await;
    assert_eq!(outcome, DeliveryOutcome::Delivered { count: 2 });
    assert_eq!(delivered.len(), 2);
    assert!(router.pending_for("parent").is_empty());
}

#[tokio::test]
async fn failed_delivery_releases_for_retry_and_reports_give_up() {
    let router =
        CompletionRouter::new(Arc::new(InMemoryCompletionStore::new())).with_max_attempts(1);
    router
        .record(CompletionRecord::new(
            "a",
            "parent",
            "worker",
            CompletionStatus::Failed,
            CompletionResult::default(),
        ))
        .await
        .unwrap();
    let delivery = CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 8,
        },
        ThreadDeliveryLeases::default(),
    );
    let mut gave_up = Vec::new();
    let outcome = delivery
        .deliver_once(
            target(&router),
            || false,
            || true,
            |_| async { Err::<(), _>("turn failed") },
            |records| gave_up = records.to_vec(),
        )
        .await;
    assert_eq!(
        outcome,
        DeliveryOutcome::Failed {
            count: 1,
            gave_up: 1
        }
    );
    assert_eq!(gave_up[0].task_id, "a");
}

#[test]
fn busy_gate_and_delivery_lease_are_raii_and_exclusive() {
    let busy = ParentBusyGate::default();
    let first = busy.enter("parent").unwrap();
    let second = busy.enter("parent").unwrap();
    assert!(busy.is_busy("parent"));
    drop(first);
    assert!(busy.is_busy("parent"));
    drop(second);
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
    router
        .record(CompletionRecord::new(
            "a",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::default(),
        ))
        .await
        .unwrap();
    let delivery = CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 8,
        },
        ThreadDeliveryLeases::default(),
    );
    let outcome = delivery
        .recover_on_boot(
            target(&router),
            || false,
            || false,
            |_| async { Ok::<(), &'static str>(()) },
            |_| {},
        )
        .await;
    assert_eq!(outcome, DeliveryOutcome::Fenced);
    assert_eq!(router.pending_for("parent").len(), 1);
}

#[tokio::test]
async fn shared_thread_lease_prevents_overlapping_delivery_turns() {
    let router = Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )));
    router
        .record(CompletionRecord::new(
            "a",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::default(),
        ))
        .await
        .unwrap();
    let leases = ThreadDeliveryLeases::default();
    let delivery = Arc::new(CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 8,
        },
        leases,
    ));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let task_router = Arc::clone(&router);
    let task_delivery = Arc::clone(&delivery);
    let mut started_tx = Some(started_tx);
    let mut release_rx = Some(release_rx);
    let first = tokio::spawn(async move {
        task_delivery
            .deliver_once(
                target(&task_router),
                || false,
                || true,
                |_| {
                    let started = started_tx.take().unwrap();
                    let release = release_rx.take().unwrap();
                    async move {
                        let _ = started.send(());
                        release.await.unwrap();
                        Ok::<(), &'static str>(())
                    }
                },
                |_| {},
            )
            .await
    });
    started_rx.await.unwrap();
    let second = delivery
        .deliver_once(
            target(&router),
            || false,
            || true,
            |_| async { Err::<(), &'static str>("overlapping delivery callback must not run") },
            |_| {},
        )
        .await;
    assert_eq!(second, DeliveryOutcome::Busy);
    release_tx.send(()).unwrap();
    assert_eq!(
        first.await.unwrap(),
        DeliveryOutcome::Delivered { count: 1 }
    );
}

#[tokio::test]
async fn aborted_delivery_releases_its_claimed_batch() {
    let router = Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )));
    router
        .record(CompletionRecord::new(
            "cancelled",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::default(),
        ))
        .await
        .unwrap();
    let delivery = Arc::new(CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 1,
        },
        ThreadDeliveryLeases::default(),
    ));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task_router = Arc::clone(&router);
    let task_delivery = Arc::clone(&delivery);
    let task = tokio::spawn(async move {
        let mut started_tx = Some(started_tx);
        task_delivery
            .deliver_once(
                target(&task_router),
                || false,
                || true,
                |_| {
                    let started = started_tx.take().unwrap();
                    async move {
                        let _ = started.send(());
                        std::future::pending::<Result<(), &'static str>>().await
                    }
                },
                |_| {},
            )
            .await
    });
    started_rx.await.unwrap();
    assert_eq!(router.in_flight_for("parent").len(), 1);
    task.abort();
    let _ = task.await;
    assert_eq!(router.claim_pending("parent", 1).unwrap().len(), 1);
}

#[tokio::test]
async fn boot_recovery_drains_more_than_one_batch() {
    let router = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    for id in ["a", "b", "c"] {
        router
            .record(CompletionRecord::new(
                id,
                "parent",
                "worker",
                CompletionStatus::Success,
                CompletionResult::default(),
            ))
            .await
            .unwrap();
    }
    let delivery = CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::ZERO,
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 1,
        },
        ThreadDeliveryLeases::default(),
    );
    let mut batches = 0;
    let outcome = delivery
        .recover_on_boot(
            target(&router),
            || false,
            || true,
            |_| {
                batches += 1;
                async { Ok::<(), &'static str>(()) }
            },
            |_| {},
        )
        .await;
    assert_eq!(outcome, DeliveryOutcome::Delivered { count: 3 });
    assert_eq!(batches, 3);
    assert!(router.pending_for("parent").is_empty());
}

#[derive(Clone)]
struct NotifiedFence {
    notify: Arc<Notify>,
    admitted: Arc<std::sync::atomic::AtomicBool>,
}
impl DeliveryFence for NotifiedFence {
    fn admitted(&self) -> bool {
        self.admitted.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn lost(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.notify.notified())
    }
}

#[tokio::test]
async fn fence_notification_interrupts_a_long_delivery_delay() {
    let fence = NotifiedFence {
        notify: Arc::new(Notify::new()),
        admitted: Arc::new(std::sync::atomic::AtomicBool::new(true)),
    };
    let signal = fence.clone();
    let router = Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )));
    router
        .record(CompletionRecord::new(
            "fenced",
            "parent",
            "worker",
            CompletionStatus::Success,
            CompletionResult::default(),
        ))
        .await
        .unwrap();
    let delivery = Arc::new(CompletionDelivery::new(
        CompletionDeliveryConfig {
            debounce: Duration::from_secs(60),
            recovery_delay: Duration::ZERO,
            retry_backoff: Duration::ZERO,
            busy_poll: Duration::ZERO,
            max_busy_polls: 0,
            batch_size: 1,
        },
        ThreadDeliveryLeases::default(),
    ));
    let task = tokio::spawn(async move {
        delivery
            .deliver_once(
                target(&router),
                || false,
                fence,
                |_| async { Ok::<(), &'static str>(()) },
                |_| {},
            )
            .await
    });
    tokio::task::yield_now().await;
    signal
        .admitted
        .store(false, std::sync::atomic::Ordering::SeqCst);
    signal.notify.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        DeliveryOutcome::Fenced
    );
}
