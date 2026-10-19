use super::*;
use std::time::Duration;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use tinyagents_harness::CancellationToken;

#[tokio::test]
async fn releasing_a_turn_lease_cancels_its_admission_and_successor_gets_new_epoch() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let lease_scope = Scope::new("cluster").unwrap();
    let tenant = Scope::new("user:alice").unwrap();
    let first_factory = DurableThreadDeliveryLeases::new(
        backend.as_ref(),
        lease_scope.clone(),
        "test-cluster",
        Duration::from_secs(30),
    )
    .unwrap();
    let second_factory = DurableThreadDeliveryLeases::new(
        backend.as_ref(),
        lease_scope,
        "test-cluster",
        Duration::from_secs(30),
    )
    .unwrap();

    let first = first_factory
        .try_acquire(&tenant, "thread-1")
        .await
        .unwrap()
        .unwrap();
    let first_epoch = first.epoch();
    let turn_cancellation = CancellationToken::new();
    let loss_listener = first.cancel_on_loss(turn_cancellation.clone());

    assert!(
        second_factory
            .try_acquire(&tenant, "thread-1")
            .await
            .unwrap()
            .is_none(),
        "a live thread turn has one durable holder"
    );
    assert!(first.release().await.unwrap());
    tokio::time::timeout(Duration::from_secs(1), turn_cancellation.cancelled())
        .await
        .expect("loss of the held tenure cancels the executing turn");
    loss_listener.await.unwrap();

    let successor = second_factory
        .try_acquire(&tenant, "thread-1")
        .await
        .unwrap()
        .unwrap();
    assert!(successor.epoch() > first_epoch);
    assert_eq!(successor.fence().collection(), "leases");
    successor.release().await.unwrap();
}
