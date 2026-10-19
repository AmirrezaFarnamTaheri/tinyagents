use super::*;
use std::sync::Arc;
use std::time::Duration;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use tinyagents_harness::CancellationToken;

#[tokio::test]
async fn releasing_a_turn_lease_cancels_its_admission_and_successor_gets_new_epoch() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let lease_scope = Scope::new("cluster").unwrap();
    let tenant = Scope::new("u:alice").unwrap();
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
    assert_eq!(successor.fence().collection(), "tsd.leases");
    successor.release().await.unwrap();
}

#[tokio::test]
async fn a_takeover_lost_during_renewal_permanently_cancels_the_turn() {
    use serde_json::json;
    use tinystoragedrivers_core::{Key, Precondition};

    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let lease_scope = Scope::new("cluster").unwrap();
    let tenant = Scope::new("u:alice").unwrap();
    let factory = DurableThreadDeliveryLeases::new(
        backend.as_ref(),
        lease_scope.clone(),
        "test-cluster",
        Duration::from_secs(30),
    )
    .unwrap();
    let first = factory
        .try_acquire(&tenant, "thread-1")
        .await
        .unwrap()
        .unwrap();
    let old_epoch = first.epoch();
    let turn_cancellation = CancellationToken::new();
    let loss_listener = first.cancel_on_loss(turn_cancellation.clone());

    // Simulate another process taking over through the released document CAS
    // contract. The lease renewer observes the new holder/epoch and permanently
    // closes this tenure; no private clock or test-only lease hook is involved.
    let encoded_key = serde_json::to_string(&("test-cluster", tenant.as_str(), "thread-1"))
        .unwrap();
    let lease_id = Key::of(["tinyagents.thread-turn", encoded_key.as_str()])
        .unwrap()
        .as_str()
        .to_owned();
    let documents = backend.for_scope(&lease_scope).unwrap().documents().clone();
    let current = documents
        .get("tsd.leases", &lease_id)
        .await
        .unwrap()
        .unwrap();
    let successor_expiry = current.doc["expires_ms"].as_i64().unwrap() + 60_000;
    documents
        .put(
            "tsd.leases",
            &lease_id,
            json!({
                "holder": "successor-process",
                "epoch": old_epoch + 1,
                "expires_ms": successor_expiry,
            }),
            Precondition::Version(current.version),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), turn_cancellation.cancelled())
        .await
        .expect("renewal detects the takeover and cancels the active turn");
    loss_listener.await.unwrap();
    assert!(!first.admitted(), "lost tenure cannot regain admission");
    assert!(
        factory
            .try_acquire(&tenant, "thread-1")
            .await
            .unwrap()
            .is_none(),
        "the old turn must not reacquire while a successor owns the thread"
    );
}
