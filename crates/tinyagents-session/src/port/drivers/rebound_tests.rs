use super::*;
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn retained_and_cloned_handles_follow_rebind_without_crossing_agent_scopes() {
    let registry = SessionStoreRegistry::default();
    let old_backend = Arc::new(MemoryStorage::new());
    let provider =
        RebindableSessionStores::new_in(&registry, old_backend.clone(), false).unwrap();
    let first = provider.for_agent("agent-one");
    let first_clone = first.clone();
    let second = provider.for_agent("agent-two");
    first
        .kv
        .put("prefs", "key", serde_json::json!("old-one"))
        .await
        .unwrap();
    second
        .kv
        .put("prefs", "key", serde_json::json!("old-two"))
        .await
        .unwrap();

    let new_backend = Arc::new(MemoryStorage::new());
    assert_eq!(registry.rebind(new_backend.clone()).unwrap(), 1);
    assert_eq!(first_clone.kv.get("prefs", "key").await.unwrap(), None);
    first_clone
        .kv
        .put("prefs", "key", serde_json::json!("new-one"))
        .await
        .unwrap();
    second
        .kv
        .put("prefs", "key", serde_json::json!("new-two"))
        .await
        .unwrap();

    assert_eq!(
        first.kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("new-one"))
    );
    assert_eq!(
        second.kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("new-two"))
    );
    let fresh_driver = DriverSessionStores::new(new_backend).unwrap();
    assert_eq!(
        fresh_driver.for_agent("agent-one").kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("new-one"))
    );
    let old_driver = DriverSessionStores::new(old_backend).unwrap();
    assert_eq!(
        old_driver.for_agent("agent-one").kv.get("prefs", "key").await.unwrap(),
        Some(serde_json::json!("old-one"))
    );
}

#[tokio::test]
async fn retained_journal_handle_routes_later_appends_to_new_backend() {
    let registry = SessionStoreRegistry::default();
    let provider =
        RebindableSessionStores::new_in(&registry, Arc::new(MemoryStorage::new()), false).unwrap();
    let stores = provider.for_agent("agent");
    stores
        .journal
        .append("turns", serde_json::json!({"where":"old"}))
        .await
        .unwrap();
    provider.rebind(Arc::new(MemoryStorage::new())).unwrap();
    assert_eq!(stores.journal.len("turns").await.unwrap(), 0);
    stores
        .journal
        .append("turns", serde_json::json!({"where":"new"}))
        .await
        .unwrap();
    assert_eq!(stores.journal.len("turns").await.unwrap(), 1);
}

#[test]
fn concurrent_registration_and_rebind_leave_all_retained_handles_on_current_backend() {
    let registry = SessionStoreRegistry::default();
    let provider =
        RebindableSessionStores::new_in(&registry, Arc::new(MemoryStorage::new()), false).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let registration_provider = Arc::clone(&provider);
    let registration_barrier = Arc::clone(&barrier);
    let registration = std::thread::spawn(move || {
        registration_barrier.wait();
        registration_provider.for_agent("racing-agent")
    });
    let rebind_provider = Arc::clone(&provider);
    let rebind_barrier = Arc::clone(&barrier);
    let new_backend = Arc::new(MemoryStorage::new());
    let expected_backend = Arc::clone(&new_backend);
    let rebind = std::thread::spawn(move || {
        rebind_barrier.wait();
        rebind_provider.rebind(new_backend).unwrap();
    });
    barrier.wait();
    let registered = registration.join().unwrap();
    rebind.join().unwrap();

    let fresh = provider.for_agent("racing-agent");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        registered
            .kv
            .put("n", "k", serde_json::json!("retained"))
            .await
            .unwrap();
        let independent = DriverSessionStores::new(expected_backend)
            .unwrap()
            .for_agent("racing-agent");
        assert_eq!(
            independent.kv.get("n", "k").await.unwrap(),
            Some(serde_json::json!("retained"))
        );
        assert_eq!(
            fresh.kv.get("n", "k").await.unwrap(),
            Some(serde_json::json!("retained"))
        );
    });
}

#[test]
fn concurrent_rebinds_publish_one_consistent_backend_for_existing_and_new_handles() {
    let registry = SessionStoreRegistry::default();
    let provider =
        RebindableSessionStores::new_in(&registry, Arc::new(MemoryStorage::new()), false).unwrap();
    let retained = provider.for_agent("agent");
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let backends = [Arc::new(MemoryStorage::new()), Arc::new(MemoryStorage::new())];
    let mut workers = Vec::new();
    for backend in backends {
        let provider = Arc::clone(&provider);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            provider.rebind(backend).unwrap();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let fresh = provider.for_agent("agent");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        retained
            .kv
            .put("n", "k", serde_json::json!("same-backend"))
            .await
            .unwrap();
        assert_eq!(
            fresh.kv.get("n", "k").await.unwrap(),
            Some(serde_json::json!("same-backend"))
        );
    });
}
