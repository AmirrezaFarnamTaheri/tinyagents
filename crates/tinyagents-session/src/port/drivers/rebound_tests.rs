use super::*;
use serde_json::Value;
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

struct BlockingStore {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl Store for BlockingStore {
    async fn get(&self, _: &str, _: &str) -> HarnessResult<Option<Value>> {
        if let Some(entered) = self.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        if let Some(release) = self.release.lock().unwrap().take() {
            let _ = release.await;
        }
        Ok(Some(serde_json::json!("old-operation")))
    }
    async fn put(&self, _: &str, _: &str, _: Value) -> HarnessResult<()> { Ok(()) }
    async fn delete(&self, _: &str, _: &str) -> HarnessResult<()> { Ok(()) }
    async fn list(&self, _: &str) -> HarnessResult<Vec<String>> { Ok(Vec::new()) }
}

#[tokio::test]
async fn rebind_during_a_blocked_store_call_keeps_that_call_old_and_routes_the_next_new() {
    let base_provider = DriverSessionStores::new(Arc::new(MemoryStorage::new())).unwrap();
    let mut old = base_provider.for_agent("agent");
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    old.kv = Arc::new(BlockingStore {
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(Some(release_rx)),
    });
    let binding = Arc::new(LiveAgentStores { current: RwLock::new(old) });
    let operation = {
        let store = LiveStore(Arc::clone(&binding));
        tokio::spawn(async move { store.get("n", "k").await.unwrap() })
    };
    entered_rx.await.unwrap();

    let mut next = base_provider.for_agent("agent");
    next.kv = Arc::new(tinyagents_harness::store::InMemoryStore::new());
    next.kv.put("n", "k", serde_json::json!("new-operation")).await.unwrap();
    *binding.current.write().unwrap() = next;
    release_tx.send(()).unwrap();

    assert_eq!(operation.await.unwrap(), Some(serde_json::json!("old-operation")));
    assert_eq!(LiveStore(binding).get("n", "k").await.unwrap(), Some(serde_json::json!("new-operation")));
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
    let candidate_backends = backends.clone();
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
        let mut winner = None;
        for (index, backend) in candidate_backends.into_iter().enumerate() {
            let direct = DriverSessionStores::new(backend).unwrap();
            if direct
                .for_agent("agent")
                .kv
                .get("n", "k")
                .await
                .unwrap()
                .is_some()
            {
                winner = Some(index);
            }
        }
        let winner = winner.expect("one concurrent rebind backend has the retained write");
        fresh
            .kv
            .put("n", "fresh", serde_json::json!("current"))
            .await
            .unwrap();
        let direct = DriverSessionStores::new(backends[winner].clone()).unwrap();
        assert_eq!(
            direct
                .for_agent("agent")
                .kv
                .get("n", "k")
                .await
                .unwrap(),
            Some(serde_json::json!("same-backend"))
        );
        assert_eq!(
            direct
                .for_agent("agent")
                .kv
                .get("n", "fresh")
                .await
                .unwrap(),
            Some(serde_json::json!("current"))
        );
    });
}
