use super::*;
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn retained_and_cloned_handles_follow_rebind_without_crossing_agent_scopes() {
    let registry = SessionStoreRegistry::default();
    let provider = RebindableSessionStores::new_in(
        &registry,
        Arc::new(MemoryStorage::new()),
        false,
    )
    .unwrap();
    let first = provider.for_agent("agent-one");
    let first_clone = first.clone();
    let second = provider.for_agent("agent-two");
    first.kv.put("prefs", "key", serde_json::json!("old-one")).await.unwrap();
    second.kv.put("prefs", "key", serde_json::json!("old-two")).await.unwrap();

    assert_eq!(registry.rebind(Arc::new(MemoryStorage::new())).unwrap(), 1);
    assert_eq!(first_clone.kv.get("prefs", "key").await.unwrap(), None);
    first_clone.kv.put("prefs", "key", serde_json::json!("new-one")).await.unwrap();
    second.kv.put("prefs", "key", serde_json::json!("new-two")).await.unwrap();

    assert_eq!(first.kv.get("prefs", "key").await.unwrap(), Some(serde_json::json!("new-one")));
    assert_eq!(second.kv.get("prefs", "key").await.unwrap(), Some(serde_json::json!("new-two")));
}

#[tokio::test]
async fn retained_journal_handle_routes_later_appends_to_new_backend() {
    let registry = SessionStoreRegistry::default();
    let provider = RebindableSessionStores::new_in(
        &registry,
        Arc::new(MemoryStorage::new()),
        false,
    )
    .unwrap();
    let stores = provider.for_agent("agent");
    stores.journal.append("turns", serde_json::json!({"where":"old"})).await.unwrap();
    provider.rebind(Arc::new(MemoryStorage::new())).unwrap();
    assert_eq!(stores.journal.len("turns").await.unwrap(), 0);
    stores.journal.append("turns", serde_json::json!({"where":"new"})).await.unwrap();
    assert_eq!(stores.journal.len("turns").await.unwrap(), 1);
}
