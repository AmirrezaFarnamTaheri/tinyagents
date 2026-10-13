use super::*;
use std::time::Duration;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn docs(storage: &dyn StorageBackend, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

async fn round_trips_expires_and_counts(cache: DriverResponseCache) {
    assert!(cache.get("k").await.unwrap().is_none());
    cache
        .put("k", ModelResponse::assistant("durable"))
        .await
        .unwrap();
    assert_eq!(cache.get("k").await.unwrap().unwrap().text(), "durable");

    cache
        .put_with_ttl(
            "short",
            ModelResponse::assistant("stale"),
            Some(Duration::from_millis(20)),
        )
        .await
        .unwrap();
    assert_eq!(cache.get("short").await.unwrap().unwrap().text(), "stale");
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        cache.get("short").await.unwrap().is_none(),
        "an expired entry reads as a miss"
    );
    assert!(cache.get("short").await.unwrap().is_none(), "and stays gone");
    // A rewrite revives the key.
    cache
        .put("short", ModelResponse::assistant("fresh"))
        .await
        .unwrap();
    assert_eq!(cache.get("short").await.unwrap().unwrap().text(), "fresh");

    let stats = cache.stats();
    assert_eq!(stats.writes, 3);
    // A backend with native TTL hides the expired entry itself, so only a
    // backend without it lets this process observe (and count) the expiry.
    assert!(stats.expirations <= 1);
    assert_eq!(stats.hits, 3);
    assert_eq!(stats.misses, 3);
}

#[tokio::test]
async fn memory_backend_round_trips_expires_and_counts() {
    round_trips_expires_and_counts(DriverResponseCache::new(docs(&MemoryStorage::new(), "local")))
        .await;
}

#[tokio::test]
async fn sqlite_backend_round_trips_expires_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        tinystoragedrivers_sqlite::SqliteStorage::open(dir.path().join("cache.sqlite")).unwrap();
    round_trips_expires_and_counts(DriverResponseCache::new(docs(&storage, "local"))).await;
}

#[tokio::test]
async fn entries_survive_a_dropped_handle_and_a_reopened_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("responses.sqlite");
    {
        let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
        DriverResponseCache::new(docs(&storage, "local"))
            .put("k", ModelResponse::assistant("persisted"))
            .await
            .unwrap();
    }
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
    let reopened = DriverResponseCache::new(docs(&storage, "local"));
    assert_eq!(reopened.get("k").await.unwrap().unwrap().text(), "persisted");
}

#[tokio::test]
async fn namespaces_do_not_cross_serve_and_clear_independently() {
    let base = DriverResponseCache::new(docs(&MemoryStorage::new(), "local"));
    let tenant_a = base.with_namespace("tenant-a");
    let tenant_b = base.with_namespace("tenant-b");
    assert_eq!(tenant_a.namespace(), "tenant-a");
    tenant_a
        .put("k", ModelResponse::assistant("a"))
        .await
        .unwrap();
    assert!(tenant_b.get("k").await.unwrap().is_none());
    tenant_b
        .put("k", ModelResponse::assistant("b"))
        .await
        .unwrap();
    tenant_a.clear().await.unwrap();
    assert!(tenant_a.get("k").await.unwrap().is_none());
    assert_eq!(tenant_b.get("k").await.unwrap().unwrap().text(), "b");
}

#[tokio::test]
async fn scopes_and_collections_keep_entries_apart_and_long_keys_fit() {
    let storage = MemoryStorage::new();
    let alice = DriverResponseCache::new(docs(&storage, "alice"));
    let bob = DriverResponseCache::new(docs(&storage, "bob"));
    let other = DriverResponseCache::with_collection(docs(&storage, "alice"), "other");
    let long = "k".repeat(10_000);
    alice
        .put(&long, ModelResponse::assistant("mine"))
        .await
        .unwrap();
    assert_eq!(alice.get(&long).await.unwrap().unwrap().text(), "mine");
    assert!(bob.get(&long).await.unwrap().is_none());
    assert!(other.get(&long).await.unwrap().is_none());
}

/// A backend without native TTL declares no expiry field, so the cache itself
/// must hide, count and remove an expired entry.
#[tokio::test]
async fn expiry_is_enforced_on_read_when_the_backend_has_no_native_ttl() {
    let docs: Arc<dyn DocumentStore> = Arc::new(crate::testkit::WithoutNativeTtl(docs(
        &MemoryStorage::new(),
        "local",
    )));
    let cache = DriverResponseCache::new(docs.clone());
    cache
        .put_with_ttl(
            "short",
            ModelResponse::assistant("stale"),
            Some(Duration::from_millis(20)),
        )
        .await
        .unwrap();
    assert_eq!(cache.get("short").await.unwrap().unwrap().text(), "stale");
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(cache.get("short").await.unwrap().is_none());
    assert_eq!(cache.stats().expirations, 1);
    // The expired document was removed, not merely skipped.
    assert_eq!(
        docs.count(DriverResponseCache::DEFAULT_COLLECTION, &Filter::default())
            .await
            .unwrap(),
        0
    );
}
