use super::*;
use tinystoragedrivers_core::{MemoryStorage, Scope, ScopedStorage, StorageBackend};

const TINY_PNG: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";

fn scoped(storage: &dyn StorageBackend, scope: &str) -> ScopedStorage {
    storage.for_scope(&Scope::new(scope).unwrap()).unwrap()
}

fn stash_on(storage: &ScopedStorage, max: u64, ttl: Duration) -> BlobAttachmentStash {
    BlobAttachmentStash::new(
        Arc::clone(storage.blobs()),
        Arc::clone(storage.documents()),
        max,
        ttl,
    )
}

fn stash(max: u64, ttl: Duration) -> BlobAttachmentStash {
    stash_on(&scoped(&MemoryStorage::new(), "local"), max, ttl)
}

fn image_uri(fill: u8, len: usize) -> String {
    data_uri::encode_data_uri("image/png", &vec![fill; len])
}

#[tokio::test]
async fn write_is_content_addressed_deduped_and_readable() {
    let s = stash(u64::MAX, Duration::from_secs(3600));
    let first = s.write("abc123", TINY_PNG).await.unwrap();
    assert_eq!(first.key, "attachments/abc123");
    assert_eq!(first.mime, "image/png");
    let second = s.write("abc123", TINY_PNG).await.unwrap();
    assert_eq!(first, second);
    let (bytes, mime) = s.read("abc123").await.unwrap().unwrap();
    assert_eq!(mime, "image/png");
    assert_eq!(bytes.len() as u64, first.len);
    assert_eq!(
        s.read_data_uri("abc123").await.unwrap().as_deref().map(|u| u.starts_with("data:image/png")),
        Some(true)
    );
    let index = s.build_index().await.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index.get("abc123"), Some(&first));
    assert!(s.read("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn unsafe_ids_and_undecodable_uris_are_rejected() {
    let s = stash(u64::MAX, Duration::from_secs(3600));
    for id in ["", "../x", "a/b", "a b", "ünï"] {
        assert!(s.write(id, TINY_PNG).await.is_err(), "id {id:?}");
        assert!(s.read(id).await.unwrap().is_none(), "id {id:?}");
    }
    assert!(s.write("x", "not a data uri").await.is_err());
    assert!(s.build_index().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_attachment_larger_than_the_cap_is_rejected_before_any_write() {
    let s = stash(10, Duration::from_secs(3600));
    s.write("small", &image_uri(1, 5)).await.unwrap();
    assert!(s.write("big", &image_uri(2, 50)).await.is_err());
    assert!(s.read("big").await.unwrap().is_none());
    assert!(s.read("small").await.unwrap().is_some(), "nothing was evicted");
}

#[tokio::test]
async fn cap_evicts_least_recently_used_first() {
    let s = stash(250, Duration::from_secs(3600));
    s.write("old", &image_uri(1, 100)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    s.write("mid", &image_uri(2, 100)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // Reusing `old` refreshes it, so `mid` is now the oldest.
    s.write("old", &image_uri(1, 100)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    s.write("new", &image_uri(3, 100)).await.unwrap();
    assert!(s.read("mid").await.unwrap().is_none(), "least recently used goes");
    assert!(s.read("old").await.unwrap().is_some());
    assert!(s.read("new").await.unwrap().is_some());
}

#[tokio::test]
async fn sweep_removes_only_attachments_older_than_the_ttl() {
    let s = stash(u64::MAX, Duration::from_millis(40));
    s.write("stale", &image_uri(1, 100)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    s.write("fresh", &image_uri(2, 60)).await.unwrap();
    assert_eq!(s.sweep_stale().await.unwrap(), 100);
    assert!(s.read("stale").await.unwrap().is_none());
    assert!(s.read("fresh").await.unwrap().is_some());
    assert_eq!(s.sweep_stale().await.unwrap(), 0);
}

#[tokio::test]
async fn tenants_do_not_see_each_others_attachments() {
    let storage = MemoryStorage::new();
    let alice = stash_on(&scoped(&storage, "alice"), u64::MAX, Duration::from_secs(60));
    let bob = stash_on(&scoped(&storage, "bob"), u64::MAX, Duration::from_secs(60));
    alice.write("shared-id", TINY_PNG).await.unwrap();
    assert!(bob.read("shared-id").await.unwrap().is_none());
    assert!(bob.build_index().await.unwrap().is_empty());
}

#[tokio::test]
async fn attachments_survive_a_reopened_sqlite_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stash.sqlite");
    {
        let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
        stash_on(&scoped(&storage, "local"), u64::MAX, Duration::from_secs(60))
            .write("persisted", TINY_PNG)
            .await
            .unwrap();
    }
    let storage = tinystoragedrivers_sqlite::SqliteStorage::open(&path).unwrap();
    let reopened = stash_on(&scoped(&storage, "local"), u64::MAX, Duration::from_secs(60));
    assert!(reopened.read("persisted").await.unwrap().is_some());
}
