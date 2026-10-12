use super::*;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, Fence, MemoryStorage, Scope, StorageBackend};

fn request(id: &str) -> CreateConversationThread {
    CreateConversationThread {
        id: id.into(),
        title: "Initial".into(),
        created_at: "2026-01-01T00:00:00Z".into(),
        parent_thread_id: None,
        labels: None,
        personality_id: None,
        working_dir: None,
    }
}

async fn missing_declaration(backend: &dyn StorageBackend) {
    backend.prepare_schema().await.unwrap();
    let scope = Scope::new("alice").unwrap();
    let scoped = backend.for_scope(&scope).unwrap();
    let docs = scoped.documents().clone();
    let store = DriverConversationStore::new_validate_only(scoped);
    let error = store
        .get_thread("missing")
        .await
        .expect_err("explicit admission must require a persisted declaration before read");
    assert!(matches!(error, ConversationError::Driver(e) if e.kind() == ErrorKind::NotFound));
    assert_eq!(docs.count(COLLECTION, &Filter::All).await.unwrap(), 0);
}

async fn prepared_fenced(backend: &dyn StorageBackend) {
    backend.prepare_schema().await.unwrap();
    let cluster = Scope::new("cluster").unwrap();
    let profile = Scope::new("alice").unwrap();
    let raw = backend.for_scope(&profile).unwrap();
    raw.documents()
        .ensure_collection(&CollectionSpec::new(COLLECTION))
        .await
        .unwrap();
    let owner = backend.for_scope(&cluster).unwrap();
    owner
        .documents()
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    owner
        .documents()
        .put("leases", "alice", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster, "leases", "alice", "epoch", 1);
    let actor = backend.existing_schema().unwrap();
    let store = DriverConversationStore::new_validate_only(
        actor.for_scope_fenced(&profile, &fence).unwrap(),
    );
    let thread = store.ensure_thread(request("one")).await.unwrap();
    assert_eq!(store.get_thread("one").await.unwrap(), Some(thread.clone()));
    assert!(store.get_messages("one").await.unwrap().is_empty());
    assert_eq!(store.list_threads().await.unwrap(), vec![thread.clone()]);
    let sibling = DriverConversationStore::new_validate_only(
        actor.for_scope(&Scope::new("bob").unwrap()).unwrap(),
    );
    assert!(sibling.get_thread("one").await.unwrap().is_none());
    owner
        .documents()
        .put("leases", "alice", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    let error = store
        .update_thread_title("one", "stale", "now")
        .await
        .unwrap_err();
    assert!(matches!(error, ConversationError::Driver(e) if e.kind() == ErrorKind::Fenced));
    assert_eq!(
        DriverConversationStore::new(raw)
            .get_thread("one")
            .await
            .unwrap(),
        Some(thread)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_explicit_admission_refuses_missing_declaration_before_read() {
    missing_declaration(&MemoryStorage::new()).await;
}
#[tokio::test(flavor = "current_thread")]
async fn sqlite_explicit_admission_refuses_missing_declaration_before_read() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        tinystoragedrivers_sqlite::SqliteStorage::open(dir.path().join("conversation.db")).unwrap();
    missing_declaration(&backend).await;
}
#[tokio::test(flavor = "current_thread")]
async fn memory_prepared_actor_retains_original_native_fence_and_scope() {
    prepared_fenced(&MemoryStorage::new()).await;
}
#[tokio::test(flavor = "current_thread")]
async fn sqlite_prepared_actor_retains_original_native_fence_and_scope() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        tinystoragedrivers_sqlite::SqliteStorage::open(dir.path().join("conversation.db")).unwrap();
    prepared_fenced(&backend).await;
}
