use super::*;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, MemoryStorage, Scope, StorageBackend};

// Explicit fixture preparation is owning bootstrap authority, not actor DDL.
// The production append-preparation API is added only after the runtime RED.
const PENDING_COLLECTION: &str = "conversation_appends_v1";

fn request(id: &str) -> CreateConversationThread {
    CreateConversationThread {
        id: id.into(),
        title: "Original".into(),
        created_at: "2026-01-01T00:00:00Z".into(),
        parent_thread_id: None,
        labels: None,
        personality_id: None,
        working_dir: None,
    }
}

fn message(id: &str, content: &str) -> ThreadMessage {
    ThreadMessage {
        id: id.into(),
        content: content.into(),
        message_type: "text".into(),
        extra_metadata: json!({}),
        sender: "assistant".into(),
        created_at: "2026-02-01T00:00:00Z".into(),
    }
}

async fn prepared_append(backend: &dyn StorageBackend) {
    backend.prepare_schema().await.unwrap();
    let alice = Scope::new("alice").unwrap();
    let bob = Scope::new("bob").unwrap();
    let raw = backend.for_scope(&alice).unwrap();
    DriverConversationStore::prepare_schema(raw.documents().as_ref())
        .await
        .unwrap();
    raw.documents()
        .ensure_collection(&CollectionSpec::new(PENDING_COLLECTION))
        .await
        .unwrap();
    let restricted = backend.existing_schema().unwrap();
    let a = DriverConversationStore::new_validate_only(restricted.for_scope(&alice).unwrap());
    let b = DriverConversationStore::new_validate_only(restricted.for_scope(&alice).unwrap());
    let other = DriverConversationStore::new_validate_only(restricted.for_scope(&bob).unwrap());
    a.ensure_thread(request("same")).await.unwrap();
    other.ensure_thread(request("same")).await.unwrap();

    let first = message("agent:run", "first original 東京");
    assert_eq!(
        a.append_message("same", first.clone()).await.unwrap(),
        first
    );
    let mut retry = first.clone();
    retry.content = "different retry must return original".into();
    assert_eq!(b.append_message("same", retry).await.unwrap(), first);

    let mut channel = message("channel-row", "channel original");
    channel.extra_metadata = json!({"scope": "channel"});
    assert_eq!(
        a.append_message("same", channel.clone()).await.unwrap(),
        channel
    );
    let mut channel_retry = channel.clone();
    channel_retry.content = "different channel retry".into();
    assert_eq!(
        b.append_message("same", channel_retry).await.unwrap(),
        channel
    );

    // Ordinary repeated IDs retain classic append semantics; no generic
    // exactly-once promise is introduced by the native outbox.
    let mut ordinary = message("ordinary-row", "ordinary repeats append");
    ordinary.created_at = "2026-01-02T00:00:00Z".into();
    a.append_message("same", ordinary.clone()).await.unwrap();
    b.update_thread_title("same", "Edited", "now")
        .await
        .unwrap();
    b.append_message("same", ordinary.clone()).await.unwrap();

    let reopened =
        DriverConversationStore::new_validate_only(restricted.for_scope(&alice).unwrap());
    assert_eq!(
        reopened.get_messages("same").await.unwrap(),
        vec![first, channel, ordinary.clone(), ordinary.clone()]
    );
    let thread = reopened.get_thread("same").await.unwrap().unwrap();
    assert_eq!(thread.title, "Edited");
    assert_eq!(thread.created_at, "2026-01-01T00:00:00Z");
    assert_eq!(thread.message_count, 4);
    assert_eq!(thread.last_message_at, ordinary.created_at);
    assert!(other.get_messages("same").await.unwrap().is_empty());
    assert_eq!(
        other
            .get_thread("same")
            .await
            .unwrap()
            .unwrap()
            .message_count,
        0
    );
    assert_eq!(
        raw.documents()
            .count(PENDING_COLLECTION, &Filter::All)
            .await
            .unwrap(),
        0,
        "successful appends retire their exact pending intent"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_prepared_native_append_reopens_and_preserves_classic_duplicate_semantics() {
    prepared_append(&MemoryStorage::new()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_prepared_native_append_reopens_and_preserves_classic_duplicate_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        tinystoragedrivers_sqlite::SqliteStorage::open(dir.path().join("conversation.db")).unwrap();
    prepared_append(&backend).await;
}
