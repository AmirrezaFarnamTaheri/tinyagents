use super::*;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, MemoryStorage, Scope, StorageBackend};

// Explicit fixture preparation is owning bootstrap authority, not actor DDL.
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

#[path = "driver_append_fixtures.rs"]
mod fixtures;
use fixtures::{Action, Fault, Phase, actor, faulted};

async fn canceled(backend: &dyn StorageBackend, phase: Phase) {
    let store = actor(backend).await;
    let fault = Fault::new(phase, Action::Pause);
    let interrupted = faulted(&store, fault.clone());
    let mut future =
        Box::pin(interrupted.append_message("same", message("agent:first", "accepted")));
    tokio::select! {
        _ = fault.reached.notified() => {},
        result = &mut future => panic!("phase never paused: {result:?}"),
    }
    assert!(
        store.get_messages("same").await.unwrap().is_empty(),
        "unpublished rows must remain invisible"
    );
    assert_eq!(
        store
            .get_thread("same")
            .await
            .unwrap()
            .unwrap()
            .message_count,
        0
    );
    drop(future);
    let resumed = DriverConversationStore { ..store.clone() };
    resumed
        .append_message("same", message("agent:second", "next"))
        .await
        .unwrap();
    let rows = resumed.get_messages("same").await.unwrap();
    let expected = if phase == Phase::Probe { 1 } else { 2 };
    assert_eq!(rows.len(), expected);
    if phase != Phase::Probe {
        assert_eq!(rows[0].content, "accepted");
    }
    assert_eq!(
        resumed
            .docs
            .count(PENDING_COLLECTION, &Filter::All)
            .await
            .unwrap(),
        0
    );
}
#[tokio::test]
async fn canceled_memory_writers_keep_reserved_and_appended_rows_invisible_until_recovery() {
    for phase in [Phase::Probe, Phase::Reserve, Phase::Append] {
        canceled(&MemoryStorage::new(), phase).await;
    }
}
#[tokio::test]
async fn canceled_sqlite_writers_keep_reserved_and_appended_rows_invisible_until_recovery() {
    for phase in [Phase::Probe, Phase::Reserve, Phase::Append] {
        let dir = tempfile::tempdir().unwrap();
        let backend =
            tinystoragedrivers_sqlite::SqliteStorage::open(dir.path().join("phase.db")).unwrap();
        canceled(&backend, phase).await;
    }
}
#[tokio::test]
async fn lost_native_acknowledgments_reconcile_exact_reserved_attempt_without_duplicates() {
    for phase in [Phase::Reserve, Phase::Append, Phase::Finalize] {
        let store = actor(&MemoryStorage::new()).await;
        let interrupted = faulted(&store, Fault::new(phase, Action::LoseAck));
        let original = message("ordinary", "one actual attempt");
        assert_eq!(
            interrupted
                .append_message("same", original.clone())
                .await
                .unwrap(),
            original
        );
        assert_eq!(store.get_messages("same").await.unwrap(), vec![original]);
        assert_eq!(
            store
                .docs
                .count(PENDING_COLLECTION, &Filter::All)
                .await
                .unwrap(),
            0
        );
    }
}
#[tokio::test]
async fn unobserved_ordinary_reservation_returns_typed_uncertainty_with_original_native_source() {
    use std::error::Error;
    let store = actor(&MemoryStorage::new()).await;
    let interrupted = faulted(&store, Fault::new(Phase::Reserve, Action::FailBefore));
    let error = interrupted
        .append_message("same", message("ordinary", "not accepted"))
        .await
        .unwrap_err();
    let ConversationError::Uncertain { source, .. } = error else {
        panic!("expected typed uncertainty")
    };
    assert_eq!(source.kind(), ErrorKind::Unavailable);
    assert_eq!(
        source
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .to_string(),
        "original native cause"
    );
    assert!(store.get_messages("same").await.unwrap().is_empty());
    assert_eq!(
        store
            .docs
            .count(PENDING_COLLECTION, &Filter::All)
            .await
            .unwrap(),
        0
    );
}
#[tokio::test]
async fn counterfeit_document_capabilities_do_not_reserve_when_actual_stream_probe_refuses() {
    let store = actor(&MemoryStorage::new()).await;
    let interrupted = faulted(&store, Fault::new(Phase::Probe, Action::Unsupported));
    let error = interrupted
        .append_message("same", message("agent:a", "refused"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ConversationError::Driver(ref e) if e.kind() == ErrorKind::Unsupported)
    );
    assert!(store.get_messages("same").await.unwrap().is_empty());
    assert_eq!(
        store
            .docs
            .count(PENDING_COLLECTION, &Filter::All)
            .await
            .unwrap(),
        0
    );
}
