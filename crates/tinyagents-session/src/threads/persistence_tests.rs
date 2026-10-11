use super::*;
use serde_json::json;

fn thread(id: &str) -> CreateConversationThread {
    CreateConversationThread {
        id: id.into(),
        title: "First".into(),
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
        created_at: "2026-01-02T00:00:00Z".into(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn file_adapter_preserves_deterministic_append_and_missing_thread_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let store: Box<dyn ConversationPersistence> =
        Box::new(FileConversationStore::new(dir.path().into()));
    assert!(store.get_thread("missing").await.unwrap().is_none());
    assert!(store.get_messages("missing").await.unwrap().is_empty());
    assert!(matches!(
        store
            .append_message("missing", message("agent:x", "first"))
            .await,
        Err(ConversationError::Legacy(_))
    ));
    store.ensure_thread(thread("one")).await.unwrap();
    let first = store
        .append_message("one", message("agent:x", "first"))
        .await
        .unwrap();
    assert_eq!(
        store
            .append_message("one", message("agent:x", "second"))
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        store
            .get_thread("one")
            .await
            .unwrap()
            .unwrap()
            .message_count,
        1
    );
}

#[tokio::test]
async fn file_adapter_preserves_title_labels_and_metadata_patch() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileConversationStore::new(dir.path().into());
    store.ensure_thread(thread("one")).await.unwrap();
    store
        .update_thread_labels("one", vec!["work".into(), "general".into()], "2026-01-03")
        .await
        .unwrap();
    let updated = store
        .update_thread_title("one", "New", "2026-01-04")
        .await
        .unwrap();
    assert_eq!(updated.title, "New");
    assert_eq!(updated.labels, vec!["general"]);
    store
        .append_message("one", message("agent:x", "unchanged"))
        .await
        .unwrap();
    let patched = store
        .update_message(
            "one",
            "agent:x",
            ThreadMessagePatch {
                extra_metadata: Some(json!({"success": true})),
            },
        )
        .await
        .unwrap();
    assert_eq!(patched.content, "unchanged");
    assert_eq!(patched.extra_metadata, json!({"success": true}));
    store
        .update_thread_working_dir("one", Some("/approved".into()), "2026-01-05")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_thread("one")
            .await
            .unwrap()
            .unwrap()
            .working_dir
            .as_deref(),
        Some("/approved")
    );
    store
        .update_thread_working_dir("one", None, "2026-01-06")
        .await
        .unwrap();
    assert!(
        store
            .get_thread("one")
            .await
            .unwrap()
            .unwrap()
            .working_dir
            .is_none()
    );
}

#[tokio::test]
async fn file_adapter_tail_delete_recreate_and_purge_match_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileConversationStore::new(dir.path().into());
    store.ensure_thread(thread("one")).await.unwrap();
    for id in ["a", "b", "c"] {
        store.append_message("one", message(id, id)).await.unwrap();
    }
    assert_eq!(
        store.delete_messages_from("one", "unknown").await.unwrap(),
        None
    );
    assert_eq!(
        store.delete_messages_from("one", "b").await.unwrap(),
        Some(2)
    );
    assert_eq!(
        store.get_messages("one").await.unwrap(),
        vec![message("a", "a")]
    );
    assert!(store.delete_thread("one", "2026-01-03").await.unwrap());
    assert!(!store.delete_thread("one", "2026-01-03").await.unwrap());
    store.ensure_thread(thread("one")).await.unwrap();
    assert!(store.get_messages("one").await.unwrap().is_empty());
    store
        .append_message("one", message("new", "new"))
        .await
        .unwrap();
    let counts = store.purge_threads().await.unwrap();
    assert_eq!((counts.thread_count, counts.message_count), (1, 1));
    assert!(store.list_threads().await.unwrap().is_empty());
}

#[tokio::test]
async fn file_adapter_substring_and_cjk_search_match_existing_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileConversationStore::new(dir.path().into());
    for (id, content) in [
        ("a", "Café substring alphabet"),
        ("b", "東京で日本語"),
        ("c", "unrelated"),
    ] {
        store.ensure_thread(thread(id)).await.unwrap();
        store
            .append_message(id, message(id, content))
            .await
            .unwrap();
    }
    let legacy = ConversationStore::new(dir.path().into());
    for query in ["cafe", "pha", "東京", "日", "missing", "a", ""] {
        assert_eq!(
            store
                .search_cross_thread_messages(query, 20, None)
                .await
                .unwrap(),
            legacy
                .search_cross_thread_messages(query, 20, None)
                .unwrap()
        );
    }
    assert!(
        store
            .search_cross_thread_messages("東京", 20, Some("b"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn file_adapter_refuses_authoritative_turn_claim_without_creating_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileConversationStore::new(dir.path().into());
    let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let request = ConversationTurnClaimRequest {
        thread_id: "one".into(),
        request_id: "turn".into(),
        owner: "pod".into(),
        now,
        lease_until: now + chrono::Duration::seconds(30),
    };
    assert!(matches!(
        store.claim_turn(request).await,
        Err(ConversationError::Unsupported {
            operation: "claim_turn"
        })
    ));
    let claim = ConversationTurnClaim {
        thread_id: "one".into(),
        request_id: "turn".into(),
        owner: "pod".into(),
        epoch: 1,
        lease_until: now,
    };
    assert!(matches!(
        store.renew_turn(&claim, now, now).await,
        Err(ConversationError::Unsupported {
            operation: "renew_turn"
        })
    ));
    assert!(matches!(
        store.complete_turn(&claim, now).await,
        Err(ConversationError::Unsupported {
            operation: "complete_turn"
        })
    ));
    assert!(matches!(
        store.release_turn(&claim, now).await,
        Err(ConversationError::Unsupported {
            operation: "release_turn"
        })
    ));
    assert!(!dir.path().join("memory").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn file_adapter_preserves_channel_dedup_and_optional_upsert_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileConversationStore::new(dir.path().into());
    let mut first = thread("one");
    first.labels = Some(vec!["worker".into()]);
    first.personality_id = Some("persona".into());
    first.working_dir = Some("/approved".into());
    store.ensure_thread(first).await.unwrap();
    let mut updated = thread("one");
    updated.created_at = "2026-01-03T00:00:00Z".into();
    let row = store.ensure_thread(updated).await.unwrap();
    assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
    assert_eq!(row.labels, vec!["tasks"]);
    assert_eq!(row.personality_id.as_deref(), Some("persona"));
    assert_eq!(row.working_dir.as_deref(), Some("/approved"));
    let mut channel = message("platform-id", "first");
    channel.extra_metadata = json!({"scope": "channel"});
    store.append_message("one", channel.clone()).await.unwrap();
    let mut retry = channel.clone();
    retry.content = "different".into();
    assert_eq!(store.append_message("one", retry).await.unwrap(), channel);
    assert_eq!(store.get_messages("one").await.unwrap().len(), 1);
}

#[tokio::test]
async fn file_adapter_reports_legacy_io_failure_without_reclassifying_as_absence() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("memory"), b"not a directory").unwrap();
    let store = FileConversationStore::new(dir.path().into());
    let Err(ConversationError::Legacy(detail)) = store.ensure_thread(thread("one")).await else {
        panic!("expected unchanged classic IO failure");
    };
    assert!(detail.contains("create") || detail.contains("directory"));
    assert_eq!(
        std::fs::read(dir.path().join("memory")).unwrap(),
        b"not a directory"
    );
}
