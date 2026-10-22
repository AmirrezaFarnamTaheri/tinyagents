use super::*;
use crate::legacy::OnceOutcome;
use crate::port::SessionStoreProvider;
use crate::testkit::conformance::{
    session_store_conformance, session_store_isolation_conformance, session_store_lease_conformance,
};
use crate::transcript::{
    SessionRef, TranscriptMessage, TranscriptMeta, TranscriptTurn, session_stem, write_transcript,
};

fn meta() -> TranscriptMeta {
    TranscriptMeta {
        session_id: None,
        parent_session_id: None,
        agent_name: "agent".into(),
        agent_id: Some("agent".into()),
        agent_type: Some("root".into()),
        dispatcher: "native".into(),
        provider: None,
        model: None,
        created: "2026-01-01T00:00:00Z".into(),
        updated: "2026-01-01T00:00:00Z".into(),
        turn_count: 1,
        prefix_message_count: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_input_tokens: 0,
        charged_amount_usd: 0.0,
        thread_id: Some("thread-ws".into()),
        task_id: None,
    }
}

#[test]
fn legacy_generations_resume_through_the_port_and_a_new_generation_chains() {
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("session_raw");
    std::fs::create_dir_all(&raw).unwrap();
    let session = SessionRef::root("thread-ws");
    let g1 = session.next_generation();
    let sealed = raw.join(format!("{}.jsonl", session_stem(&session)));
    let head = raw.join(format!("{}.jsonl", session_stem(&g1)));
    let rows = |t: &str| {
        vec![
            TranscriptMessage::user(t),
            TranscriptMessage::assistant("ok"),
        ]
    };
    write_transcript(&sealed, &rows("first"), &meta(), None).unwrap();
    write_transcript(&head, &rows("second"), &meta(), None).unwrap();
    let (sealed_bytes, head_bytes) = (
        std::fs::read(&sealed).unwrap(),
        std::fs::read(&head).unwrap(),
    );

    let opened = WorkspaceStores::open(dir.path(), "agent").unwrap();
    let OnceOutcome::Imported { report, marked } = opened.import_outcome() else {
        panic!("first open imports");
    };
    assert_eq!(report.imported, 2);
    assert!(marked);

    let locator = opened.transcripts();
    assert_eq!(
        locator.session_chain(&session),
        vec![session.clone(), g1.clone()]
    );
    assert_eq!(locator.head_generation(&session), g1);
    let resumed = locator
        .read_session_transcript(&g1)
        .unwrap()
        .read_session()
        .unwrap()
        .unwrap();
    assert_eq!(resumed.messages[0].content, "second");

    // Compaction never erases: a new generation chains, both predecessors stay.
    let (g2, handle) = locator.begin_generation(&g1, meta()).unwrap();
    let next = rows("summary");
    handle
        .append_turn(TranscriptTurn {
            prev: &[],
            next: &next,
            meta: &meta(),
            turn_usage: None,
            request_id: None,
            tools: None,
        })
        .unwrap();
    assert_eq!(locator.session_chain(&session).len(), 3);
    assert_eq!(locator.head_generation(&session), g2);
    assert!(locator.session_exists(&session));

    // Legacy files were only read.
    assert_eq!(std::fs::read(&sealed).unwrap(), sealed_bytes);
    assert_eq!(std::fs::read(&head).unwrap(), head_bytes);
    assert!(dir.path().join(STORE_DIR).is_dir());

    // The handle is cached per workspace and agent.
    let again = WorkspaceStores::open(dir.path(), "agent").unwrap();
    assert!(Arc::ptr_eq(again.provider(), opened.provider()));
}

#[test]
fn a_workspace_with_no_legacy_files_opens_empty() {
    let dir = tempfile::tempdir().unwrap();
    let opened = WorkspaceStores::open(dir.path(), "agent").unwrap();
    assert!(opened.transcripts().latest_for_agent("agent").is_none());
}

#[tokio::test]
async fn workspace_provider_passes_full_session_store_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let opened = WorkspaceStores::open(dir.path(), "conformance-agent").unwrap();
    let provider = opened.provider();
    session_store_conformance(provider.as_ref()).await;
    session_store_isolation_conformance(provider.as_ref()).await;
    session_store_lease_conformance(provider.as_ref()).await;
}

#[tokio::test]
async fn a_workspace_provider_lease_keeps_the_opened_store_alive() {
    let dir = tempfile::tempdir().unwrap();
    let opened = WorkspaceStores::open(dir.path(), "agent").unwrap();
    let lease = opened.provider().lease_for_agent("agent").unwrap();
    lease
        .stores()
        .kv
        .put("lease-test", "key", serde_json::json!("retained"))
        .await
        .unwrap();
    drop(opened);
    assert_eq!(
        lease.stores().kv.get("lease-test", "key").await.unwrap(),
        Some(serde_json::json!("retained"))
    );
    lease
        .stores()
        .kv
        .put(
            "lease-test",
            "after-drop",
            serde_json::json!("written through lease"),
        )
        .await
        .unwrap();

    let reopened = WorkspaceStores::open(dir.path(), "agent").unwrap();
    assert_eq!(
        reopened
            .provider()
            .lease_for_agent("agent")
            .unwrap()
            .stores()
            .kv
            .get("lease-test", "key")
            .await
            .unwrap(),
        Some(serde_json::json!("retained"))
    );
    assert_eq!(
        reopened
            .provider()
            .lease_for_agent("agent")
            .unwrap()
            .stores()
            .kv
            .get("lease-test", "after-drop")
            .await
            .unwrap(),
        Some(serde_json::json!("written through lease"))
    );
}

#[test]
fn a_dropped_handle_releases_its_backend_and_reopens_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let first = WorkspaceStores::open(dir.path(), "agent").unwrap();
    let weak = Arc::downgrade(first.provider());
    drop(first);
    assert!(
        weak.upgrade().is_none(),
        "cache must not keep the backend alive"
    );
    WorkspaceStores::open(dir.path(), "agent").unwrap();
}

#[test]
fn an_incomplete_import_is_retried_on_the_next_open() {
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("session_raw");
    std::fs::create_dir_all(&raw).unwrap();
    std::fs::write(raw.join("broken.jsonl"), "not json at all\n").unwrap();
    let first = WorkspaceStores::open(dir.path(), "agent").unwrap();
    assert!(matches!(
        first.import_outcome(),
        OnceOutcome::Imported { marked: false, .. }
    ));
    let second = WorkspaceStores::open(dir.path(), "agent").unwrap();
    assert!(!Arc::ptr_eq(first.provider(), second.provider()));
}
