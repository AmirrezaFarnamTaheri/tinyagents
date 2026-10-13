//! Importer tests for the file stores: JSONL transcripts and turn-state JSON.

use super::*;
use crate::port::{DriverSessionStores, TurnStates};
use crate::transcript::{
    MessageUsage, TranscriptMessage, TranscriptMeta, TurnUsage, append_interrupted_partial,
    append_tools_record, append_transcript_turn, read_transcript, read_transcript_display,
    write_transcript,
};
use crate::turn_state::{TurnLifecycle, TurnState};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tinystoragedrivers_core::{MemoryStorage, StorageBackend};

fn meta(thread: &str, turns: usize) -> TranscriptMeta {
    TranscriptMeta {
        session_id: None,
        parent_session_id: None,
        agent_name: "agent".into(),
        agent_id: Some("agent-id".into()),
        agent_type: Some("root".into()),
        dispatcher: "native".into(),
        provider: Some("provider".into()),
        model: Some("model".into()),
        created: "2026-01-01T00:00:00Z".into(),
        updated: "2026-01-01T00:00:00Z".into(),
        turn_count: turns,
        prefix_message_count: None,
        input_tokens: 10,
        output_tokens: 20,
        cached_input_tokens: 0,
        charged_amount_usd: 0.5,
        thread_id: Some(thread.into()),
        task_id: None,
    }
}

fn usage(iteration: u32) -> TurnUsage {
    TurnUsage {
        provider: "provider".into(),
        model: "model".into(),
        usage: MessageUsage {
            input: 10,
            output: 20,
            cost_usd: 0.01,
            ..Default::default()
        },
        ts: "2026-01-01T00:00:01Z".into(),
        reasoning_content: Some("because".into()),
        tool_calls: Vec::new(),
        iteration,
    }
}

fn provider() -> DriverSessionStores {
    DriverSessionStores::new(Arc::new(MemoryStorage::new()) as Arc<dyn StorageBackend>).unwrap()
}

fn raw(dir: &Path) -> PathBuf {
    let raw = dir.join("session_raw");
    std::fs::create_dir_all(&raw).unwrap();
    raw
}

/// A three-turn conversation with a compaction in the middle and an
/// interrupted partial at the end, written by the classic JSONL writer.
fn write_compacted_conversation(path: &Path) {
    let m = meta("thread-1", 1);
    let hello = vec![
        TranscriptMessage::system("be brief"),
        TranscriptMessage::user("first question"),
        TranscriptMessage::assistant("first answer"),
    ];
    append_transcript_turn(path, &[], &hello, &m, Some(&usage(1)), Some("req-1")).unwrap();
    let mut second = hello.clone();
    second.push(TranscriptMessage::user("second question"));
    second.push(TranscriptMessage::assistant("second answer"));
    append_transcript_turn(path, &hello, &second, &m, Some(&usage(2)), Some("req-2")).unwrap();
    // Compaction: the retained set is not an extension, so the old rows stay
    // on disk and a replacement record takes over the model's context.
    let compacted = vec![
        TranscriptMessage::system("be brief"),
        TranscriptMessage::user("summary of the earlier turns"),
    ];
    append_transcript_turn(path, &second, &compacted, &m, None, Some("req-3")).unwrap();
    append_interrupted_partial(path, "half an answ", Some("req-4"), Some(2), Some("hmm")).unwrap();
    append_tools_record(path, &serde_json::json!([{"name": "grep"}])).unwrap();
}

#[test]
fn a_transcript_replays_with_compaction_partials_usage_and_tools() {
    let dir = tempfile::tempdir().unwrap();
    let path = raw(dir.path()).join("1700000000_agent.jsonl");
    write_compacted_conversation(&path);
    let before = std::fs::read(&path).unwrap();
    let provider = provider();
    let stores = provider.try_for_agent("agent").unwrap();

    let report = import_transcripts(dir.path(), stores.transcripts.as_ref()).unwrap();
    assert_eq!(report.read, 1);
    assert_eq!(report.imported, 1);
    assert_eq!(report.already_present, 0);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "the source is untouched"
    );

    // The model-context replay equals the file's.
    let source = read_transcript(&path).unwrap();
    let history = stores
        .transcripts
        .open_stem("1700000000_agent", meta("thread-1", 1))
        .unwrap();
    let imported = history.read_session().unwrap().expect("imported");
    assert_eq!(
        imported
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        source
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(imported.messages.len(), 2, "the compaction took effect");
    assert_eq!(
        imported.tools, source.tools,
        "recorded tool declarations survive"
    );
    assert_eq!(imported.meta.thread_id.as_deref(), Some("thread-1"));

    // Idempotent: a second pass finds it and leaves it alone.
    let again = import_transcripts(dir.path(), stores.transcripts.as_ref()).unwrap();
    assert_eq!((again.imported, again.already_present), (0, 1));

    // The thread lookup the host uses finds the imported root.
    assert!(stores.transcripts.root_for_thread("thread-1").is_some());
    let _ = read_transcript_display(&path).unwrap();
}

#[test]
fn generations_subagents_and_date_directories_keep_their_stems() {
    let dir = tempfile::tempdir().unwrap();
    let raw = raw(dir.path());
    let m = meta("thread-2", 1);
    let rows = |text: &str| {
        vec![
            TranscriptMessage::user(text),
            TranscriptMessage::assistant("ok"),
        ]
    };
    for stem in ["root", "root.g1", "root__child"] {
        write_transcript(&raw.join(format!("{stem}.jsonl")), &rows(stem), &m, None).unwrap();
    }
    // The pre-flat layout: a date directory with the same stem as a flat file
    // is a stale duplicate, and a stem only the date directory has is kept.
    std::fs::create_dir_all(raw.join("01012026")).unwrap();
    write_transcript(
        &raw.join("01012026").join("root.jsonl"),
        &rows("stale"),
        &m,
        None,
    )
    .unwrap();
    write_transcript(
        &raw.join("01012026").join("dated.jsonl"),
        &rows("dated"),
        &m,
        None,
    )
    .unwrap();
    std::fs::write(raw.join("notes.txt"), "not a transcript").unwrap();
    // A markdown session from the oldest layout is counted, not converted.
    std::fs::create_dir_all(dir.path().join("sessions").join("01012026")).unwrap();
    std::fs::write(
        dir.path().join("sessions").join("01012026").join("old.md"),
        "# old",
    )
    .unwrap();

    let provider = provider();
    let stores = provider.try_for_agent("agent").unwrap();
    let report = import_transcripts(dir.path(), stores.transcripts.as_ref()).unwrap();
    assert_eq!(report.imported, 4);
    assert_eq!(report.markdown_skipped, 1);
    for stem in ["root", "root.g1", "root__child", "dated"] {
        let history = stores.transcripts.open_stem(stem, m.clone()).unwrap();
        assert!(history.read_session().unwrap().is_some(), "{stem}");
    }
    let root = stores.transcripts.open_stem("root", m.clone()).unwrap();
    assert_eq!(
        root.read_session().unwrap().unwrap().messages[0].content,
        "root",
        "the flat file wins over the date-grouped duplicate"
    );
}

#[test]
fn an_unreadable_transcript_is_skipped_and_an_existing_one_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let raw = raw(dir.path());
    let m = meta("thread-3", 1);
    write_transcript(
        &raw.join("good.jsonl"),
        &[TranscriptMessage::user("hi")],
        &m,
        None,
    )
    .unwrap();
    write_transcript(
        &raw.join("kept.jsonl"),
        &[TranscriptMessage::user("from the file")],
        &m,
        None,
    )
    .unwrap();
    std::fs::write(raw.join("empty.jsonl"), "").unwrap();

    let provider = provider();
    let stores = provider.try_for_agent("agent").unwrap();
    // The destination already holds `kept`, with different content.
    let existing = stores.transcripts.open_stem("kept", m.clone()).unwrap();
    existing
        .append(TranscriptMessage::user("already here"))
        .unwrap();

    let report = import_transcripts(dir.path(), stores.transcripts.as_ref()).unwrap();
    assert_eq!(report.imported, 1, "{report:?}");
    assert_eq!(report.already_present, 1);
    assert_eq!(report.unreadable, 1);
    let kept = stores.transcripts.open_stem("kept", m).unwrap();
    assert_eq!(
        kept.read_session().unwrap().unwrap().messages[0].content,
        "already here"
    );
}

#[test]
fn a_workspace_without_a_raw_directory_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let provider = provider();
    let stores = provider.try_for_agent("agent").unwrap();
    assert_eq!(
        import_transcripts(dir.path(), stores.transcripts.as_ref()).unwrap(),
        TranscriptImportReport::default()
    );
    assert_eq!(
        import_turn_states(dir.path(), stores.turn_states.as_ref()).unwrap(),
        TurnStateImportReport::default()
    );
}

fn state(thread: &str, request: &str, lifecycle: TurnLifecycle) -> TurnState {
    let mut state = TurnState::started(thread, request, 4, "2026-01-01T00:00:00Z");
    state.lifecycle = lifecycle;
    state
}

/// Writes one snapshot in the classic per-turn layout
/// (`turn_states/<hex(thread)>/<hex(request)>.json`), the shape the importer reads.
fn write_snapshot(workspace: &std::path::Path, state: &TurnState) {
    let thread_dir = turn_states::turn_state_dir(workspace).join(hex::encode(&state.thread_id));
    std::fs::create_dir_all(&thread_dir).unwrap();
    std::fs::write(
        thread_dir.join(format!("{}.json", hex::encode(&state.request_id))),
        serde_json::to_string_pretty(state).unwrap(),
    )
    .unwrap();
}

#[test]
fn turn_states_keep_their_ids_including_the_flat_legacy_form() {
    let dir = tempfile::tempdir().unwrap();
    write_snapshot(
        dir.path(),
        &state("thread-a", "req-1", TurnLifecycle::Completed),
    );
    write_snapshot(
        dir.path(),
        &state("thread-a", "req-2", TurnLifecycle::Streaming),
    );
    write_snapshot(
        dir.path(),
        &state("thread-b", "req-9", TurnLifecycle::Started),
    );
    // The flat form older cores wrote: <hex(thread)>.json at the root.
    let flat =
        turn_states::turn_state_dir(dir.path()).join(format!("{}.json", hex::encode("thread-c")));
    std::fs::write(
        &flat,
        serde_json::to_string(&state("thread-c", "req-7", TurnLifecycle::Interrupted)).unwrap(),
    )
    .unwrap();
    std::fs::write(
        turn_states::turn_state_dir(dir.path()).join("garbage.json"),
        "{not json",
    )
    .unwrap();
    let snapshot_before: Vec<_> = walk(&turn_states::turn_state_dir(dir.path()));

    let provider = provider();
    let stores = provider.try_for_agent("agent").unwrap();
    let report = import_turn_states(dir.path(), stores.turn_states.as_ref()).unwrap();
    assert_eq!(report.read, 4);
    assert_eq!(report.imported, 4);
    assert_eq!(report.unreadable, 1);
    // Nothing under the source moved: the flat file was NOT migrated in place.
    assert_eq!(
        walk(&turn_states::turn_state_dir(dir.path())),
        snapshot_before
    );
    assert!(flat.exists());

    let turns: Arc<dyn TurnStates> = stores.turn_states.clone();
    assert_eq!(turns.list_thread("thread-a").unwrap().len(), 2);
    assert_eq!(
        turns
            .get_turn("thread-a", "req-1")
            .unwrap()
            .unwrap()
            .lifecycle,
        TurnLifecycle::Completed
    );
    assert_eq!(
        turns
            .get_turn("thread-c", "req-7")
            .unwrap()
            .unwrap()
            .lifecycle,
        TurnLifecycle::Interrupted
    );

    // Edit one, re-run: nothing is overwritten and nothing is re-imported.
    let mut edited = state("thread-b", "req-9", TurnLifecycle::Completed);
    edited.iteration = 3;
    turns.put(&edited).unwrap();
    let again = import_turn_states(dir.path(), turns.as_ref()).unwrap();
    assert_eq!((again.imported, again.already_present), (0, 4));
    assert_eq!(
        turns
            .get_turn("thread-b", "req-9")
            .unwrap()
            .unwrap()
            .iteration,
        3
    );
}

fn walk(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push((path.clone(), std::fs::read(&path).unwrap()));
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_whole_workspace_imports_into_one_agents_stores() {
    let dir = tempfile::tempdir().unwrap();
    // Transcripts and turn states from the file stores...
    write_compacted_conversation(&raw(dir.path()).join("1700000000_agent.jsonl"));
    write_snapshot(
        dir.path(),
        &state("thread-1", "req-1", TurnLifecycle::Interrupted),
    );
    // ...and the database.
    let db_dir = dir.path().join("session_db");
    std::fs::create_dir_all(&db_dir).unwrap();
    super::fixtures::classic_database(&db_dir.join("sessions.db"), 7, |db| {
        db.execute_batch(
            "INSERT INTO sessions (id, agent_definition_id, agent_definition_name, session_key,
                                   status, started_at)
             VALUES ('s1', 'a', 'Agent One', 'k', 'completed', '2026-01-01T00:00:00+00:00');
             INSERT INTO session_messages (id, session_id, role, content, created_at)
             VALUES (4, 's1', 'user', 'findable words', '2026-01-01T00:00:00+00:00');",
        )
        .unwrap();
    });

    let provider = provider();
    let report = import_workspace(&provider, "agent", dir.path())
        .await
        .unwrap();
    let database = report.database.expect("the database was imported");
    assert_eq!(database.messages.imported, 1);
    assert_eq!(database.fts_rows, 2);
    assert_eq!(report.transcripts.imported, 1);
    assert_eq!(report.turn_states.imported, 1);

    let stores = provider.try_for_agent("agent").unwrap();
    assert!(stores.transcripts.root_for_thread("thread-1").is_some());
    let ledger = provider.driver_run_ledger("agent").unwrap();
    use crate::run_ledger::SessionHistoryStore;
    assert_eq!(ledger.list_messages("s1", None).await.unwrap()[0].id, 4);

    // Another agent saw none of it; a repeat changes nothing.
    let other = provider.try_for_agent("someone-else").unwrap();
    assert!(other.transcripts.root_for_thread("thread-1").is_none());
    let again = import_workspace(&provider, "agent", dir.path())
        .await
        .unwrap();
    assert_eq!(again.transcripts.imported, 0);
    assert_eq!(again.turn_states.imported, 0);
    assert_eq!(again.database.unwrap().messages.imported, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workspace_with_nothing_in_it_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let report = import_workspace(&provider(), "agent", dir.path())
        .await
        .unwrap();
    assert_eq!(report, LegacyWorkspaceReport::default());
}
