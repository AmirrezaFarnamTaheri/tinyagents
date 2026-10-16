use super::*;

// The one-time transcript import: marker, idempotence, untouched sources.
use crate::port::DriverSessionStores;
use crate::transcript::{TranscriptMessage, TranscriptMeta, write_transcript};
use std::path::Path;
use std::sync::Arc;
use tinystoragedrivers_core::{MemoryStorage, StorageBackend};

fn meta() -> TranscriptMeta {
    TranscriptMeta {
        session_id: None,
        parent_session_id: None,
        agent_name: "agent".into(),
        agent_id: Some("agent-id".into()),
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
        thread_id: Some("thread-once".into()),
        task_id: None,
    }
}

fn provider() -> DriverSessionStores {
    DriverSessionStores::new(
        Arc::new(MemoryStorage::new()) as Arc<dyn StorageBackend>,
        tinystoragedrivers_core::Scope::local(),
    )
    .unwrap()
}

fn write(workspace: &Path, stem: &str, text: &str) -> std::path::PathBuf {
    let raw = workspace.join("session_raw");
    std::fs::create_dir_all(&raw).unwrap();
    let path = raw.join(format!("{stem}.jsonl"));
    write_transcript(
        &path,
        &[
            TranscriptMessage::user(text),
            TranscriptMessage::assistant("ok"),
        ],
        &meta(),
        None,
    )
    .unwrap();
    path
}

#[test]
fn a_second_launch_skips_the_legacy_scan_and_never_touches_the_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "1700000000_agent", "hello");
    let before = std::fs::read(&path).unwrap();
    let provider = provider();

    let first = import_transcripts_once(&provider, "agent", dir.path()).unwrap();
    let OnceOutcome::Imported { report, marked } = first else {
        panic!("first launch imports");
    };
    assert_eq!((report.imported, report.already_present), (1, 0));
    assert!(marked);

    // A file an old binary drops in after the marker is not picked up: the
    // import is one-time.
    write(dir.path(), "1700000001_agent", "late");
    let second = import_transcripts_once(&provider, "agent", dir.path()).unwrap();
    assert_eq!(second, OnceOutcome::AlreadyImported);
    let stores = provider.try_for_agent("agent").unwrap();
    let late = stores
        .transcripts
        .open_stem("1700000001_agent", meta())
        .unwrap();
    assert!(late.read_session().unwrap().is_none());

    assert_eq!(std::fs::read(&path).unwrap(), before, "source untouched");
}

#[test]
fn an_unreadable_file_leaves_the_import_unmarked_so_it_is_retried() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "good", "hi");
    std::fs::write(dir.path().join("session_raw").join("bad.jsonl"), "").unwrap();
    let provider = provider();

    let first = import_transcripts_once(&provider, "agent", dir.path()).unwrap();
    let OnceOutcome::Imported { report, marked } = first else {
        panic!("imports");
    };
    assert_eq!(report.unreadable, 1);
    assert!(!marked);

    // Retry is idempotent: the good transcript is already there.
    let again = import_transcripts_once(&provider, "agent", dir.path()).unwrap();
    let OnceOutcome::Imported { report, .. } = again else {
        panic!("retries");
    };
    assert_eq!((report.imported, report.already_present), (0, 1));
}

#[test]
fn markers_are_per_agent_scope() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "s", "x");
    let provider = provider();
    import_transcripts_once(&provider, "a", dir.path()).unwrap();
    let other = import_transcripts_once(&provider, "b", dir.path()).unwrap();
    assert!(matches!(other, OnceOutcome::Imported { marked: true, .. }));
}
