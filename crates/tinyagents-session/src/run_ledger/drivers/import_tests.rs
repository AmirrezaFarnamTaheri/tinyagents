use super::super::tests::ledgers;
use super::*;
use crate::legacy::fixtures::classic_database;
use tinystoragedrivers_core::ErrorKind;
use tinystoragedrivers_sqlite::rusqlite;

const STARTED: &str = "2026-01-01T00:00:00+00:00";

/// A closed, checkpointed classic database holding run `run` (the shape
/// `agent_tests::run` builds), the given events, and optionally telemetry.
fn classic_source(
    ledger: &DriverRunLedger,
    root: &tempfile::TempDir,
    events: &[(i64, &str)],
    input_tokens: Option<i64>,
) -> QuiescentSource {
    let path = root.path().join("snapshot.db");
    classic_database(&path, 7, |db| {
        db.execute(
            "INSERT INTO agent_runs (id, kind, parent_run_id, parent_thread_id, agent_id, status,
                                     prompt_ref, worker_thread_id, checkpoint_json, summary,
                                     metadata_json, started_at, updated_at)
             VALUES ('run', 'worker_thread', 'parent', 'thread', 'agent', 'running', 'prompt',
                     'worker', '{\"step\":1}', 'original', '{\"keep\":true}', ?1, ?1)",
            [STARTED],
        )
        .unwrap();
        for (sequence, label) in events {
            db.execute(
                "INSERT INTO run_events (run_id, sequence, event_type, payload_json, timestamp)
                 VALUES ('run', ?1, ?2, '{\"source\":true}', ?3)",
                rusqlite::params![sequence, label, STARTED],
            )
            .unwrap();
        }
        if let Some(tokens) = input_tokens {
            db.execute(
                "INSERT INTO run_telemetry (run_id, input_tokens, updated_at) VALUES ('run', ?1, ?2)",
                rusqlite::params![tokens, STARTED],
            )
            .unwrap();
        }
    });
    QuiescentSource::asserted_by_trusted_host(
        path,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    )
}

/// Cross-tenant authority is rejected before a missing source can be opened or created.
#[test]
fn preflight_rejects_wrong_owner_before_source_open() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing.db");
    let source = QuiescentSource::asserted_by_trusted_host(
        &path,
        SourceOwner::asserted_by_trusted_host(Scope::new("profile-b").unwrap()),
    );
    assert!(matches!(ledger.preflight_source(&source).unwrap_err(),
        super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::InvalidInput));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

/// Source preflight reads historical schema without applying pending migrations or writing sidecars.
#[test]
fn preflight_checkpointed_source_preserves_directory_bytes() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.db");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE schema_version(id INTEGER PRIMARY KEY, version INTEGER NOT NULL); INSERT INTO schema_version VALUES(1, 2); CREATE TABLE agent_runs(id TEXT PRIMARY KEY); INSERT INTO agent_runs VALUES('old');").unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let source = QuiescentSource::asserted_by_trusted_host(
        &path,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    );
    let snapshot = ledger.preflight_source(&source).unwrap();
    assert_eq!(snapshot.schema_version, 2);
    assert_eq!(snapshot.columns("agent_runs").unwrap(), ["id"]);
    assert!(snapshot.tables.iter().any(|table| table == "agent_runs"));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

/// An uncheckpointed WAL is never ignored and every original directory entry stays unchanged.
#[test]
fn preflight_rejects_wal_without_mutating_source_or_auxiliary_files() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.db");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE agent_runs(id TEXT); INSERT INTO agent_runs VALUES('committed-in-wal');").unwrap();
    let before: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            (path.clone(), std::fs::read(path).unwrap())
        })
        .collect();
    assert!(
        before
            .iter()
            .any(|(path, _)| path.to_string_lossy().ends_with("-wal"))
    );
    let source = QuiescentSource::asserted_by_trusted_host(
        &path,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    );
    assert!(matches!(
        ledger.preflight_source(&source).unwrap_err(),
        super::super::super::LedgerError::SourceRequiresSnapshot { .. }
    ));
    assert_eq!(
        std::fs::read_dir(root.path()).unwrap().count(),
        before.len()
    );
    for (path, bytes) in before {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    drop(db);
}

/// A correctly owned missing source returns absence without creating its directory or database.
#[test]
fn preflight_missing_owned_source_never_creates_database() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing-parent").join("missing.db");
    let source = QuiescentSource::asserted_by_trusted_host(
        &path,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    );
    assert!(matches!(ledger.preflight_source(&source).unwrap_err(),
        super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::NotFound));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

/// URI-reserved characters cannot select a different SQLite file or disable immutable admission.
#[test]
fn preflight_reserved_path_characters_preserve_original_source() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let name = if cfg!(windows) {
        "source #% ü.db"
    } else {
        "source ?#% ü.db"
    };
    let path = root.path().join(name);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE historical(id TEXT);")
        .unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let source = QuiescentSource::asserted_by_trusted_host(
        &path,
        SourceOwner::asserted_by_trusted_host(ledger.scope().clone()),
    );
    let snapshot = ledger.preflight_source(&source).unwrap();
    assert_eq!(snapshot.schema_version, -1);
    assert_eq!(snapshot.columns("historical").unwrap(), ["id"]);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn admitted_agent_import_preserves_source_and_retry_never_overwrites_native_edits() {
    let (ledger, sibling) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let source = classic_source(&ledger, &root, &[(1, "old")], Some(7));
    let path = source.path().to_path_buf();
    let bytes = std::fs::read(&path).unwrap();
    let report = ledger
        .import_agent_family(&source, "import-1")
        .await
        .unwrap();
    assert_eq!(report.agent_runs, 1);
    assert_eq!(report.events, 1);
    assert_eq!(report.telemetry, 1);
    let row = ledger.get_agent_run("run").await.unwrap().unwrap();
    assert_eq!(row.started_at.to_rfc3339(), STARTED);
    assert_eq!(row.telemetry.unwrap().input_tokens, 7);
    ledger
        .transition_agent_run_status("run", AgentRunStatus::Completed, None, None)
        .await
        .unwrap();
    assert_eq!(
        ledger
            .import_agent_family(&source, "import-1")
            .await
            .unwrap(),
        report
    );
    assert_eq!(
        ledger.get_agent_run("run").await.unwrap().unwrap().status,
        AgentRunStatus::Completed
    );
    assert!(sibling.get_agent_run("run").await.unwrap().is_none());
    assert_eq!(ledger.reconcile_run_events("run").await.unwrap(), 1);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

fn complete_source(ledger: &DriverRunLedger, root: &tempfile::TempDir) -> QuiescentSource {
    classic_source(ledger, root, &[(1, "one"), (2, "two"), (3, "three")], None)
}

#[tokio::test]
async fn later_event_collision_rolls_back_runs_and_import_receipt_together() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    ledger
        .append_run_event(RunEventAppend {
            run_id: "run".into(),
            event_type: "native".into(),
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
    assert!(
        matches!(ledger.import_agent_family(&source, "import").await.unwrap_err(), super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::Conflict)
    );
    assert!(ledger.get_agent_run("run").await.unwrap().is_none());
    assert!(
        ledger
            .read::<ImportReceipt>(super::super::storage::IMPORTS, "import")
            .await
            .unwrap()
            .is_none()
    );
    let events = ledger
        .list_recent_run_events(&RunEventListRequest {
            run_id: "run".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(events.count, 1);
    assert_eq!(events.events[0].event_type, "native");
}

#[tokio::test]
async fn imported_pruned_events_keep_original_ordinals_and_continue_allocation() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    let db = rusqlite::Connection::open(source.path()).unwrap();
    db.execute("DELETE FROM run_events WHERE sequence < 3", [])
        .unwrap();
    drop(db);
    ledger.import_agent_family(&source, "import").await.unwrap();
    let appended = ledger
        .append_run_event(RunEventAppend {
            run_id: "run".into(),
            event_type: "four".into(),
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
    assert_eq!(appended.sequence, 4);
    assert_eq!(ledger.reconcile_run_events("run").await.unwrap(), 2);
    let events = ledger
        .list_recent_run_events(&RunEventListRequest {
            run_id: "run".into(),
            after_sequence: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        events
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [3, 4]
    );
}

#[tokio::test]
async fn changed_source_cannot_reuse_a_committed_import_operation() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    ledger.import_agent_family(&source, "import").await.unwrap();
    let db = rusqlite::Connection::open(source.path()).unwrap();
    db.execute("UPDATE agent_runs SET summary = 'changed'", [])
        .unwrap();
    drop(db);
    assert!(
        matches!(ledger.import_agent_family(&source, "import").await.unwrap_err(), super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::Conflict)
    );
    assert_eq!(
        ledger
            .get_agent_run("run")
            .await
            .unwrap()
            .unwrap()
            .summary
            .as_deref(),
        Some("original")
    );
}

#[tokio::test]
async fn imported_event_counter_can_allocate_the_last_classic_signed_sequence() {
    let (ledger, _) = ledgers();
    let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    let db = rusqlite::Connection::open(source.path()).unwrap();
    db.execute("DELETE FROM run_events WHERE sequence != 3", [])
        .unwrap();
    db.execute("UPDATE run_events SET sequence = ?1", [i64::MAX - 1])
        .unwrap();
    drop(db);
    ledger.import_agent_family(&source, "import").await.unwrap();
    let input = || RunEventAppend {
        run_id: "run".into(),
        event_type: "last".into(),
        payload: serde_json::json!({}),
    };
    assert_eq!(
        ledger.append_run_event(input()).await.unwrap().sequence,
        i64::MAX as u64
    );
    assert!(
        matches!(ledger.append_run_event(input()).await.unwrap_err(), super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::Serialization)
    );
}

#[tokio::test]
async fn imported_last_signed_sequence_persists_bson_representable_terminal_head() {
    use tinystoragedrivers_core::{MemoryStorage, StorageBackend};
    let backend = MemoryStorage::new();
    let scope = Scope::local();
    let native = backend.for_scope(&scope).unwrap();
    let ledger = super::super::event_bson_tests::wrap(native);
    let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    let before = {
        let db = rusqlite::Connection::open(source.path()).unwrap();
        db.execute("DELETE FROM run_events WHERE sequence != 3", [])
            .unwrap();
        db.execute("UPDATE run_events SET sequence = ?1", [i64::MAX])
            .unwrap();
        drop(db);
        std::fs::read(source.path()).unwrap()
    };
    ledger
        .import_agent_family(&source, "terminal-import")
        .await
        .unwrap();
    assert_eq!(std::fs::read(source.path()).unwrap(), before);
    assert!(
        matches!(ledger.append_run_event(RunEventAppend { run_id: "run".into(), event_type: "overflow".into(), payload: serde_json::json!({}) }).await.unwrap_err(), super::super::super::LedgerError::Driver(error) if error.kind() == ErrorKind::Serialization)
    );
}
