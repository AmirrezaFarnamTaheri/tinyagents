use super::super::tests::ledgers;
use super::*;
use tinystoragedrivers_core::ErrorKind;

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
    let input = super::super::agent_tests::run("run");
    let original = crate::run_ledger::upsert_agent_run(root.path(), input).unwrap();
    crate::run_ledger::append_run_event(root.path(), RunEventAppend { run_id: "run".into(), event_type: "old".into(), payload: serde_json::json!({"source":true}) }).unwrap();
    crate::run_ledger::upsert_run_telemetry(root.path(), RunTelemetryUpsert { run_id: "run".into(), input_tokens: Some(7), ..Default::default() }).unwrap();
    let path = root.path().join("snapshot.db");
    // Trusted fixture stops source calls and creates a consistent, closed snapshot;
    // the importer must never checkpoint the cached classic connection itself.
    let db = rusqlite::Connection::open(crate::store::db_path(root.path())).unwrap();
    db.execute("VACUUM INTO ?1", [path.to_str().unwrap()]).unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let source = QuiescentSource::asserted_by_trusted_host(&path, SourceOwner::asserted_by_trusted_host(ledger.scope().clone()));
    let report = ledger.import_agent_family(&source, "import-1").await.unwrap();
    assert_eq!(report.agent_runs, 1); assert_eq!(report.events, 1); assert_eq!(report.telemetry, 1);
    let row = ledger.get_agent_run("run").await.unwrap().unwrap();
    assert_eq!(row.started_at, original.started_at); assert_eq!(row.telemetry.unwrap().input_tokens, 7);
    ledger.transition_agent_run_status("run", AgentRunStatus::Completed, None, None).await.unwrap();
    assert_eq!(ledger.import_agent_family(&source, "import-1").await.unwrap(), report);
    assert_eq!(ledger.get_agent_run("run").await.unwrap().unwrap().status, AgentRunStatus::Completed);
    assert!(sibling.get_agent_run("run").await.unwrap().is_none());
    assert_eq!(ledger.reconcile_run_events("run").await.unwrap(), 1);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

fn complete_source(ledger: &DriverRunLedger, root: &tempfile::TempDir) -> QuiescentSource {
    crate::run_ledger::upsert_agent_run(root.path(), super::super::agent_tests::run("run")).unwrap();
    for label in ["one", "two", "three"] {
        crate::run_ledger::append_run_event(root.path(), RunEventAppend { run_id: "run".into(), event_type: label.into(), payload: serde_json::json!({}) }).unwrap();
    }
    let snapshot = root.path().join("snapshot.db");
    let db = rusqlite::Connection::open(crate::store::db_path(root.path())).unwrap();
    db.execute("VACUUM INTO ?1", [snapshot.to_str().unwrap()]).unwrap(); drop(db);
    QuiescentSource::asserted_by_trusted_host(snapshot, SourceOwner::asserted_by_trusted_host(ledger.scope().clone()))
}

#[tokio::test]
async fn later_event_collision_rolls_back_runs_and_import_receipt_together() {
    let (ledger, _) = ledgers(); let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    ledger.append_run_event(RunEventAppend { run_id: "run".into(), event_type: "native".into(), payload: serde_json::json!({}) }).await.unwrap();
    assert!(matches!(ledger.import_agent_family(&source, "import").await.unwrap_err(), super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::Conflict));
    assert!(ledger.get_agent_run("run").await.unwrap().is_none());
    assert!(ledger.read::<ImportReceipt>(super::super::storage::IMPORTS, "import").await.unwrap().is_none());
    let events = ledger.list_recent_run_events(&RunEventListRequest { run_id: "run".into(), ..Default::default() }).await.unwrap();
    assert_eq!(events.count, 1); assert_eq!(events.events[0].event_type, "native");
}

#[tokio::test]
async fn imported_pruned_events_keep_original_ordinals_and_continue_allocation() {
    let (ledger, _) = ledgers(); let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    let db = rusqlite::Connection::open(source.path()).unwrap();
    db.execute("DELETE FROM run_events WHERE sequence < 3", []).unwrap(); drop(db);
    ledger.import_agent_family(&source, "import").await.unwrap();
    let appended = ledger.append_run_event(RunEventAppend { run_id: "run".into(), event_type: "four".into(), payload: serde_json::json!({}) }).await.unwrap();
    assert_eq!(appended.sequence, 4);
    assert_eq!(ledger.reconcile_run_events("run").await.unwrap(), 2);
    let events = ledger.list_recent_run_events(&RunEventListRequest { run_id: "run".into(), after_sequence: Some(2), ..Default::default() }).await.unwrap();
    assert_eq!(events.events.iter().map(|event| event.sequence).collect::<Vec<_>>(), [3,4]);
}

#[tokio::test]
async fn changed_source_cannot_reuse_a_committed_import_operation() {
    let (ledger, _) = ledgers(); let root = tempfile::tempdir().unwrap();
    let source = complete_source(&ledger, &root);
    ledger.import_agent_family(&source, "import").await.unwrap();
    let db = rusqlite::Connection::open(source.path()).unwrap();
    db.execute("UPDATE agent_runs SET summary = 'changed'", []).unwrap(); drop(db);
    assert!(matches!(ledger.import_agent_family(&source, "import").await.unwrap_err(), super::super::super::LedgerError::Driver(ref error) if error.kind() == ErrorKind::Conflict));
    assert_eq!(ledger.get_agent_run("run").await.unwrap().unwrap().summary.as_deref(), Some("original"));
}
