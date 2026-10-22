use super::*;
use crate::run_ledger::import::{AgentFamilyImport, LegacyLedgerRows};
use crate::run_ledger::port::RunLedgerStore;
use crate::testkit::MemoryStorage;
use chrono::{DateTime, Utc};
use serde_json::json;
use tinystoragedrivers_api::StorageBackend;

fn ledger() -> DriverRunLedger {
    let backend = MemoryStorage::new();
    let scope = Scope::new("legacy-import").unwrap();
    DriverRunLedger::new(ScopedStorage::new(
        backend.for_scope(&scope).unwrap(),
        scope,
    ))
}

fn time(value: &str) -> chrono::DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

#[tokio::test]
async fn agent_family_import_receipt_replays_and_rejects_changed_source() {
    let ledger = ledger();
    let import = AgentFamilyImport {
        operation_id: "legacy-run-family".into(),
        source_fingerprint: "sha256:source-a".into(),
        ..Default::default()
    };
    let first = ledger.import_agent_family(import.clone()).await.unwrap();
    assert_eq!(first, AgentFamilyImportReport::default());
    assert_eq!(ledger.import_agent_family(import).await.unwrap(), first);

    let changed = AgentFamilyImport {
        operation_id: "legacy-run-family".into(),
        source_fingerprint: "sha256:source-b".into(),
        ..Default::default()
    };
    assert!(ledger.import_agent_family(changed).await.is_err());
}

#[tokio::test]
async fn legacy_ledger_import_preserves_workflow_fields_and_is_only_if_absent() {
    let ledger = ledger();
    let workflow = WorkflowRun {
        id: "workflow-legacy".into(),
        definition_id: "definition".into(),
        parent_thread_id: Some("thread".into()),
        input: json!({"prompt":"old"}),
        phase_states: json!({"phase":"complete"}),
        child_run_ids: vec!["child".into()],
        status: WorkflowRunStatus::Interrupted,
        summary: Some("legacy summary".into()),
        started_at: time("2023-04-05T06:07:08Z"),
        updated_at: time("2023-04-05T06:08:09Z"),
        completed_at: Some(time("2023-04-05T06:09:10Z")),
        revision: 17,
        lease_owner: Some("old-process".into()),
        lease_expires_at: Some(time("2099-04-05T06:10:11Z")),
    };
    let rows = LegacyLedgerRows {
        workflow_runs: vec![workflow.clone()],
        ..Default::default()
    };
    let first = ledger.import_legacy_rows(rows.clone()).await.unwrap();
    assert_eq!(first.workflow_runs.imported, 1);
    assert_eq!(
        ledger.get_workflow_run(&workflow.id).await.unwrap(),
        Some(workflow.clone())
    );
    let replay = ledger.import_legacy_rows(rows).await.unwrap();
    assert_eq!(replay.workflow_runs.already_present, 1);
    assert_eq!(
        ledger.get_workflow_run(&workflow.id).await.unwrap(),
        Some(workflow)
    );
}
