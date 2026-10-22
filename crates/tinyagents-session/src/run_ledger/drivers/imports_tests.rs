use super::*;
use crate::run_ledger::import::{AgentFamilyImport, AgentFamilyImportReport, LegacyLedgerRows};
use crate::run_ledger::port::RunLedgerStore;
use crate::run_ledger::types::{
    AgentRun, AgentRunKind, AgentRunStatus, RunEvent, RunEventAppend, RunTelemetry,
};
use crate::testkit::MemoryStorage;
use chrono::{DateTime, Utc};
use serde_json::json;

fn ledger() -> DriverRunLedger {
    let backend = MemoryStorage::new();
    let scope = Scope::new("legacy-import").unwrap();
    DriverRunLedger::new(backend.for_scope(&scope).unwrap())
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
async fn agent_family_import_keeps_telemetry_separate_and_event_head_atomic() {
    let ledger = ledger();
    let event = RunEvent {
        run_id: "run-legacy".into(),
        sequence: 7,
        event_type: "started".into(),
        payload: json!({"legacy":true}),
        timestamp: time("2024-02-03T04:05:06Z"),
    };
    let run = AgentRun {
        id: "run-legacy".into(),
        kind: AgentRunKind::Subagent,
        parent_run_id: None,
        parent_thread_id: None,
        agent_id: Some("agent".into()),
        status: AgentRunStatus::Completed,
        prompt_ref: None,
        worker_thread_id: None,
        checkpoint_path: None,
        checkpoint: None,
        summary: Some("done".into()),
        error: None,
        metadata: json!({}),
        telemetry: Some(RunTelemetry {
            run_id: "run-legacy".into(),
            input_tokens: 1,
            ..Default::default()
        }),
        started_at: time("2024-02-03T04:05:06Z"),
        updated_at: time("2024-02-03T04:06:07Z"),
        completed_at: Some(time("2024-02-03T04:07:08Z")),
    };
    let telemetry = RunTelemetry {
        run_id: "run-legacy".into(),
        input_tokens: 17,
        output_tokens: 23,
        ..Default::default()
    };
    let import = AgentFamilyImport {
        operation_id: "run-family-with-events".into(),
        source_fingerprint: "sha256:events".into(),
        runs: vec![run],
        events: vec![event],
        telemetry: vec![telemetry.clone()],
    };
    let report = ledger.import_agent_family(import.clone()).await.unwrap();
    assert_eq!(report.events.imported, 1);
    let stored_run = ledger
        .scoped
        .documents()
        .get(AGENTS, &super::storage::key(&["run-legacy"]).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(stored_run.doc.get("telemetry").is_none());
    assert_eq!(
        ledger
            .get_agent_run("run-legacy")
            .await
            .unwrap()
            .unwrap()
            .telemetry,
        Some(telemetry)
    );
    assert_eq!(ledger.import_agent_family(import).await.unwrap(), report);

    let appended = ledger
        .append_run_event(RunEventAppend {
            run_id: "run-legacy".into(),
            event_type: "resumed".into(),
            payload: json!({}),
        })
        .await
        .unwrap();
    assert_eq!(appended.sequence, 8);
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
