use super::*;
use crate::run_ledger::LedgerError;
use chrono::{Duration, Utc};
use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, MemoryStorage, StorageBackend};

/// Explicit handles share a backend while retaining separate tenant namespaces.
fn ledgers() -> (DriverRunLedger, DriverRunLedger) {
    let backend = MemoryStorage::new();
    (
        DriverRunLedger::new(
            backend
                .for_scope(&Scope::new("profile-a").unwrap())
                .unwrap(),
        ),
        DriverRunLedger::new(
            backend
                .for_scope(&Scope::new("profile-b").unwrap())
                .unwrap(),
        ),
    )
}

/// A known workflow input whose first SQL revision is zero.
fn workflow(id: &str) -> WorkflowRunUpsert {
    WorkflowRunUpsert {
        id: id.into(),
        definition_id: "definition".into(),
        parent_thread_id: Some("thread".into()),
        input: json!({"value":1}),
        phase_states: json!({}),
        child_run_ids: vec![],
        status: WorkflowRunStatus::Running,
        summary: Some("original".into()),
        started_at: Some(Utc::now() - Duration::days(1)),
        completed_at: None,
    }
}

/// Workflow upserts preserve SQL merge/revision rules and isolate the configured namespaces.
#[tokio::test]
async fn scoped_workflow_upserts_preserve_revision_and_partial_fields() {
    let (a, b) = ledgers();
    let first = a.upsert_workflow_run(workflow("workflow")).await.unwrap();
    assert_eq!(first.revision, 0);
    let mut edit = workflow("workflow");
    edit.summary = None;
    edit.parent_thread_id = None;
    edit.started_at = None;
    edit.input = json!({"value":2});
    let changed = a.upsert_workflow_run(edit).await.unwrap();
    assert_eq!(changed.revision, 1);
    assert_eq!(changed.started_at, first.started_at);
    assert_eq!(changed.summary, first.summary);
    assert_eq!(changed.parent_thread_id, first.parent_thread_id);
    assert_eq!(
        a.get_workflow_run("workflow").await.unwrap().unwrap(),
        changed
    );
    assert!(b.get_workflow_run("workflow").await.unwrap().is_none());
}

/// Counts represent all matches and blank filters/offset/zero limit follow classic SQL semantics.
#[tokio::test]
async fn scoped_workflow_queries_preserve_filter_and_page_semantics() {
    let (a, b) = ledgers();
    for id in ["one", "two", "three"] {
        a.upsert_workflow_run(workflow(id)).await.unwrap();
    }
    let request = WorkflowRunListRequest {
        status: Some("   ".into()),
        limit: Some(1),
        offset: Some(1),
        ..Default::default()
    };
    let page = a.list_workflow_runs(&request).await.unwrap();
    assert_eq!(page.count, 3);
    assert_eq!(page.runs.len(), 1);
    assert_eq!(b.list_workflow_runs(&request).await.unwrap().count, 0);
    let padded = WorkflowRunListRequest {
        status: Some(" running ".into()),
        ..Default::default()
    };
    assert_eq!(a.list_workflow_runs(&padded).await.unwrap().count, 0);
    let empty = WorkflowRunListRequest {
        limit: Some(0),
        ..Default::default()
    };
    let page = a.list_workflow_runs(&empty).await.unwrap();
    assert_eq!(page.count, 3);
    assert!(page.runs.is_empty());
    let invalid = WorkflowRunListRequest {
        offset: Some(u64::MAX),
        ..Default::default()
    };
    assert!(
        matches!(a.list_workflow_runs(&invalid).await.unwrap_err(), LedgerError::Driver(ref native) if native.kind() == ErrorKind::InvalidInput)
    );
}

/// Repeated telemetry deltas preserve counters and do not cross a scoped handle.
#[tokio::test]
async fn scoped_telemetry_upserts_merge_and_isolate_records() {
    let (a, b) = ledgers();
    let first = a
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: "run".into(),
            input_tokens: Some(42),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first.output_tokens, 0);
    let changed = a
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: "run".into(),
            model: Some("model".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(changed.input_tokens, 42);
    assert_eq!(changed.model.as_deref(), Some("model"));
    let separate = b
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: "run".into(),
            error: Some("failure".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(separate.input_tokens, 0);
    assert!(separate.model.is_none());
}
