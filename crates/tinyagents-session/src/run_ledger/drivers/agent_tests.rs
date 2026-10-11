use super::*;
use serde_json::json;
use tinystoragedrivers_core::{MemoryStorage, StorageBackend};

fn run(id: &str) -> AgentRunUpsert {
    AgentRunUpsert {
        id: id.into(), kind: AgentRunKind::WorkerThread,
        parent_run_id: Some("parent".into()), parent_thread_id: Some("thread".into()),
        agent_id: Some("agent".into()), status: AgentRunStatus::Running,
        prompt_ref: Some("prompt".into()), worker_thread_id: Some("worker".into()),
        checkpoint_path: None, checkpoint: Some(json!({"step":1})),
        summary: Some("original".into()), error: None, metadata: json!({"keep":true}),
        started_at: Some(Utc::now()), completed_at: None,
    }
}

#[tokio::test]
async fn independent_agent_handles_preserve_merge_and_isolate_profiles() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("alice").unwrap();
    let a = DriverRunLedger::new(backend.for_scope(&scope).unwrap());
    let peer = DriverRunLedger::new(backend.for_scope(&scope).unwrap());
    let other = DriverRunLedger::new(backend.for_scope(&Scope::new("bob").unwrap()).unwrap());
    let first = a.upsert_agent_run(run("run")).await.unwrap();
    let mut edit = run("run");
    edit.kind = AgentRunKind::Subagent; edit.started_at = None;
    edit.summary = None; edit.metadata = json!({});
    let changed = peer.upsert_agent_run(edit).await.unwrap();
    assert_eq!(changed.kind, AgentRunKind::WorkerThread);
    assert_eq!(changed.started_at, first.started_at);
    assert_eq!(changed.summary, first.summary);
    assert_eq!(changed.metadata, first.metadata);
    assert_eq!(a.get_agent_run("run").await.unwrap().unwrap(), changed);
    assert!(other.get_agent_run("run").await.unwrap().is_none());
}

#[tokio::test]
async fn native_status_compare_and_swap_records_only_the_winning_event() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("alice").unwrap();
    let a = DriverRunLedger::new(backend.for_scope(&scope).unwrap());
    a.upsert_agent_run(run("run")).await.unwrap();
    let event = || Some(RunEventAppend { run_id: "run".into(), event_type: "completed".into(), payload: json!({}) });
    let result = a.transition_agent_run_status_from("run", AgentRunStatus::Running, AgentRunStatus::Completed,
        None, Some(Utc::now()), event()).await.unwrap();
    assert!(matches!(result, RunTransition::Applied(_)));
    let stale = a.transition_agent_run_status_from("run", AgentRunStatus::Running, AgentRunStatus::Failed,
        Some("stale"), None, event()).await.unwrap();
    assert!(matches!(stale, RunTransition::StatusChanged(_)));
    let events = a.list_recent_run_events(&RunEventListRequest { run_id: "run".into(), ..Default::default() }).await.unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].sequence, 1);
}

#[tokio::test]
async fn agent_queries_join_telemetry_and_exclusive_sweep_preserves_resumable_runs() {
    let (a, b) = super::tests::ledgers();
    for (id, status) in [("running", AgentRunStatus::Running), ("pending", AgentRunStatus::Pending),
        ("paused", AgentRunStatus::Paused), ("awaiting", AgentRunStatus::AwaitingUser)] {
        let mut input = run(id); input.status = status;
        a.upsert_agent_run(input).await.unwrap();
    }
    a.upsert_run_telemetry(RunTelemetryUpsert { run_id: "running".into(), input_tokens: Some(42), ..Default::default() }).await.unwrap();
    let page = a.list_agent_runs(&AgentRunListRequest { limit: Some(1), ..Default::default() }).await.unwrap();
    assert_eq!(page.count, 4);
    assert_eq!(page.runs.len(), 1);
    assert_eq!(a.get_agent_run("running").await.unwrap().unwrap().telemetry.unwrap().input_tokens, 42);
    assert_eq!(a.interrupt_orphaned_agent_runs().await.unwrap(), 2);
    assert_eq!(a.get_agent_run("running").await.unwrap().unwrap().status, AgentRunStatus::Interrupted);
    assert_eq!(a.get_agent_run("paused").await.unwrap().unwrap().status, AgentRunStatus::Paused);
    assert_eq!(a.get_agent_run("awaiting").await.unwrap().unwrap().status, AgentRunStatus::AwaitingUser);
    assert_eq!(b.list_agent_runs(&Default::default()).await.unwrap().count, 0);
}

#[tokio::test]
async fn independent_event_allocators_replay_without_duplicates_and_keep_cursor_order() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("alice").unwrap();
    let a = DriverRunLedger::new(backend.for_scope(&scope).unwrap());
    let b = DriverRunLedger::new(backend.for_scope(&scope).unwrap());
    let input = |label: &str| RunEventAppend { run_id: "run".into(), event_type: label.into(), payload: json!({}) };
    let (one, two) = tokio::join!(a.append_run_event(input("one")), b.append_run_event(input("two")));
    let mut sequences = [one.unwrap().sequence, two.unwrap().sequence]; sequences.sort();
    assert_eq!(sequences, [1, 2]);
    assert_eq!(a.reconcile_run_events("run").await.unwrap(), 2);
    assert_eq!(b.reconcile_run_events("run").await.unwrap(), 2);
    let entries = backend.for_scope(&scope).unwrap().streams()
        .read_window(&format!("run_events:{}", storage::key(&["run"])), 0, 10).await.unwrap();
    assert_eq!(entries.len(), 2);
    let page = b.list_recent_run_events(&RunEventListRequest { run_id: "run".into(), after_sequence: Some(1), limit: Some(1) }).await.unwrap();
    assert_eq!(page.count, 1); assert_eq!(page.events[0].sequence, 2);
}
