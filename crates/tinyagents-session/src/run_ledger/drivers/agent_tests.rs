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
