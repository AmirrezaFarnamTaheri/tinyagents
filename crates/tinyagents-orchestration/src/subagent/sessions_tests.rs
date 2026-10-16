use super::*;
use crate::subagent::SubagentRequest;
use std::sync::Arc;
use tinyagents_harness::context::{RunConfig, RunContext};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

fn docs_of(storage: &dyn StorageBackend, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

fn selector(task_key: &str) -> SubagentSessionSelector {
    SubagentSessionSelector {
        parent_session: "parent".into(),
        parent_thread_id: Some("thread".into()),
        agent_id: "researcher".into(),
        model: Some("model-a".into()),
        sandbox_mode: "read_only".into(),
        action_root: None,
        task_key: task_key.into(),
    }
}

fn lifecycle_key() -> SubagentTaskKey {
    SubagentTaskKey {
        root_run_id: "root".into(),
        parent_run_id: "parent-run".into(),
        thread_id: Some("thread".into()),
        task_id: "task-1".into(),
    }
}

fn upsert(force_fresh: bool, task_id: &str) -> SubagentSessionUpsert {
    SubagentSessionUpsert {
        selector: selector("analysis"),
        lifecycle_key: lifecycle_key(),
        display_name: Some("Researcher".into()),
        task_title: "Analyze the repository".into(),
        worker_thread_id: Some("worker-thread".into()),
        task_id: task_id.into(),
        force_fresh,
    }
}

#[tokio::test]
async fn session_selector_requires_matching_scope_and_reusable_status() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let selector = selector("analysis");
    let (session, _) = store.upsert_running(upsert(false, "task-1")).await.unwrap();
    assert!(session.matches_selector(&selector));

    let mut other_task = selector.clone();
    other_task.task_key = "different".into();
    assert!(!session.matches_selector(&other_task));
}

#[tokio::test]
async fn driver_store_reuses_matching_session_and_force_fresh_creates_another() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let (first, first_decision) = store.upsert_running(upsert(false, "task-1")).await.unwrap();
    assert_eq!(first_decision, ReuseDecision::SpawnedNew);

    let (reused, decision) = store.upsert_running(upsert(false, "task-2")).await.unwrap();
    assert_eq!(decision, ReuseDecision::ReusedRunning);
    assert_eq!(reused.subagent_session_id, first.subagent_session_id);
    assert_eq!(reused.current_task_id.as_deref(), Some("task-2"));

    let (fresh, decision) = store.upsert_running(upsert(true, "task-3")).await.unwrap();
    assert_eq!(decision, ReuseDecision::ForcedFresh);
    assert_ne!(fresh.subagent_session_id, first.subagent_session_id);
}

#[tokio::test]
async fn closing_a_session_removes_it_from_reuse_and_preserves_it_in_listing() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let (session, _) = store.upsert_running(upsert(false, "task-1")).await.unwrap();
    assert!(store.close(&session.subagent_session_id).await.unwrap());
    assert!(!store.close("missing").await.unwrap());

    let closed = store
        .get(&session.subagent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed.status, DurableSubagentStatus::Closed);
    assert!(!closed.reusable);
    assert!(find_reusable_session([closed], &selector("analysis")).is_none());
}

#[tokio::test]
async fn stale_completion_cannot_overwrite_a_reassigned_or_closed_session() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let (session, _) = store.upsert_running(upsert(false, "task-1")).await.unwrap();
    store.upsert_running(upsert(false, "task-2")).await.unwrap();
    assert!(
        !store
            .mark_finished(
                &session.subagent_session_id,
                "task-1",
                DurableSubagentStatus::Idle,
                Vec::new()
            )
            .await
            .unwrap()
    );
    assert!(store.close(&session.subagent_session_id).await.unwrap());
    assert!(
        !store
            .mark_failed(
                &session.subagent_session_id,
                "task-2",
                "late failure".into()
            )
            .await
            .unwrap()
    );
    let current = store
        .get(&session.subagent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, DurableSubagentStatus::Closed);
    assert_eq!(current.current_task_id.as_deref(), Some("task-2"));
}

#[tokio::test]
async fn concurrent_first_upserts_claim_one_durable_session_for_a_selector() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let (left, right) = tokio::join!(
        store.upsert_running(upsert(false, "task-left")),
        store.upsert_running(upsert(false, "task-right")),
    );
    let (left, _) = left.unwrap();
    let (right, _) = right.unwrap();
    assert_eq!(left.subagent_session_id, right.subagent_session_id);
    let sessions = store.list_for_parent("parent", None).await.unwrap();
    assert_eq!(sessions.len(), 1);
}

#[tokio::test]
async fn continuation_by_session_id_keeps_the_original_lifecycle_key() {
    let store = DriverSubagentSessionStore::new(docs_of(&MemoryStorage::new(), "local")).unwrap();
    let (session, _) = store.upsert_running(upsert(false, "task-1")).await.unwrap();
    let fresh = RunContext::new(RunConfig::new("fresh-parent").with_thread("thread"), ());

    let request = SubagentRequest::continue_with_session_id(
        &store,
        &session.subagent_session_id,
        fresh,
        (),
        "continue work",
        None,
    )
    .await
    .unwrap();
    let parts = request.into_parts();

    assert_eq!(parts.task_key, lifecycle_key());
    assert_eq!(parts.input, "continue work");
}

#[test]
fn task_helpers_bound_length_and_normalize_display_text() {
    assert_eq!(normalize_task_key("  Fix  the BUG! "), "fix-the-bug");
    assert_eq!(normalize_task_key("  "), "untitled-task");
    assert!(normalize_task_key(&"a".repeat(160)).len() <= 96);
    assert_eq!(task_title_from_prompt("  one\n two "), "one two");
    assert_eq!(reuse_decision(None, true), ReuseDecision::ForcedFresh);
}
