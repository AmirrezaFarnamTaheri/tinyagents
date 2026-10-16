use super::*;
use super::{SpawnedSubagent, record_spawned, record_status};
use std::sync::Arc;
use std::time::Duration;

use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::ids::TaskId;
use tinyagents_tasks::{
    CompletionRecord, CompletionResult, CompletionRouter, CompletionStatus, CompletionStore,
    DetachedTaskRegistry, InMemoryCompletionStore, InMemoryTaskStore, OrchestrationTaskStatus,
    SteeringRegistry, TaskStore,
};

#[derive(Clone)]
struct Metadata {
    agent: String,
    session: Option<String>,
}

impl SubagentIdentity for Metadata {
    fn agent_id(&self) -> &str {
        &self.agent
    }
    fn subagent_session_id(&self) -> Option<&str> {
        self.session.as_deref()
    }
}

fn registry() -> DetachedTaskRegistry<Metadata, DetachedSubagentStatus> {
    DetachedTaskRegistry::new(
        SteeringRegistry::new(),
        16,
        DetachedSubagentStatus::is_terminal,
    )
}

fn spawn(store: &InMemoryTaskStore, task_id: &str, parent: &str, session: &str) {
    record_spawned(
        store,
        &SpawnedSubagent {
            task_id,
            agent_id: "researcher",
            parent_session: parent,
            session_parent_prefix: None,
            subagent_session_id: Some(session),
            workspace_dir: "/workspace",
            parent_thread_id: None,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn wait_falls_back_to_owned_durable_terminal_record_after_restart() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-finished", "parent-a", "worker-session");
    record_status(
        &store,
        "task-finished",
        &DetachedSubagentStatus::Completed {
            output: "done".into(),
            iterations: 3,
        },
    )
    .unwrap();
    let completion_store = InMemoryCompletionStore::new();
    let completions = CompletionRouter::new(Arc::new(completion_store.clone()));

    let outcome = wait_detached_or_durable(
        &registry(),
        &store,
        &completions,
        "task-finished",
        "parent-a",
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, WaitOutcome::Terminal(DetachedSubagentStatus::Completed { output, iterations: 0 }) if output == "done")
    );
    assert_eq!(
        completion_store.get("task-finished").unwrap().state,
        tinyagents_tasks::CompletionState::Tombstoned
    );
    drop(completions);
    let after_restart = CompletionRouter::new(Arc::new(completion_store));
    assert_eq!(
        after_restart
            .record(CompletionRecord::new(
                "task-finished",
                "parent-a",
                "researcher",
                CompletionStatus::Success,
                CompletionResult::text("late")
            ))
            .await
            .unwrap(),
        tinyagents_tasks::RecordOutcome::Suppressed
    );
    assert_eq!(
        wait_detached_or_durable(
            &registry(),
            &store,
            &after_restart,
            "task-finished",
            "other",
            Duration::ZERO
        )
        .await
        .unwrap_err(),
        WaitError::NotOwned
    );
}

#[tokio::test]
async fn awaiting_user_wait_keeps_completion_slot_for_later_resume() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-paused", "parent-a", "worker-session");
    record_status(
        &store,
        "task-paused",
        &DetachedSubagentStatus::AwaitingUser {
            question: "Which option?".into(),
        },
    )
    .unwrap();
    let completion_store = InMemoryCompletionStore::new();
    let completions = CompletionRouter::new(Arc::new(completion_store.clone()));

    let paused = wait_detached_or_durable(
        &registry(),
        &store,
        &completions,
        "task-paused",
        "parent-a",
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(matches!(
        paused,
        WaitOutcome::Terminal(DetachedSubagentStatus::AwaitingUser { .. })
    ));
    assert!(completion_store.get("task-paused").is_none());

    record_status(
        &store,
        "task-paused",
        &DetachedSubagentStatus::Completed {
            output: "resumed".into(),
            iterations: 2,
        },
    )
    .unwrap();
    let finished = wait_detached_or_durable(
        &registry(),
        &store,
        &completions,
        "task-paused",
        "parent-a",
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(
        matches!(finished, WaitOutcome::Terminal(DetachedSubagentStatus::Completed { output, .. }) if output == "resumed")
    );
    assert_eq!(
        completion_store.get("task-paused").unwrap().state,
        tinyagents_tasks::CompletionState::Tombstoned
    );
}

#[test]
fn durable_session_id_resolves_to_task_metadata_after_restart() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-session", "parent-a", "worker-session");
    let resolved =
        resolve_detached_or_durable(&registry(), &store, "worker-session", "parent-a").unwrap();
    assert_eq!(resolved.task_id, "task-session");
    assert_eq!(resolved.agent_id, "researcher");
    assert_eq!(
        resolved.subagent_session_id.as_deref(),
        Some("worker-session")
    );
    assert_eq!(
        resolve_detached_or_durable(&registry(), &store, "worker-session", "other").unwrap_err(),
        WaitError::NotOwned
    );
}

#[test]
fn durable_cancel_settles_running_task_and_keeps_cancelled_metadata() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-cancel", "parent-a", "worker-session");
    let completion_store = InMemoryCompletionStore::new();
    let completions = CompletionRouter::new(Arc::new(completion_store.clone()));
    let outcome =
        cancel_detached_or_durable(&registry(), &store, &completions, "task-cancel", "parent-a")
            .unwrap();
    assert!(
        matches!(outcome, CancelDetachedOutcome::Cancelled { metadata } if metadata.task_id == "task-cancel" && metadata.parent_session == "parent-a" && metadata.agent_id == "researcher" && metadata.subagent_session_id.as_deref() == Some("worker-session"))
    );
    assert_eq!(
        store.get(&TaskId::new("task-cancel")).unwrap().status,
        OrchestrationTaskStatus::Cancelled
    );
    assert_eq!(
        completion_store.get("task-cancel").unwrap().state,
        tinyagents_tasks::CompletionState::Tombstoned
    );
    assert!(matches!(
        cancel_detached_or_durable(&registry(), &store, &completions, "task-cancel", "parent-a")
            .unwrap(),
        CancelDetachedOutcome::AlreadyFinished { .. }
    ));
}

#[tokio::test]
async fn wait_all_uses_shared_deadline_and_still_returns_durable_children() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-live", "parent-a", "live-session");
    spawn(&store, "task-done", "parent-a", "done-session");
    record_status(
        &store,
        "task-done",
        &DetachedSubagentStatus::Completed {
            output: "done".into(),
            iterations: 1,
        },
    )
    .unwrap();
    let completions = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));

    let registry = registry();
    let (status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            TaskId::new("task-live"),
            "parent-a",
            Metadata {
                agent: "researcher".into(),
                session: Some("live-session".into()),
            },
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();
    let started = tokio::time::Instant::now();
    let results = wait_all_detached(
        &registry,
        &store,
        &completions,
        &["task-live".into(), "task-done".into()],
        "parent-a",
        Duration::from_millis(20),
    )
    .await;
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(results.len(), 2);
    assert!(matches!(
        results[0].result,
        Ok(WaitOutcome::TimedOut(DetachedSubagentStatus::Running))
    ));
    assert!(
        matches!(&results[1].result, Ok(WaitOutcome::Terminal(DetachedSubagentStatus::Completed { output, .. })) if output == "done")
    );
    drop(status_tx);
    task.abort();
}
