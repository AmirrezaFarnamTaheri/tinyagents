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
fn qualified_live_owner_can_resolve_a_raw_parent_durable_session() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "task-raw-parent", "parent-a", "raw-parent-worker");

    // The live key may be tenant-qualified while durable parentSession remains
    // the raw product parent. This calls the existing one-owner API to pin the
    // behavior gap before exercising the additive split-owner API.
    let owners = DetachedSubagentOwners::new("tenant-a::parent-a", "parent-a");
    let resolved =
        resolve_detached_or_durable_with_owners(&registry(), &store, "raw-parent-worker", owners)
            .unwrap();
    assert_eq!(resolved.task_id, "task-raw-parent");
}

#[tokio::test]
async fn qualified_foreign_live_owner_is_denied_even_when_raw_parent_matches() {
    let store = InMemoryTaskStore::new();
    spawn(
        &store,
        "shared-raw-parent-task",
        "parent-a",
        "shared-session",
    );
    let registry = registry();
    let (_status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            TaskId::new("shared-raw-parent-task"),
            "tenant-a::parent-a",
            Metadata {
                agent: "researcher".into(),
                session: Some("shared-session".into()),
            },
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();

    assert_eq!(
        resolve_detached_or_durable_with_owners(
            &registry,
            &store,
            "shared-raw-parent-task",
            DetachedSubagentOwners::new("tenant-b::parent-a", "parent-a"),
        )
        .unwrap_err(),
        WaitError::NotOwned
    );
    task.abort();
}

#[tokio::test]
async fn wait_after_restart_uses_raw_durable_parent_with_qualified_live_owner() {
    let store = InMemoryTaskStore::new();
    spawn(
        &store,
        "raw-parent-finished",
        "parent-a",
        "raw-parent-finished-session",
    );
    record_status(
        &store,
        "raw-parent-finished",
        &DetachedSubagentStatus::Completed {
            output: "done".into(),
            iterations: 1,
        },
    )
    .unwrap();
    let completion_store = InMemoryCompletionStore::new();
    let completions = CompletionRouter::new(Arc::new(completion_store.clone()));

    let outcome = wait_detached_or_durable_with_owners(
        &registry(),
        &store,
        &completions,
        "raw-parent-finished",
        DetachedSubagentOwners::new("tenant-a::parent-a", "parent-a"),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        WaitOutcome::Terminal(DetachedSubagentStatus::Completed { output, .. }) if output == "done"
    ));
    assert_eq!(
        completion_store.get("raw-parent-finished").unwrap().state,
        tinyagents_tasks::CompletionState::Tombstoned
    );
}

#[test]
fn cancel_after_restart_uses_raw_parent_and_returns_raw_parent_metadata() {
    let store = InMemoryTaskStore::new();
    spawn(
        &store,
        "raw-parent-cancel",
        "parent-a",
        "raw-parent-cancel-session",
    );
    let completion_store = InMemoryCompletionStore::new();
    let completions = CompletionRouter::new(Arc::new(completion_store.clone()));

    let outcome = cancel_detached_or_durable_with_owners(
        &registry(),
        &store,
        &completions,
        "raw-parent-cancel",
        DetachedSubagentOwners::new("tenant-a::parent-a", "parent-a"),
    )
    .unwrap();
    assert!(matches!(
        outcome,
        CancelDetachedOutcome::Cancelled { metadata }
            if metadata.parent_session == "parent-a"
                && metadata.task_id == "raw-parent-cancel"
    ));
    assert_eq!(
        store.get(&TaskId::new("raw-parent-cancel")).unwrap().status,
        OrchestrationTaskStatus::Cancelled
    );
    assert_eq!(
        completion_store.get("raw-parent-cancel").unwrap().state,
        tinyagents_tasks::CompletionState::Tombstoned
    );
}

#[tokio::test]
async fn wait_all_threads_split_owners_through_deadline_recovery() {
    let store = InMemoryTaskStore::new();
    spawn(&store, "raw-parent-done-a", "parent-a", "session-a");
    spawn(&store, "raw-parent-done-b", "parent-a", "session-b");
    for task_id in ["raw-parent-done-a", "raw-parent-done-b"] {
        record_status(
            &store,
            task_id,
            &DetachedSubagentStatus::Completed {
                output: task_id.into(),
                iterations: 1,
            },
        )
        .unwrap();
    }
    let completions = CompletionRouter::new(Arc::new(InMemoryCompletionStore::new()));
    let task_ids = vec!["raw-parent-done-a".into(), "raw-parent-done-b".into()];

    let results = wait_all_detached_with_owners(
        &registry(),
        &store,
        &completions,
        &task_ids,
        DetachedSubagentOwners::new("tenant-a::parent-a", "parent-a"),
        Duration::ZERO,
    )
    .await;
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|item| matches!(
        &item.result,
        Ok(WaitOutcome::Terminal(
            DetachedSubagentStatus::Completed { .. }
        ))
    )));
}

#[tokio::test]
async fn live_foreign_task_id_is_not_hidden_by_empty_profile_store() {
    let registry = registry();
    let store = InMemoryTaskStore::new();
    let (_status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            TaskId::new("foreign-live-task"),
            "parent-a",
            Metadata {
                agent: "researcher".into(),
                session: Some("foreign-worker-session".into()),
            },
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();

    assert_eq!(
        resolve_detached_or_durable(&registry, &store, "foreign-live-task", "parent-b")
            .unwrap_err(),
        WaitError::NotOwned
    );
    task.abort();
}

#[tokio::test]
async fn live_foreign_session_id_is_not_hidden_by_empty_profile_store() {
    let registry = registry();
    let store = InMemoryTaskStore::new();
    let (_status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            TaskId::new("foreign-session-task"),
            "parent-a",
            Metadata {
                agent: "researcher".into(),
                session: Some("foreign-session-id".into()),
            },
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();

    assert_eq!(
        resolve_detached_or_durable(&registry, &store, "foreign-session-id", "parent-b")
            .unwrap_err(),
        WaitError::NotOwned
    );
    task.abort();
}

#[tokio::test]
async fn owned_live_task_and_session_resolution_precede_durable_records() {
    let store = InMemoryTaskStore::new();
    spawn(
        &store,
        "durable-shadow",
        "parent-a",
        "shared-worker-session",
    );
    record_status(
        &store,
        "durable-shadow",
        &DetachedSubagentStatus::Completed {
            output: "old durable result".into(),
            iterations: 1,
        },
    )
    .unwrap();

    let registry = registry();
    let (_status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            TaskId::new("live-task"),
            "parent-a",
            Metadata {
                agent: "live-researcher".into(),
                session: Some("shared-worker-session".into()),
            },
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();

    let by_task = resolve_detached_or_durable(&registry, &store, "live-task", "parent-a").unwrap();
    let by_session =
        resolve_detached_or_durable(&registry, &store, "shared-worker-session", "parent-a")
            .unwrap();
    assert_eq!(by_task.task_id, "live-task");
    assert_eq!(by_task.agent_id, "live-researcher");
    assert_eq!(by_session.task_id, "live-task");
    assert_eq!(by_session.agent_id, "live-researcher");
    task.abort();
}

#[tokio::test]
async fn exact_live_task_id_precedes_a_colliding_live_session_id() {
    let registry = registry();
    let store = InMemoryTaskStore::new();
    let mut tasks = Vec::new();
    for (task_id, session_id, agent_id) in [
        ("collision", "first-session", "first-agent"),
        ("other-task", "collision", "second-agent"),
    ] {
        let (_status_tx, status_rx) = tokio::sync::watch::channel(DetachedSubagentStatus::Running);
        let task = tokio::spawn(std::future::pending::<()>());
        registry
            .register(
                TaskId::new(task_id),
                "parent-a",
                Metadata {
                    agent: agent_id.into(),
                    session: Some(session_id.into()),
                },
                status_rx,
                CancellationToken::new(),
                task.abort_handle(),
            )
            .unwrap();
        tasks.push(task);
    }

    let resolved = resolve_detached_or_durable(&registry, &store, "collision", "parent-a").unwrap();
    assert_eq!(resolved.task_id, "collision");
    assert_eq!(resolved.agent_id, "first-agent");
    for task in tasks {
        task.abort();
    }
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
