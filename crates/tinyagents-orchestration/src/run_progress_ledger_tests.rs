use super::*;
use crate::test_support::ledger_in;
use std::sync::Arc;
use tempfile::tempdir;
use tinyagents_harness::progress::AgentProgress;
use tinyagents_session::run_ledger::{RunEventListRequest, RunLedgerStore};

fn recorder(store: Arc<dyn RunLedgerStore>) -> RunProgressLedger {
    RunProgressLedger::new(
        Some(store),
        RunProgressIdentity {
            client_id: "client-a".into(),
            thread_id: "thread-a".into(),
            request_id: "request-a".into(),
            source: "test".into(),
            agent_id: "orchestrator".into(),
        },
    )
}

#[tokio::test]
async fn observes_agent_progress_once_and_retains_child_correlation_for_the_host() {
    let dir = tempdir().expect("temporary storage directory");
    let store: Arc<dyn RunLedgerStore> = Arc::new(ledger_in(dir.path()));
    let mut recorder = recorder(Arc::clone(&store));

    recorder.observe(&AgentProgress::TurnStarted).await;
    recorder
        .observe(&AgentProgress::ToolCallStarted {
            call_id: "parent-call".into(),
            tool_name: "delegate".into(),
            arguments: serde_json::json!({}),
            iteration: 1,
            display_label: None,
            display_detail: None,
        })
        .await;
    recorder
        .observe(&AgentProgress::TurnCostUpdated {
            model: "model-a".into(),
            iteration: 1,
            input_tokens: 18,
            output_tokens: 4,
            cached_input_tokens: 7,
            total_usd: 0.002,
        })
        .await;
    recorder
        .observe(&AgentProgress::SubagentSpawned {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            mode: "typed".into(),
            dedicated_thread: false,
            prompt_chars: 12,
            worker_thread_id: None,
            display_name: Some("Child".into()),
            prompt: "do work".into(),
            parent_call_id: Some("parent-call".into()),
        })
        .await;
    recorder
        .observe(&AgentProgress::SubagentToolCallStarted {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            call_id: "child-call".into(),
            tool_name: "read".into(),
            arguments: serde_json::json!({}),
            iteration: 1,
            display_label: None,
            display_detail: None,
        })
        .await;
    let context = recorder
        .observe(&AgentProgress::SubagentAwaitingUser {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            question: "Approve?".into(),
            worker_thread_id: None,
            checkpoint_path: None,
        })
        .await;
    assert_eq!(context.parent_call_id.as_deref(), Some("parent-call"));

    recorder
        .observe(&AgentProgress::SubagentFailed {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            error: "denied".into(),
        })
        .await;
    recorder
        .observe(&AgentProgress::TurnCompleted {
            iterations: 1,
            stop: None,
        })
        .await;

    let parent = store
        .get_agent_run("request-a")
        .await
        .expect("read parent")
        .expect("parent exists");
    let telemetry = parent.telemetry.expect("parent telemetry");
    assert_eq!(telemetry.tool_count, 1);
    assert_eq!(telemetry.input_tokens, 18);
    assert_eq!(telemetry.model.as_deref(), Some("model-a"));
    assert_eq!(parent.status, AgentRunStatus::Completed);
    let child = store
        .get_agent_run("task-child")
        .await
        .expect("read child")
        .expect("child exists");
    assert_eq!(child.status, AgentRunStatus::Failed);
    assert_eq!(child.error.as_deref(), Some("denied"));
    assert_eq!(child.telemetry.expect("child telemetry").tool_count, 1);
    assert_eq!(
        events_for(store.as_ref(), "request-a").await,
        ["turn_started", "tool_call_started", "turn_completed"]
    );
    assert_eq!(
        events_for(store.as_ref(), "task-child").await,
        [
            "subagent_spawned",
            "subagent_tool_call_started",
            "subagent_awaiting_user",
            "subagent_failed"
        ]
    );
    assert_eq!(recorder.events_seen(), 8);
    recorder.finish().await;
    assert!(
        !events_for(store.as_ref(), "request-a")
            .await
            .contains(&"turn_interrupted".to_owned())
    );
}

#[tokio::test]
async fn observes_child_correlation_without_a_store() {
    let mut recorder = RunProgressLedger::new(
        None,
        RunProgressIdentity {
            client_id: "client-a".into(),
            thread_id: "thread-a".into(),
            request_id: "request-a".into(),
            source: "test".into(),
            agent_id: "orchestrator".into(),
        },
    );
    recorder
        .observe(&AgentProgress::SubagentSpawned {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            mode: "typed".into(),
            dedicated_thread: false,
            prompt_chars: 12,
            worker_thread_id: None,
            display_name: None,
            prompt: "do work".into(),
            parent_call_id: Some("parent-call".into()),
        })
        .await;
    let context = recorder
        .observe(&AgentProgress::SubagentAwaitingUser {
            agent_id: "agent-child".into(),
            task_id: "task-child".into(),
            question: "Approve?".into(),
            worker_thread_id: None,
            checkpoint_path: None,
        })
        .await;
    assert_eq!(context.parent_call_id.as_deref(), Some("parent-call"));
    assert_eq!(recorder.events_seen(), 2);
    recorder.finish().await;
    assert_eq!(recorder.events_seen(), 2);
}

#[tokio::test]
async fn finish_records_interruption_when_parent_never_completes() {
    let dir = tempdir().expect("temporary storage directory");
    let store: Arc<dyn RunLedgerStore> = Arc::new(ledger_in(dir.path()));
    let mut recorder = recorder(Arc::clone(&store));
    recorder.observe(&AgentProgress::TurnStarted).await;
    recorder.finish().await;

    let parent = store
        .get_agent_run("request-a")
        .await
        .expect("read parent")
        .expect("interrupted run is written");
    assert_eq!(parent.status, AgentRunStatus::Interrupted);
    assert_eq!(
        events_for(store.as_ref(), "request-a").await,
        ["turn_started", "turn_interrupted"]
    );
}

async fn events_for(store: &dyn RunLedgerStore, run_id: &str) -> Vec<String> {
    store
        .list_recent_run_events(&RunEventListRequest {
            run_id: run_id.into(),
            ..Default::default()
        })
        .await
        .expect("read run events")
        .events
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

#[tokio::test]
async fn records_parent_lifecycle_tool_events_and_latest_telemetry() {
    let dir = tempdir().expect("temporary storage directory");
    let store: Arc<dyn RunLedgerStore> = Arc::new(ledger_in(dir.path()));
    let recorder = recorder(Arc::clone(&store));

    recorder.turn_started().await;
    recorder.tool_started("call-a", "search", 2, 1).await;
    recorder.cost("model-a", 31, 7, 13, 0.0042).await;
    recorder
        .tool_completed(serde_json::json!({"callId": "call-a", "success": true}))
        .await;
    let telemetry = recorder.read_telemetry().await.expect("run telemetry");
    assert_eq!(telemetry.run_id, "request-a");
    assert_eq!(telemetry.input_tokens, 31);
    assert_eq!(telemetry.output_tokens, 7);
    assert_eq!(telemetry.cached_input_tokens, 13);
    assert_eq!(telemetry.tool_count, 1);
    assert_eq!(telemetry.model.as_deref(), Some("model-a"));
    assert!((telemetry.cost_usd - 0.0042).abs() < f64::EPSILON);

    recorder.turn_completed(3).await;
    let parent = store
        .get_agent_run("request-a")
        .await
        .expect("read parent run")
        .expect("parent run was written");
    assert_eq!(parent.parent_thread_id.as_deref(), Some("thread-a"));
    assert_eq!(parent.status, AgentRunStatus::Completed);
    assert_eq!(
        events_for(store.as_ref(), "request-a").await,
        [
            "turn_started",
            "tool_call_started",
            "tool_call_completed",
            "turn_completed"
        ]
    );
}

#[tokio::test]
async fn records_child_completion_failure_and_checkpoint_availability() {
    let dir = tempdir().expect("temporary storage directory");
    let store: Arc<dyn RunLedgerStore> = Arc::new(ledger_in(dir.path()));
    let recorder = recorder(Arc::clone(&store));

    recorder
        .child_spawned(
            "agent-done",
            "task-done",
            "parallel",
            false,
            12,
            &None,
            &None,
            &Some("call-done".into()),
        )
        .await;
    recorder
        .child_tool_started("agent-done", "task-done", "child-call", "read", 1, 1)
        .await;
    recorder
        .child_tool_completed(
            "task-done",
            serde_json::json!({"agentId": "agent-done", "callId": "child-call", "success": true}),
        )
        .await;
    let worktree_path = Some("/work/agent-done".to_owned());
    let changed_files = vec!["src/lib.rs".to_owned()];
    let completed_parent_call = Some("call-done".to_owned());
    let done = ChildDone {
        agent_id: "agent-done",
        task_id: "task-done",
        elapsed_ms: 120,
        iterations: 2,
        output_chars: 42,
        worktree_path: &worktree_path,
        changed_files: &changed_files,
        dirty_status: Some(true),
        parent_call_id: &completed_parent_call,
        tool_count: Some(4),
    };
    recorder.child_completed(&done).await;

    recorder
        .child_spawned(
            "agent-failed",
            "task-failed",
            "sequential",
            true,
            8,
            &None,
            &None,
            &None,
        )
        .await;
    recorder
        .child_failed("agent-failed", "task-failed", "tool failed", &None, Some(2))
        .await;

    recorder
        .child_awaiting_user(
            "agent-paused",
            "task-paused-with-checkpoint",
            "Need confirmation",
            &Some("worker-thread".into()),
            &Some("checkpoint.json".into()),
            &Some("parent-call".into()),
        )
        .await;
    recorder
        .child_awaiting_user(
            "agent-paused",
            "task-paused-without-checkpoint",
            "Need confirmation",
            &None,
            &None,
            &None,
        )
        .await;

    let completed = store
        .get_agent_run("task-done")
        .await
        .expect("read completed child")
        .expect("completed child row");
    assert_eq!(completed.parent_run_id.as_deref(), Some("request-a"));
    assert_eq!(completed.parent_thread_id.as_deref(), Some("thread-a"));
    assert_eq!(completed.status, AgentRunStatus::Completed);
    assert_eq!(
        completed.summary.as_deref(),
        Some("Completed in 2 iteration(s), 42 output chars")
    );
    let completed_telemetry = completed.telemetry.expect("child telemetry");
    assert_eq!(completed_telemetry.elapsed_ms, Some(120));
    assert_eq!(completed_telemetry.tool_count, 4);

    let failed = store
        .get_agent_run("task-failed")
        .await
        .expect("read failed child")
        .expect("failed child row");
    assert_eq!(failed.status, AgentRunStatus::Failed);
    assert_eq!(failed.error.as_deref(), Some("tool failed"));
    assert_eq!(failed.telemetry.expect("failed telemetry").tool_count, 2);

    let with_checkpoint = store
        .get_agent_run("task-paused-with-checkpoint")
        .await
        .expect("read checkpointed child")
        .expect("checkpointed child row");
    assert_eq!(
        with_checkpoint.checkpoint_path.as_deref(),
        Some("checkpoint.json")
    );
    assert_eq!(
        with_checkpoint.checkpoint.expect("checkpoint details")["checkpointPersisted"],
        true
    );
    let without_checkpoint = store
        .get_agent_run("task-paused-without-checkpoint")
        .await
        .expect("read child without checkpoint")
        .expect("child row without checkpoint");
    assert_eq!(without_checkpoint.checkpoint_path, None);
    assert_eq!(
        without_checkpoint.checkpoint.expect("checkpoint details")["checkpointPersisted"],
        false
    );

    assert_eq!(
        events_for(store.as_ref(), "task-done").await,
        [
            "subagent_spawned",
            "subagent_tool_call_started",
            "subagent_tool_call_completed",
            "subagent_completed"
        ]
    );
    assert_eq!(
        events_for(store.as_ref(), "task-failed").await,
        ["subagent_spawned", "subagent_failed"]
    );
    assert_eq!(
        events_for(store.as_ref(), "task-paused-with-checkpoint").await,
        ["subagent_awaiting_user"]
    );
}
