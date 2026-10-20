use super::*;
use crate::test_support::ledger_in;
use std::sync::Arc;
use tempfile::tempdir;
use tinyagents_session::run_ledger::{RunEventListRequest, RunLedgerStore};

fn recorder(store: Arc<dyn RunLedgerStore>) -> RunProgressLedger {
    RunProgressLedger::new(
        store,
        RunProgressIdentity {
            client_id: "client-a".into(),
            thread_id: "thread-a".into(),
            request_id: "request-a".into(),
            source: "test".into(),
            agent_id: "orchestrator".into(),
        },
    )
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
