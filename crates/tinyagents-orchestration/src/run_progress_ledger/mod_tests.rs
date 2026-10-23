use super::*;
use std::sync::Arc;
use tinyagents_harness::progress::{AgentProgress, SubagentUsage};
use tinyagents_session::run_ledger::{
    AgentRunKind, AgentRunStatus, DriverRunLedger, RunEventListRequest, RunLedgerStore,
};
use tinyagents_storage_kit::{ErrorKind, Scope, ScopedStorage, Storage, StorageBackend};
use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};

fn identity() -> RunProgressIdentity {
    RunProgressIdentity {
        client_id: "client-1".into(),
        thread_id: "thread-1".into(),
        request_id: "request-1".into(),
        source: "web_channel".into(),
        agent_id: "orchestrator".into(),
    }
}

fn make_store(backend: &MemoryBackend) -> Arc<DriverRunLedger> {
    Arc::new(DriverRunLedger::new(
        backend
            .for_scope(&Scope::new("local").expect("scope"))
            .expect("scoped storage"),
    ))
}

#[tokio::test]
async fn records_progress_with_stable_run_identity_and_terminal_status() {
    let backend = MemoryBackend::new();
    let store = make_store(&backend);
    let ledger = RunProgressLedger::new(Some(store.clone()), identity());

    ledger.observe(&AgentProgress::TurnStarted).await;
    ledger
        .observe(&AgentProgress::ToolCallStarted {
            call_id: "call-1".into(),
            tool_name: "read_file".into(),
            arguments: serde_json::json!({"path": "notes.md"}),
            iteration: 1,
            display_label: Some("Reading file".into()),
            display_detail: None,
        })
        .await;

    assert_eq!(ledger.events_seen(), 2);
    let run = store
        .get_agent_run("request-1")
        .await
        .expect("read run")
        .expect("run created");
    assert_eq!(run.kind, AgentRunKind::InteractiveTurn);
    assert_eq!(run.parent_thread_id.as_deref(), Some("thread-1"));
    assert_eq!(run.agent_id.as_deref(), Some("orchestrator"));
    assert_eq!(
        run.metadata,
        serde_json::json!({
            "client_id": "client-1",
            "thread_id": "thread-1",
            "request_id": "request-1",
            "source": "web_channel",
            "agent_id": "orchestrator",
        })
    );

    ledger
        .observe(&AgentProgress::TurnCompleted {
            iterations: 1,
            stop: None,
        })
        .await;
    let run = store
        .get_agent_run("request-1")
        .await
        .expect("read run")
        .expect("run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    let events = store
        .list_recent_run_events(&RunEventListRequest {
            run_id: "request-1".into(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("read events")
        .events;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        ["turn_started", "tool_call_started", "turn_completed"]
    );
    assert_eq!(events[1].payload["call_id"], "call-1");
    assert_eq!(events[1].payload["arguments"]["path"], "notes.md");
}

#[tokio::test]
async fn terminal_progress_event_and_status_share_one_atomic_write() {
    let backend = MemoryBackend::new();
    let scope = Scope::new("local").expect("scope");
    let scoped = backend.for_scope(&scope).expect("scoped storage");
    let faults = FaultPlan::new();
    let documents = Arc::new(Failing::with_plan(
        Arc::clone(scoped.documents()),
        faults.clone(),
    ));
    let scoped = ScopedStorage::new(
        Storage::new(
            documents,
            Arc::clone(scoped.streams()),
            Arc::clone(scoped.kv()),
            Arc::clone(scoped.blobs()),
            Arc::clone(scoped.queues()),
            Arc::clone(scoped.secrets()),
        ),
        scope,
    );
    let store = Arc::new(DriverRunLedger::new(scoped));
    let ledger = RunProgressLedger::new(Some(store.clone()), identity());
    ledger.observe(&AgentProgress::TurnStarted).await;

    // With split appends, the completion event is write one and the status
    // update is write two. The atomic transition should need only one write.
    faults.fail_operation_nth("documents.atomic_batch_checked", 3, ErrorKind::Unavailable);
    ledger.observe(&AgentProgress::turn_completed(1)).await;
    faults.clear();

    let run = store
        .get_agent_run("request-1")
        .await
        .expect("read run")
        .expect("run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    let events = store
        .list_recent_run_events(&RunEventListRequest {
            run_id: "request-1".into(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("read events")
        .events;
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        ["turn_started", "turn_completed"]
    );
}

#[tokio::test]
async fn finish_interrupts_only_an_open_run_and_is_idempotent() {
    let backend = MemoryBackend::new();
    let store = make_store(&backend);
    let ledger = RunProgressLedger::new(Some(store.clone()), identity());

    ledger.observe(&AgentProgress::TurnStarted).await;
    ledger.finish().await;
    ledger.finish().await;

    let run = store
        .get_agent_run("request-1")
        .await
        .expect("read run")
        .expect("run created");
    assert_eq!(run.status, AgentRunStatus::Interrupted);
    let events = store
        .list_recent_run_events(&RunEventListRequest {
            run_id: "request-1".into(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("read events")
        .events;
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, "turn_interrupted");

    let completed_backend = MemoryBackend::new();
    let completed_store = make_store(&completed_backend);
    let completed = RunProgressLedger::new(Some(completed_store.clone()), identity());
    completed.observe(&AgentProgress::TurnStarted).await;
    completed.observe(&AgentProgress::turn_completed(1)).await;
    completed.finish().await;
    let run = completed_store
        .get_agent_run("request-1")
        .await
        .expect("read completed run")
        .expect("completed run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn subagent_context_correlates_each_task_until_its_terminal_event() {
    let ledger = RunProgressLedger::new(None, identity());

    let spawned = ledger
        .observe(&AgentProgress::SubagentSpawned {
            agent_id: "researcher".into(),
            task_id: "task-1".into(),
            mode: "typed".into(),
            dedicated_thread: false,
            prompt_chars: 10,
            worker_thread_id: None,
            display_name: None,
            prompt: "find a source".into(),
            parent_call_id: Some("parent-call".into()),
        })
        .await;
    assert_eq!(spawned.parent_call_id.as_deref(), Some("parent-call"));

    let child_tool = ledger
        .observe(&AgentProgress::SubagentToolCallStarted {
            agent_id: "researcher".into(),
            task_id: "task-1".into(),
            call_id: "child-call".into(),
            tool_name: "search".into(),
            arguments: serde_json::json!({"query": "source"}),
            iteration: 1,
            display_label: None,
            display_detail: None,
        })
        .await;
    assert_eq!(child_tool.parent_call_id.as_deref(), Some("parent-call"));

    let finished = ledger
        .observe(&AgentProgress::SubagentCompleted {
            agent_id: "researcher".into(),
            task_id: "task-1".into(),
            elapsed_ms: 42,
            iterations: 1,
            output_chars: 4,
            usage: Some(SubagentUsage::default()),
            output: "done".into(),
            worktree_path: None,
            changed_files: vec![],
            dirty_status: None,
            stop: None,
        })
        .await;
    assert_eq!(finished.parent_call_id.as_deref(), Some("parent-call"));

    let after_finish = ledger
        .observe(&AgentProgress::SubagentIterationStarted {
            agent_id: "researcher".into(),
            task_id: "task-1".into(),
            iteration: 2,
            max_iterations: 3,
            extended_policy: false,
        })
        .await;
    assert_eq!(after_finish.parent_call_id, None);
    assert_eq!(ledger.events_seen(), 4);
}

#[tokio::test]
async fn child_model_completion_keeps_its_parent_call_context() {
    let ledger = RunProgressLedger::new(None, identity());
    ledger
        .observe(&AgentProgress::SubagentSpawned {
            agent_id: "researcher".into(),
            task_id: "task-1".into(),
            mode: "typed".into(),
            dedicated_thread: false,
            prompt_chars: 10,
            worker_thread_id: None,
            display_name: None,
            prompt: "find a source".into(),
            parent_call_id: Some("parent-call".into()),
        })
        .await;

    let completion = ledger
        .observe(&AgentProgress::ModelCallCompleted {
            model: "model-1".into(),
            provider_id: "provider-1".into(),
            subagent_task_id: Some("task-1".into()),
            input: None,
            output: None,
            iteration: 1,
            input_tokens: 10,
            output_tokens: 4,
            cached_input_tokens: 0,
            cache_creation_tokens: 0,
            reasoning_tokens: 0,
            cost_usd: None,
        })
        .await;

    assert_eq!(completion.parent_call_id.as_deref(), Some("parent-call"));
}
