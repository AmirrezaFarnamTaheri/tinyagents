//! Records generic agent progress in a host-selected, tenant-scoped run ledger.
//!
//! The host supplies the [`RunLedgerStore`] handle. This crate owns the
//! progress-to-ledger mapping; it never selects a backend or derives storage
//! scope from event payloads.

use std::sync::Arc;

use serde_json::json;
use tinyagents_session::run_ledger::{
    AgentRunKind, AgentRunStatus, AgentRunUpsert, RunEventAppend, RunLedgerStore, RunTelemetry,
    RunTelemetryUpsert,
};

/// Identity attached to progress rows. Storage scope comes only from `store`.
#[derive(Debug, Clone)]
pub struct RunProgressIdentity {
    pub client_id: String,
    pub thread_id: String,
    pub request_id: String,
    pub source: String,
    pub agent_id: String,
}

/// What a finished sub-agent adds to its ledger row.
pub struct ChildDone<'a> {
    pub agent_id: &'a str,
    pub task_id: &'a str,
    pub elapsed_ms: u64,
    pub iterations: u32,
    pub output_chars: usize,
    pub worktree_path: &'a Option<String>,
    pub changed_files: &'a [String],
    pub dirty_status: Option<bool>,
    pub parent_call_id: &'a Option<String>,
    pub tool_count: Option<u64>,
}

/// Progress recorder for one parent agent turn and its child runs.
pub struct RunProgressLedger {
    store: Arc<dyn RunLedgerStore>,
    identity: RunProgressIdentity,
}

impl RunProgressLedger {
    /// Bind the recorder to the store already selected by the host's trusted
    /// tenant/agent context.
    pub fn new(store: Arc<dyn RunLedgerStore>, identity: RunProgressIdentity) -> Self {
        Self { store, identity }
    }

    async fn run(&self, upsert: AgentRunUpsert) {
        if let Err(err) = self.store.upsert_agent_run(upsert).await {
            tracing::warn!(error = %err, "failed to upsert progress run");
        }
    }

    async fn event(&self, run_id: &str, event_type: &str, payload: serde_json::Value) {
        let append = RunEventAppend {
            run_id: run_id.to_owned(),
            event_type: event_type.to_owned(),
            payload,
        };
        if let Err(err) = self.store.append_run_event(append).await {
            tracing::warn!(error = %err, "failed to append progress event");
        }
    }

    async fn telemetry(&self, telemetry: RunTelemetryUpsert) {
        if let Err(err) = self.store.upsert_run_telemetry(telemetry).await {
            tracing::warn!(error = %err, "failed to upsert progress telemetry");
        }
    }

    /// The turn's telemetry as written so far, for the trace export.
    pub async fn read_telemetry(&self) -> Option<RunTelemetry> {
        match self.store.get_agent_run(&self.identity.request_id).await {
            Ok(run) => run.and_then(|run| run.telemetry),
            Err(err) => {
                tracing::warn!(error = %err, "failed to read progress telemetry");
                None
            }
        }
    }

    fn turn_row(
        &self,
        status: AgentRunStatus,
        summary: Option<String>,
        error: Option<String>,
        metadata: serde_json::Value,
        done: bool,
    ) -> AgentRunUpsert {
        AgentRunUpsert {
            id: self.identity.request_id.clone(),
            kind: AgentRunKind::BackgroundAgent,
            parent_run_id: None,
            parent_thread_id: Some(self.identity.thread_id.clone()),
            agent_id: Some(self.identity.agent_id.clone()),
            status,
            prompt_ref: Some(format!(
                "thread:{}:request:{}",
                self.identity.thread_id, self.identity.request_id
            )),
            worker_thread_id: None,
            checkpoint_path: None,
            checkpoint: None,
            summary,
            error,
            metadata,
            started_at: None,
            completed_at: done.then(chrono::Utc::now),
        }
    }

    pub async fn turn_started(&self) {
        self.run(self.turn_row(
            AgentRunStatus::Running,
            None,
            None,
            json!({ "clientId": &self.identity.client_id, "source": &self.identity.source, "schemaVersion": 1 }),
            false,
        )).await;
        self.event(
            &self.identity.request_id,
            "turn_started",
            json!({ "threadId": &self.identity.thread_id, "clientId": &self.identity.client_id }),
        )
        .await;
    }

    pub async fn turn_completed(&self, iterations: u32) {
        self.run(self.turn_row(
            AgentRunStatus::Completed,
            Some(format!("Completed in {iterations} iteration(s)")),
            None,
            json!({}),
            true,
        ))
        .await;
        self.event(
            &self.identity.request_id,
            "turn_completed",
            json!({ "iterations": iterations }),
        )
        .await;
    }

    pub async fn turn_interrupted(&self, events_seen: u64) {
        self.run(self.turn_row(
            AgentRunStatus::Interrupted,
            None,
            Some("progress bridge exited before turn completion".to_owned()),
            json!({}),
            true,
        ))
        .await;
        self.event(
            &self.identity.request_id,
            "turn_interrupted",
            json!({ "eventsSeen": events_seen }),
        )
        .await;
    }

    pub async fn tool_started(&self, call_id: &str, tool_name: &str, iteration: u32, count: u64) {
        self.event(
            &self.identity.request_id,
            "tool_call_started",
            json!({ "callId": call_id, "toolName": tool_name, "iteration": iteration }),
        )
        .await;
        self.telemetry(RunTelemetryUpsert {
            run_id: self.identity.request_id.clone(),
            tool_count: Some(count),
            ..Default::default()
        })
        .await;
    }

    pub async fn tool_completed(&self, payload: serde_json::Value) {
        self.event(&self.identity.request_id, "tool_call_completed", payload)
            .await;
    }

    pub async fn cost(
        &self,
        model: &str,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        cost_usd: f64,
    ) {
        self.telemetry(RunTelemetryUpsert {
            run_id: self.identity.request_id.clone(),
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            cached_input_tokens: Some(cached_input_tokens),
            cost_usd: Some(cost_usd),
            model: Some(model.to_owned()),
            ..Default::default()
        })
        .await;
    }

    fn child_row(
        &self,
        task_id: &str,
        agent_id: &str,
        kind: AgentRunKind,
        status: AgentRunStatus,
    ) -> AgentRunUpsert {
        AgentRunUpsert {
            id: task_id.to_owned(),
            kind,
            parent_run_id: Some(self.identity.request_id.clone()),
            parent_thread_id: Some(self.identity.thread_id.clone()),
            agent_id: Some(agent_id.to_owned()),
            status,
            prompt_ref: None,
            worker_thread_id: None,
            checkpoint_path: None,
            checkpoint: None,
            summary: None,
            error: None,
            metadata: json!({}),
            started_at: None,
            completed_at: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn child_spawned(
        &self,
        agent_id: &str,
        task_id: &str,
        mode: &str,
        dedicated_thread: bool,
        prompt_chars: usize,
        worker_thread_id: &Option<String>,
        display_name: &Option<String>,
        parent_call_id: &Option<String>,
    ) {
        let kind = if worker_thread_id.is_some() {
            AgentRunKind::WorkerThread
        } else {
            AgentRunKind::Subagent
        };
        let mut row = self.child_row(task_id, agent_id, kind, AgentRunStatus::Running);
        row.prompt_ref = worker_thread_id
            .as_ref()
            .map(|id| format!("thread:{id}:message:seed"));
        row.worker_thread_id.clone_from(worker_thread_id);
        row.metadata = json!({
            "mode": mode,
            "dedicatedThread": dedicated_thread,
            "promptChars": prompt_chars,
            "displayName": display_name,
            "parentCallId": parent_call_id,
            "source": "agent_progress",
            "schemaVersion": 1
        });
        self.run(row).await;
        self.event(
            task_id,
            "subagent_spawned",
            json!({
                "agentId": agent_id,
                "parentRunId": &self.identity.request_id,
                "threadId": &self.identity.thread_id,
                "workerThreadId": worker_thread_id,
                "mode": mode,
                "dedicatedThread": dedicated_thread,
                "promptChars": prompt_chars,
                "displayName": display_name,
                "parentCallId": parent_call_id
            }),
        )
        .await;
    }

    pub async fn child_completed(&self, done: &ChildDone<'_>) {
        let mut row = self.child_row(
            done.task_id,
            done.agent_id,
            AgentRunKind::Subagent,
            AgentRunStatus::Completed,
        );
        row.summary = Some(format!(
            "Completed in {} iteration(s), {} output chars",
            done.iterations, done.output_chars
        ));
        row.completed_at = Some(chrono::Utc::now());
        self.run(row).await;
        self.telemetry(RunTelemetryUpsert {
            run_id: done.task_id.to_owned(),
            elapsed_ms: Some(done.elapsed_ms),
            tool_count: done.tool_count,
            ..Default::default()
        })
        .await;
        self.event(
            done.task_id,
            "subagent_completed",
            json!({
                "agentId": done.agent_id,
                "elapsedMs": done.elapsed_ms,
                "iterations": done.iterations,
                "outputChars": done.output_chars,
                "worktreePath": done.worktree_path,
                "changedFiles": done.changed_files,
                "dirtyStatus": done.dirty_status,
                "parentCallId": done.parent_call_id
            }),
        )
        .await;
    }

    pub async fn child_failed(
        &self,
        agent_id: &str,
        task_id: &str,
        error: &str,
        parent_call_id: &Option<String>,
        tool_count: Option<u64>,
    ) {
        let mut row = self.child_row(
            task_id,
            agent_id,
            AgentRunKind::Subagent,
            AgentRunStatus::Failed,
        );
        row.error = Some(error.to_owned());
        row.completed_at = Some(chrono::Utc::now());
        self.run(row).await;
        self.telemetry(RunTelemetryUpsert {
            run_id: task_id.to_owned(),
            tool_count,
            error: Some(error.to_owned()),
            ..Default::default()
        })
        .await;
        self.event(
            task_id,
            "subagent_failed",
            json!({ "agentId": agent_id, "error": error, "parentCallId": parent_call_id }),
        )
        .await;
    }

    pub async fn child_awaiting_user(
        &self,
        agent_id: &str,
        task_id: &str,
        question: &str,
        worker_thread_id: &Option<String>,
        checkpoint_path: &Option<String>,
        parent_call_id: &Option<String>,
    ) {
        let kind = if worker_thread_id.is_some() {
            AgentRunKind::WorkerThread
        } else {
            AgentRunKind::Subagent
        };
        let mut row = self.child_row(task_id, agent_id, kind, AgentRunStatus::AwaitingUser);
        row.worker_thread_id.clone_from(worker_thread_id);
        // What the runner actually wrote; a rebuild from `workspace_dir` would
        // assert a checkpoint that may never have been written (#5928).
        row.checkpoint_path.clone_from(checkpoint_path);
        row.checkpoint = Some(json!({
            "resumeTool": "continue_subagent",
            "taskId": task_id,
            "agentId": agent_id,
            "question": question,
            "workerThreadId": worker_thread_id,
            "checkpointPersisted": checkpoint_path.is_some()
        }));
        row.summary = Some(question.to_owned());
        self.run(row).await;
        self.event(
            task_id,
            "subagent_awaiting_user",
            json!({
                "agentId": agent_id,
                "question": question,
                "workerThreadId": worker_thread_id,
                "parentCallId": parent_call_id
            }),
        )
        .await;
    }

    pub async fn child_tool_started(
        &self,
        agent_id: &str,
        task_id: &str,
        call_id: &str,
        tool_name: &str,
        iteration: u32,
        count: u64,
    ) {
        self.telemetry(RunTelemetryUpsert {
            run_id: task_id.to_owned(),
            tool_count: Some(count),
            ..Default::default()
        })
        .await;
        self.event(
            task_id,
            "subagent_tool_call_started",
            json!({
                "agentId": agent_id,
                "callId": call_id,
                "toolName": tool_name,
                "iteration": iteration
            }),
        )
        .await;
    }

    pub async fn child_tool_completed(&self, task_id: &str, payload: serde_json::Value) {
        self.event(task_id, "subagent_tool_call_completed", payload)
            .await;
    }
}

#[cfg(test)]
#[path = "run_progress_ledger_tests.rs"]
mod tests;
