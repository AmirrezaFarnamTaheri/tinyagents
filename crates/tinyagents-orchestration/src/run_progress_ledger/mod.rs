//! Best-effort durable observation of one agent turn's progress stream.
//!
//! The ledger writes each [`AgentProgress`](tinyagents_harness::progress::AgentProgress)
//! as an append-only run event and keeps the run status aligned with turn
//! completion. It also preserves subagent task-to-parent-call correlation for
//! host projections whose later lifecycle events omit the parent call id.

mod types;

use std::sync::atomic::Ordering;
pub use types::{ProgressLedgerContext, RunProgressIdentity, RunProgressLedger};

use serde_json::{Map, Value};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tinyagents_harness::progress::AgentProgress;
use tinyagents_session::run_ledger::{
    AgentRunKind, AgentRunStatus, AgentRunUpsert, RunEventAppend, RunLedgerStore,
};
use tokio::sync::Mutex;
use types::LedgerState;

impl RunProgressLedger {
    /// Creates a progress observer for one turn.
    pub fn new(store: Option<Arc<dyn RunLedgerStore>>, identity: RunProgressIdentity) -> Self {
        Self {
            store,
            identity,
            state: Mutex::new(LedgerState::default()),
            events_seen: AtomicUsize::new(0),
        }
    }

    /// Records one progress event and returns its authoritative projection context.
    pub async fn observe(&self, event: &AgentProgress) -> ProgressLedgerContext {
        self.events_seen.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().await;
        let context = correlation_for(event, &mut state);

        let Some(store) = self.store.as_ref() else {
            return context;
        };

        if let Err(error) = self.ensure_run(store.as_ref()).await {
            tracing::warn!(run_id = %self.identity.request_id, %error, "could not create progress ledger run");
            return context;
        }

        let completed = match event {
            AgentProgress::TurnCompleted {
                stop: Some(stop), ..
            } if stop.kind == tinyagents_harness::TurnStopKind::Breaker => {
                Some(AgentRunStatus::Failed)
            }
            AgentProgress::TurnCompleted { .. } => Some(AgentRunStatus::Completed),
            _ => None,
        };
        if let Some((event_type, payload)) = serialize_event(event) {
            let event = RunEventAppend {
                run_id: self.identity.request_id.clone(),
                event_type,
                payload,
            };
            if let Some(status) = completed {
                self.transition_if_open(store.as_ref(), status, None, event)
                    .await;
            } else if let Err(error) = store.append_run_event(event).await {
                tracing::warn!(run_id = %self.identity.request_id, %error, "could not append progress ledger event");
            }
        } else {
            tracing::warn!(run_id = %self.identity.request_id, "could not serialize progress ledger event");
        }
        context
    }

    /// Marks a still-open run interrupted. Repeated calls and completed runs are unchanged.
    pub async fn finish(&self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let _state = self.state.lock().await;
        let run = match store.get_agent_run(&self.identity.request_id).await {
            Ok(Some(run)) => run,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(run_id = %self.identity.request_id, %error, "could not read progress ledger run during finish");
                return;
            }
        };
        if is_terminal(run.status) {
            return;
        }

        if let Err(error) = store
            .transition_agent_run_status_from(
                &self.identity.request_id,
                run.status,
                AgentRunStatus::Interrupted,
                None,
                Some(chrono::Utc::now()),
                Some(RunEventAppend {
                    run_id: self.identity.request_id.clone(),
                    event_type: "turn_interrupted".into(),
                    payload: Value::Object(Map::new()),
                }),
            )
            .await
        {
            tracing::warn!(run_id = %self.identity.request_id, %error, "could not interrupt progress ledger run");
        }
    }

    /// Number of events observed, including when no durable store is installed.
    pub fn events_seen(&self) -> usize {
        self.events_seen.load(Ordering::Relaxed)
    }

    async fn ensure_run(
        &self,
        store: &dyn RunLedgerStore,
    ) -> Result<(), tinyagents_session::run_ledger::LedgerError> {
        if store
            .get_agent_run(&self.identity.request_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        store
            .upsert_agent_run(AgentRunUpsert {
                id: self.identity.request_id.clone(),
                kind: AgentRunKind::InteractiveTurn,
                parent_run_id: None,
                parent_thread_id: Some(self.identity.thread_id.clone()),
                agent_id: Some(self.identity.agent_id.clone()),
                status: AgentRunStatus::Running,
                prompt_ref: None,
                worker_thread_id: None,
                checkpoint_path: None,
                checkpoint: None,
                summary: None,
                error: None,
                metadata: serde_json::json!({
                    "client_id": self.identity.client_id,
                    "thread_id": self.identity.thread_id,
                    "request_id": self.identity.request_id,
                    "source": self.identity.source,
                    "agent_id": self.identity.agent_id,
                }),
                started_at: None,
                completed_at: None,
            })
            .await?;
        Ok(())
    }

    async fn transition_if_open(
        &self,
        store: &dyn RunLedgerStore,
        status: AgentRunStatus,
        error: Option<&str>,
        event: RunEventAppend,
    ) {
        match store.get_agent_run(&self.identity.request_id).await {
            Ok(Some(run)) if !is_terminal(run.status) => {
                if let Err(error) = store
                    .transition_agent_run_status_from(
                        &self.identity.request_id,
                        run.status,
                        status,
                        error,
                        Some(chrono::Utc::now()),
                        Some(event),
                    )
                    .await
                {
                    tracing::warn!(run_id = %self.identity.request_id, %error, "could not transition progress ledger run");
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(run_id = %self.identity.request_id, %error, "could not read progress ledger run for transition")
            }
        }
    }
}

fn is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed
            | AgentRunStatus::Failed
            | AgentRunStatus::Cancelled
            | AgentRunStatus::Interrupted
    )
}

fn serialize_event(event: &AgentProgress) -> Option<(String, Value)> {
    let Value::Object(mut object) = serde_json::to_value(event).ok()? else {
        return None;
    };
    let Value::String(event_type) = object.remove("type")? else {
        return None;
    };
    Some((event_type, Value::Object(object)))
}

fn correlation_for(event: &AgentProgress, state: &mut LedgerState) -> ProgressLedgerContext {
    match event {
        AgentProgress::SubagentSpawned {
            task_id,
            parent_call_id,
            ..
        } => {
            state
                .subagent_parent_calls
                .insert(task_id.clone(), parent_call_id.clone());
            ProgressLedgerContext {
                parent_call_id: parent_call_id.clone(),
            }
        }
        AgentProgress::SubagentCompleted { task_id, .. }
        | AgentProgress::SubagentFailed { task_id, .. } => {
            let parent_call_id = state.subagent_parent_calls.remove(task_id).flatten();
            ProgressLedgerContext { parent_call_id }
        }
        AgentProgress::SubagentAwaitingUser { task_id, .. }
        | AgentProgress::SubagentIterationStarted { task_id, .. }
        | AgentProgress::SubagentToolCallStarted { task_id, .. }
        | AgentProgress::SubagentToolCallCompleted { task_id, .. }
        | AgentProgress::SubagentTextDelta { task_id, .. }
        | AgentProgress::SubagentThinkingDelta { task_id, .. } => ProgressLedgerContext {
            parent_call_id: state.subagent_parent_calls.get(task_id).cloned().flatten(),
        },
        AgentProgress::ModelCallCompleted {
            subagent_task_id: Some(task_id),
            ..
        } => ProgressLedgerContext {
            parent_call_id: state.subagent_parent_calls.get(task_id).cloned().flatten(),
        },
        _ => ProgressLedgerContext::default(),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
