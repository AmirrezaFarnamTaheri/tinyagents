//! Best-effort durable observation of one agent turn's progress stream.
//!
//! The ledger writes each [`AgentProgress`](tinyagents_harness::progress::AgentProgress)
//! as an append-only run event and keeps the run status aligned with turn
//! completion. It also preserves subagent task-to-parent-call correlation for
//! host projections whose later lifecycle events omit the parent call id.

use std::{
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde_json::{Map, Value};
use tinyagents_harness::progress::AgentProgress;
use tinyagents_session::run_ledger::{
    AgentRunKind, AgentRunStatus, AgentRunUpsert, RunEventAppend, RunLedgerStore,
};
use tokio::sync::Mutex;

/// Stable identity assigned by the host to one observed turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProgressIdentity {
    /// Host or client that initiated the turn.
    pub client_id: String,
    /// Conversation containing the turn.
    pub thread_id: String,
    /// Stable request identifier, also used as the run id.
    pub request_id: String,
    /// Surface that initiated the turn (for example, `web_channel`).
    pub source: String,
    /// Agent definition executing the turn.
    pub agent_id: String,
}

/// Correlation data for projecting a progress event into a host surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressLedgerContext {
    /// Parent tool call that spawned this event's subagent task, when known.
    pub parent_call_id: Option<String>,
}

#[derive(Default)]
struct LedgerState {
    subagent_parent_calls: HashMap<String, Option<String>>,
}

/// Records progress for one foreground turn in the shared TinyAgents run ledger.
///
/// Storage failures are logged and do not interrupt the live agent turn. The
/// observer also works without a store, retaining only in-memory correlation
/// needed by the host projection.
pub struct RunProgressLedger {
    store: Option<Arc<dyn RunLedgerStore>>,
    identity: RunProgressIdentity,
    state: Mutex<LedgerState>,
    events_seen: AtomicUsize,
}

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

        if let Some((event_type, payload)) = serialize_event(event) {
            if let Err(error) = store
                .append_run_event(RunEventAppend {
                    run_id: self.identity.request_id.clone(),
                    event_type,
                    payload,
                })
                .await
            {
                tracing::warn!(run_id = %self.identity.request_id, %error, "could not append progress ledger event");
            }
        } else {
            tracing::warn!(run_id = %self.identity.request_id, "could not serialize progress ledger event");
        }

        if matches!(event, AgentProgress::TurnCompleted { .. }) {
            self.transition_if_open(store.as_ref(), AgentRunStatus::Completed, None)
                .await;
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
            .append_run_event(RunEventAppend {
                run_id: self.identity.request_id.clone(),
                event_type: "turn_interrupted".into(),
                payload: Value::Object(Map::new()),
            })
            .await
        {
            tracing::warn!(run_id = %self.identity.request_id, %error, "could not append interrupted progress ledger event");
        }
        if let Err(error) = store
            .transition_agent_run_status(
                &self.identity.request_id,
                AgentRunStatus::Interrupted,
                None,
                Some(chrono::Utc::now()),
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
    ) {
        match store.get_agent_run(&self.identity.request_id).await {
            Ok(Some(run)) if !is_terminal(run.status) => {
                if let Err(error) = store
                    .transition_agent_run_status(
                        &self.identity.request_id,
                        status,
                        error,
                        Some(chrono::Utc::now()),
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
        _ => ProgressLedgerContext::default(),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
