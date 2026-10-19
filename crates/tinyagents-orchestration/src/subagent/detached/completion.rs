//! Recording a detached subagent's terminal status with the completion router.
//!
//! The driver records children it runs itself. A host that tracks detached
//! children through a status channel (see [`spawn_status_watcher`]) uses
//! [`spawn_status_watcher_with_completions`] to get the same durable push:
//! the first terminal status is mirrored into the [`TaskStore`] and then
//! recorded with the router.
//!
//! [`spawn_status_watcher`]: super::spawn_status_watcher

use std::sync::Arc;

use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::TaskId;
use tinyagents_tasks::{
    CompletionRecord, CompletionResult, CompletionRouter, CompletionStatus, NotifyMode,
    OrchestrationTaskStatus, TaskStore,
};
use tokio::sync::watch;

use super::ledger::record_status_with_retries;
use super::types::DetachedSubagentStatus;

const LOG_PREFIX: &str = "[detached-completion]";
const PAUSE_RECORD_PREFIX: &str = "\u{1}subagent-pause:";

/// Stable prefix for independently deliverable pause notices belonging to a
/// task. Hosts may use it to withdraw notices when the task is collected.
pub fn pause_record_prefix(task_id: &str) -> String {
    let encoded_task: String = task_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{PAUSE_RECORD_PREFIX}{encoded_task}:")
}

/// Where a detached child's completion goes and how it announces itself.
#[derive(Clone)]
pub struct DetachedCompletionTarget {
    /// The router to record with.
    pub router: Arc<CompletionRouter>,
    /// The parent key (a thread id or session key that survives a restart).
    pub parent_key: String,
    /// The child's agent id.
    pub agent_id: String,
    /// Optional human label.
    pub label: Option<String>,
    /// How the parent is told.
    pub notify_mode: NotifyMode,
    /// Lifecycle generation captured when the child was admitted.
    pub parent_epoch: u64,
}

impl DetachedCompletionTarget {
    /// A target with the default [`NotifyMode`] and no label.
    pub fn new(
        router: Arc<CompletionRouter>,
        parent_key: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        Self {
            router,
            parent_key: parent_key.into(),
            agent_id: agent_id.into(),
            label: None,
            notify_mode: NotifyMode::default(),
            parent_epoch: 0,
        }
    }

    /// Creates a target only when the parent lifecycle is open and captures
    /// the generation that every later pause and terminal record must retain.
    pub fn try_new(
        router: Arc<CompletionRouter>,
        parent_key: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Result<Self, TinyAgentsError> {
        let parent_key = parent_key.into();
        let parent_epoch = router
            .admit_child_generation(&parent_key)?
            .ok_or_else(|| {
                TinyAgentsError::Graph(format!(
                    "detached child parent lifecycle is closed: {parent_key:?}"
                ))
            })?;
        Ok(Self {
            router,
            parent_key,
            agent_id: agent_id.into(),
            label: None,
            notify_mode: NotifyMode::default(),
            parent_epoch,
        })
    }

    /// Sets the human label.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Sets the notify mode.
    pub fn with_notify_mode(mut self, mode: NotifyMode) -> Self {
        self.notify_mode = mode;
        self
    }

    /// The completion for `status`, or `None` for a status that is not a
    /// completion: still running, or paused on a question (the same task
    /// completes later, so a pause must not occupy its dedupe slot).
    pub fn record_for(
        &self,
        task_id: &str,
        status: &DetachedSubagentStatus,
    ) -> Option<CompletionRecord> {
        let state = CompletionStatus::try_from(status).ok()?;
        let text = match status {
            DetachedSubagentStatus::Completed { output, .. } => output.clone(),
            DetachedSubagentStatus::Failed { error } => error.clone(),
            DetachedSubagentStatus::Running | DetachedSubagentStatus::AwaitingUser { .. } => {
                return None;
            }
        };
        let mut record = CompletionRecord::new(
            task_id,
            self.parent_key.clone(),
            self.agent_id.clone(),
            state,
            CompletionResult::text(text),
        )
        .with_notify_mode(self.notify_mode);
        record.label = self.label.clone();
        record.parent_epoch = self.parent_epoch;
        Some(record)
    }

    /// Creates a separate deliverable notice for a child pause. The unique
    /// record id leaves `task_id` available for the child's eventual terminal
    /// completion and permits the same child to pause more than once.
    pub fn pause_record_for(&self, task_id: &str, notice: impl Into<String>) -> CompletionRecord {
        let record_id = format!(
            "{}{}",
            pause_record_prefix(task_id),
            uuid::Uuid::new_v4().simple()
        );
        let mut record = CompletionRecord::new(
            record_id,
            self.parent_key.clone(),
            self.agent_id.clone(),
            CompletionStatus::Incomplete,
            CompletionResult::text(notice),
        )
        .with_notify_mode(self.notify_mode);
        record.label = self.label.clone();
        record.parent_epoch = self.parent_epoch;
        record
    }

    /// Records an independently deliverable pause notice with the same retry
    /// policy as terminal detached completions.
    pub async fn record_pause(&self, task_id: &str, notice: impl Into<String>) -> bool {
        let record = self.pause_record_for(task_id, notice);
        match self.router.record_with_retries(record, 3).await {
            Ok(outcome) => {
                tracing::debug!(task_id, outcome = ?outcome, "{LOG_PREFIX} recorded pause notice");
                true
            }
            Err(error) => {
                tracing::warn!(task_id, error = %error, "{LOG_PREFIX} could not record pause notice");
                false
            }
        }
    }
}

/// Records the completion `status` stands for, if it is one. A router failure
/// is logged and returned as `false`.
pub async fn record_detached_completion(
    target: &DetachedCompletionTarget,
    task_id: &str,
    status: &DetachedSubagentStatus,
) -> bool {
    let Some(record) = target.record_for(task_id, status) else {
        tracing::debug!(
            "{LOG_PREFIX} task_id={task_id} status={} is not a completion",
            status.label()
        );
        return true;
    };
    match target.router.record_with_retries(record, 3).await {
        Ok(outcome) => {
            tracing::debug!("{LOG_PREFIX} task_id={task_id} outcome={outcome:?}");
            true
        }
        Err(error) => {
            tracing::warn!("{LOG_PREFIX} task_id={task_id} could not record completion: {error}");
            false
        }
    }
}

/// Like [`spawn_status_watcher`](super::spawn_status_watcher), and additionally
/// records the first final status with `target`'s router after it is mirrored
/// into `store`.
///
/// A pause (`AwaitingUser`) is mirrored into the store and the watcher keeps
/// waiting, so a resumed child that reports on the same channel is still
/// recorded when it finishes. A dropped sender is a failed completion unless
/// the ledger already shows a requested or recorded cancellation: the parent
/// asked for that, so it is not pushed back to it. (A host that aborts a child
/// without marking the ledger should [`CompletionRouter::tombstone`] it first.)
pub fn spawn_status_watcher_with_completions(
    store: Arc<dyn TaskStore>,
    task_id: String,
    mut status: watch::Receiver<DetachedSubagentStatus>,
    target: DetachedCompletionTarget,
) {
    tokio::spawn(async move {
        let terminal = loop {
            let snapshot = status.borrow_and_update().clone();
            match snapshot {
                DetachedSubagentStatus::Running => {}
                DetachedSubagentStatus::AwaitingUser { .. } => {
                    record_status_with_retries(store.as_ref(), &task_id, &snapshot);
                }
                _ => break snapshot,
            }
            if status.changed().await.is_err() {
                break DetachedSubagentStatus::ended_without_result();
            }
        };
        let cancelled_now = || {
            matches!(
                store.get(&TaskId::new(&task_id)).map(|r| r.status),
                Some(OrchestrationTaskStatus::CancelRequested | OrchestrationTaskStatus::Cancelled)
            )
        };
        // Before the write, a requested cancel would be overwritten by it; after
        // it, a cancel that won the race leaves the record cancelled.
        let cancelled_before = cancelled_now();
        // A failed ledger write is logged, not fatal: the completion store is
        // independent, and the parent should hear about a finished child even
        // if the task ledger lags (restart reconciliation settles that record).
        if !record_status_with_retries(store.as_ref(), &task_id, &terminal) {
            tracing::warn!("{LOG_PREFIX} task_id={task_id} terminal status not written to ledger");
        }
        if cancelled_before || cancelled_now() {
            tracing::debug!("{LOG_PREFIX} task_id={task_id} cancelled; not recorded");
            return;
        }
        record_detached_completion(&target, &task_id, &terminal).await;
    });
}
