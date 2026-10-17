//! Live-first detached task operations with durable recovery fallbacks.

use std::time::{Duration, Instant};

use tinyagents_harness::ids::TaskId;
use tinyagents_tasks::{
    CompletionRouter, DetachedTaskRegistry, DetachedTaskRegistryError, TaskStore,
};

use super::ledger::{
    record_agent_id, record_cancelled, record_subagent_session_id, record_to_wait_outcome,
    subagent_record_for_task,
};
use super::roster::{
    resume_ref_for_task, resume_ref_from_record, task_id_for_session,
    task_id_for_session_in_records,
};
use super::status::wait_detached;
use super::types::{
    DetachedSubagentStatus, SubagentIdentity, SubagentResumeRef, WaitError, WaitOutcome,
};

/// Waits on a live task first, then reads its durable record when the runtime
/// is unknown (for example after restart). Ownership is checked in either path.
pub async fn wait_detached_or_durable<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    store: &dyn TaskStore,
    completions: &CompletionRouter,
    task_id: &str,
    owner: &str,
    timeout: Duration,
) -> Result<WaitOutcome, WaitError>
where
    M: Clone + Send + Sync + 'static,
{
    match wait_detached(registry, task_id, owner, timeout).await {
        Ok(outcome) => {
            tombstone_collected_completion(completions, task_id, &outcome)?;
            Ok(outcome)
        }
        Err(WaitError::Unknown) => {
            let record = subagent_record_for_task(store, task_id, owner)?;
            let outcome = record_to_wait_outcome(record);
            tombstone_collected_completion(completions, task_id, &outcome)?;
            Ok(outcome)
        }
        Err(error) => Err(error),
    }
}

fn tombstone_collected_completion(
    completions: &CompletionRouter,
    task_id: &str,
    outcome: &WaitOutcome,
) -> Result<(), WaitError> {
    if matches!(outcome, WaitOutcome::Terminal(status) if status.finished_outcome().is_some()) {
        completions.tombstone(task_id).map_err(|error| {
            tracing::warn!(task_id, error = %error, "could not tombstone collected completion");
            WaitError::Unknown
        })?;
    }
    Ok(())
}

/// Resolves a task id or durable worker session id to a resume reference,
/// preferring a live registry entry and then consulting the persistent ledger.
pub fn resolve_detached_or_durable<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    store: &dyn TaskStore,
    id_or_session: &str,
    owner: &str,
) -> Result<SubagentResumeRef, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    match resume_ref_for_task(registry, id_or_session, owner) {
        Ok(resolved) => return Ok(resolved),
        Err(WaitError::Unknown) => {}
        Err(error) => return Err(error),
    }
    match task_id_for_session(registry, id_or_session, owner) {
        Ok(task_id) => match resume_ref_for_task(registry, &task_id, owner) {
            Ok(resolved) => return Ok(resolved),
            // The entry may have finished and been pruned between session
            // lookup and snapshot. Durable recovery is appropriate only now
            // that neither live lookup can resolve the task.
            Err(WaitError::Unknown) => {}
            Err(error) => return Err(error),
        },
        Err(WaitError::Unknown) => {}
        Err(error) => return Err(error),
    }

    let direct = subagent_record_for_task(store, id_or_session, owner);
    let record = match direct {
        Ok(record) => record,
        Err(WaitError::NotOwned) => return Err(WaitError::NotOwned),
        Err(WaitError::Unknown) => {
            let task_id = task_id_for_session_in_records(
                super::ledger::list_subagent_records(store),
                id_or_session,
                owner,
            )?;
            subagent_record_for_task(store, &task_id, owner)?
        }
        Err(error) => return Err(error),
    };
    Ok(resume_ref_from_record(
        record.spec.task_id.as_str(),
        &record,
    ))
}

/// Stable metadata returned whether a cancellation was live or recovered from
/// the durable ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledSubagentMetadata {
    /// Stable task id.
    pub task_id: String,
    /// Parent session that owns the run.
    pub parent_session: String,
    /// Worker type.
    pub agent_id: String,
    /// Durable worker session reference, when one was recorded.
    pub subagent_session_id: Option<String>,
}

/// Result of cancellation, including metadata callers need to report or resume
/// cancelled and already-finished runs.
#[derive(Debug)]
pub enum CancelDetachedOutcome {
    /// The live task was cancelled and its durable record was settled.
    Cancelled { metadata: CancelledSubagentMetadata },
    /// The run was already terminal; its durable status remains authoritative.
    AlreadyFinished {
        metadata: CancelledSubagentMetadata,
        outcome: WaitOutcome,
    },
}

/// Cancels a live owned task and mirrors cancellation to the durable ledger;
/// if no live runtime exists, reports its durable terminal state or cancels a
/// still-running durable record.
pub fn cancel_detached_or_durable<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    store: &dyn TaskStore,
    completions: &CompletionRouter,
    task_id: &str,
    owner: &str,
) -> Result<CancelDetachedOutcome, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    match registry.cancel(&TaskId::new(task_id), owner) {
        Ok(cancelled) => {
            if cancelled.status.finished_outcome().is_some() {
                completions
                    .tombstone(task_id)
                    .map_err(|_| WaitError::Unknown)?;
                let record = subagent_record_for_task(store, task_id, owner)?;
                return Ok(CancelDetachedOutcome::AlreadyFinished {
                    metadata: CancelledSubagentMetadata {
                        task_id: task_id.to_string(),
                        parent_session: owner.to_string(),
                        agent_id: cancelled.metadata.agent_id().to_string(),
                        subagent_session_id: cancelled
                            .metadata
                            .subagent_session_id()
                            .map(str::to_owned),
                    },
                    outcome: record_to_wait_outcome(record),
                });
            }
            completions
                .tombstone(task_id)
                .map_err(|_| WaitError::Unknown)?;
            record_cancelled(store, task_id).map_err(|_| WaitError::Unknown)?;
            Ok(CancelDetachedOutcome::Cancelled {
                metadata: CancelledSubagentMetadata {
                    task_id: task_id.to_string(),
                    parent_session: owner.to_string(),
                    agent_id: cancelled.metadata.agent_id().to_string(),
                    subagent_session_id: cancelled
                        .metadata
                        .subagent_session_id()
                        .map(str::to_owned),
                },
            })
        }
        Err(DetachedTaskRegistryError::NotOwned) => Err(WaitError::NotOwned),
        Err(DetachedTaskRegistryError::LockPoisoned) => Err(WaitError::RegistryPoisoned),
        Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::AlreadyDone) => {
            let record = subagent_record_for_task(store, task_id, owner)?;
            if record.is_terminal() {
                completions
                    .tombstone(task_id)
                    .map_err(|_| WaitError::Unknown)?;
                Ok(CancelDetachedOutcome::AlreadyFinished {
                    metadata: durable_cancel_metadata(task_id, owner, &record),
                    outcome: record_to_wait_outcome(record),
                })
            } else {
                completions
                    .tombstone(task_id)
                    .map_err(|_| WaitError::Unknown)?;
                record_cancelled(store, task_id).map_err(|_| WaitError::Unknown)?;
                Ok(CancelDetachedOutcome::Cancelled {
                    metadata: durable_cancel_metadata(task_id, owner, &record),
                })
            }
        }
        Err(_) => Err(WaitError::Unknown),
    }
}

fn durable_cancel_metadata(
    task_id: &str,
    parent_session: &str,
    record: &tinyagents_tasks::OrchestrationTaskRecord,
) -> CancelledSubagentMetadata {
    CancelledSubagentMetadata {
        task_id: task_id.to_string(),
        parent_session: parent_session.to_string(),
        agent_id: record_agent_id(record),
        subagent_session_id: record_subagent_session_id(record).map(ToOwned::to_owned),
    }
}

/// One item returned by [`wait_all_detached`].
#[derive(Debug)]
pub struct WaitAllItem {
    /// Requested task id.
    pub task_id: String,
    /// Wait result or ownership/lookup error.
    pub result: Result<WaitOutcome, WaitError>,
}

/// Waits for several child tasks against one shared deadline. The timeout is
/// therefore a bound for the whole set, not a fresh timeout per child.
pub async fn wait_all_detached<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    store: &dyn TaskStore,
    completions: &CompletionRouter,
    task_ids: &[String],
    owner: &str,
    timeout: Duration,
) -> Vec<WaitAllItem>
where
    M: Clone + Send + Sync + 'static,
{
    let deadline = Instant::now() + timeout;
    let mut results = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result =
            wait_detached_or_durable(registry, store, completions, task_id, owner, remaining).await;
        results.push(WaitAllItem {
            task_id: task_id.clone(),
            result,
        });
        if Instant::now() >= deadline {
            // Later records are still read from the durable ledger with zero
            // wait, so already-settled children remain visible after deadline.
            for task_id in &task_ids[results.len()..] {
                let result = wait_detached_or_durable(
                    registry,
                    store,
                    completions,
                    task_id,
                    owner,
                    Duration::ZERO,
                )
                .await;
                results.push(WaitAllItem {
                    task_id: task_id.clone(),
                    result,
                });
            }
            break;
        }
    }
    results
}
