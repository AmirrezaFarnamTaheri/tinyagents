use tinyagents_harness::ids::TaskId;
use tinyagents_tasks::{DetachedTaskRegistry, OrchestrationTaskRecord};

use super::ledger::{record_agent_id, record_parent_session, record_subagent_session_id};
use super::types::{
    DetachedSubagentStatus, SubagentIdentity, SubagentResumeRef, SubagentSnapshot, WaitError,
};

/// One owner-scoped live job and its matching durable reusable-session view.
#[derive(Clone, Debug)]
pub struct ParentRosterEntry {
    /// Stable child task identity.
    pub task_id: String,
    /// Child agent identifier.
    pub agent_id: String,
    /// Current live job, when the child is still registered in this process.
    pub live_job: Option<crate::subagent::SubAgentJob>,
    /// Durable resumable-session summary for this task, when present.
    pub session: Option<crate::subagent::DurableSubagentSessionSummary>,
}

/// Builds a stable roster from the actual subagent-job registry and sessions
/// already read through the exact parent-scoped session store. Sessions with
/// another parent thread are excluded; the owner string scopes live jobs.
pub fn parent_roster_for_owner(
    jobs: &crate::subagent::SubAgentJobRegistry,
    owner: &str,
    scope: &crate::subagent::ParentJobScope,
    sessions: &[crate::subagent::DurableSubagentSessionSummary],
) -> Result<Vec<ParentRosterEntry>, crate::subagent::SubAgentJobError> {
    let mut entries = std::collections::BTreeMap::<String, ParentRosterEntry>::new();
    for job in jobs.list_for_parent_scope(owner, scope)? {
        let task_id = job
            .extensions
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(job.id.as_str())
            .to_owned();
        entries.insert(
            task_id.clone(),
            ParentRosterEntry {
                task_id,
                agent_id: job.agent.clone(),
                live_job: Some(job),
                session: None,
            },
        );
    }
    for session in sessions
        .iter()
        .filter(|session| session.parent_thread_id.as_deref() == Some(scope.thread_id.as_str()))
    {
        let Some(task_id) = session.current_task_id.as_deref() else {
            continue;
        };
        let entry = entries
            .entry(task_id.to_owned())
            .or_insert_with(|| ParentRosterEntry {
                task_id: task_id.to_owned(),
                agent_id: session.agent_id.clone(),
                live_job: None,
                session: None,
            });
        entry.session = Some(session.clone());
    }
    let mut entries: Vec<_> = entries.into_values().collect();
    entries.sort_by(|left, right| {
        left.agent_id
            .cmp(&right.agent_id)
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    Ok(entries)
}

/// Applies host wording to the actual owner-filtered live and durable roster.
/// The callback receives stable data, including `AwaitingUser` session state,
/// without TinyAgents imposing product-specific prose.
pub fn format_parent_roster_for_owner(
    jobs: &crate::subagent::SubAgentJobRegistry,
    owner: &str,
    scope: &crate::subagent::ParentJobScope,
    sessions: &[crate::subagent::DurableSubagentSessionSummary],
    format: impl FnOnce(&[ParentRosterEntry]) -> String,
) -> Result<String, crate::subagent::SubAgentJobError> {
    let entries = parent_roster_for_owner(jobs, owner, scope, sessions)?;
    Ok(format(&entries))
}

/// Snapshot the subagents registered under `owner`, with live status, ordered
/// by `agent_id` then `task_id` so a rendered roster is stable across turns.
/// A poisoned registry lock surfaces as [`WaitError::RegistryPoisoned`].
pub fn snapshot_for_owner<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    owner: &str,
) -> Result<Vec<SubagentSnapshot>, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    let mut out: Vec<SubagentSnapshot> = registry
        .snapshots(Some(owner))?
        .into_iter()
        .map(|entry| SubagentSnapshot {
            agent_id: entry.metadata.agent_id().to_string(),
            subagent_session_id: entry.metadata.subagent_session_id().map(str::to_string),
            task_id: entry.task_id.as_str().to_string(),
            status: entry.status.label(),
        })
        .collect();
    out.sort_by(|a, b| {
        a.agent_id
            .cmp(&b.agent_id)
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
    Ok(out)
}

/// Formats an owner's stable roster through host-supplied wording.
///
/// The callback receives only the owner-filtered, deterministically ordered
/// snapshot. TinyAgents supplies data and ordering; hosts own prompt wording.
pub fn format_roster_for_owner<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    owner: &str,
    format: impl FnOnce(&[SubagentSnapshot]) -> String,
) -> Result<String, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    let snapshot = snapshot_for_owner(registry, owner)?;
    Ok(format(&snapshot))
}

/// Resolve a durable session id to the live task id, enforcing ownership. A
/// non-terminal entry wins over a terminal one.
pub fn task_id_for_session<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    subagent_session_id: &str,
    owner: &str,
) -> Result<String, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    let mut saw_unowned = false;
    let mut owned_terminal: Option<String> = None;
    for snapshot in registry
        .snapshots(None)?
        .into_iter()
        .filter(|snapshot| snapshot.metadata.subagent_session_id() == Some(subagent_session_id))
    {
        if snapshot.owner_id != owner {
            saw_unowned = true;
            continue;
        }
        let task_id = snapshot.task_id.as_str().to_string();
        if !snapshot.status.is_terminal() {
            return Ok(task_id);
        }
        owned_terminal.get_or_insert(task_id);
    }
    if let Some(task_id) = owned_terminal {
        return Ok(task_id);
    }
    if saw_unowned {
        return Err(WaitError::NotOwned);
    }
    Err(WaitError::Unknown)
}

/// Resolve a session id against durable `records` (most recently updated
/// first), enforcing parent ownership.
pub fn task_id_for_session_in_records(
    records: Vec<OrchestrationTaskRecord>,
    subagent_session_id: &str,
    parent_session: &str,
) -> Result<String, WaitError> {
    let mut saw_unowned = false;
    let mut matches: Vec<OrchestrationTaskRecord> = records
        .into_iter()
        .filter(|record| record_subagent_session_id(record) == Some(subagent_session_id))
        .collect();
    matches.sort_by_key(|item| std::cmp::Reverse(item.updated_at));
    for record in matches {
        if record_parent_session(&record) != Some(parent_session) {
            saw_unowned = true;
            continue;
        }
        return Ok(record.spec.task_id.as_str().to_string());
    }
    if saw_unowned {
        return Err(WaitError::NotOwned);
    }
    Err(WaitError::Unknown)
}

/// Resume reference for a live task, enforcing ownership.
pub fn resume_ref_for_task<M>(
    registry: &DetachedTaskRegistry<M, DetachedSubagentStatus>,
    task_id: &str,
    owner: &str,
) -> Result<SubagentResumeRef, WaitError>
where
    M: SubagentIdentity + Clone + Send + Sync + 'static,
{
    let snapshot = registry.snapshot(&TaskId::new(task_id), owner)?;
    Ok(SubagentResumeRef {
        task_id: task_id.to_string(),
        agent_id: snapshot.metadata.agent_id().to_string(),
        subagent_session_id: snapshot.metadata.subagent_session_id().map(str::to_string),
    })
}

/// Resume reference recovered from a durable record.
pub fn resume_ref_from_record(
    task_id: &str,
    record: &OrchestrationTaskRecord,
) -> SubagentResumeRef {
    SubagentResumeRef {
        task_id: task_id.to_string(),
        agent_id: record_agent_id(record),
        subagent_session_id: record_subagent_session_id(record).map(ToOwned::to_owned),
    }
}
