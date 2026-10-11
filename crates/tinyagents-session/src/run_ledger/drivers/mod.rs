//! Run-ledger implementation over an explicitly scoped storage handle.
//!
//! Operations whose native atomic contracts are not implemented yet report a
//! typed unsupported error. They never approximate a transaction with weaker
//! individual writes or switch to a local SQLite database.

mod agents;
mod events;
mod effects;
mod import;
mod storage;
pub use import::{LegacySourceSnapshot, QuiescentSource, SourceOwner};

use std::sync::Arc;
use storage::{MEMBERS, TASKS, TEAMS, TELEMETRY, WORKFLOWS, eq_nonblank};
use tinystoragedrivers_core::{
    Capability, Filter, Query, Scope, ScopedStorage, Sort, StorageError,
};
use tokio::sync::OnceCell;

use super::LedgerResult as Result;
use chrono::{DateTime, Utc};
use tinyagents_harness::tinyinference_llm::Message;

use super::command_center::{AgentWorkRow, CommandCenterView, ControlVerb};
use super::ops::RunTransition;
use super::port::RunLedgerStore;
use super::tool_effects::{ToolEffectRow, ToolEffectSettle, ToolEffectStart};
use super::types::*;
use crate::entry_tree::{Branch, Entry, EntryId, EntryKind, Fork};
use crate::retention::RetentionReport;

/// Ledger storage bound to the supplied tenant/agent scope.
///
/// The host supplies an admitted handle. Records and model arguments never
/// select a namespace. This adapter does not open a workspace or local file.
#[derive(Debug, Clone)]
pub struct DriverRunLedger {
    scoped: ScopedStorage,
    declared: Arc<OnceCell<()>>,
}

impl DriverRunLedger {
    /// Bind the already admitted scoped ports without opening any local database.
    pub fn new(scoped: ScopedStorage) -> Self {
        Self {
            scoped,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The immutable namespace this handle was supplied for.
    pub fn scope(&self) -> &Scope {
        self.scoped.scope()
    }
}

impl DriverRunLedger {
    /// The timestamp is explicit internally so the expiry boundary is tested without sleeps.
    async fn claim_workflow_at(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
        now: DateTime<Utc>,
    ) -> Result<WorkflowLeaseClaim> {
        self.change(WORKFLOWS, id, |previous: Option<WorkflowRun>| {
            let Some(mut row) = previous else {
                return Ok((None, WorkflowLeaseClaim::Missing));
            };
            if row.lease_owner.is_some()
                && row.lease_owner.as_deref() != Some(owner)
                && row.lease_expires_at.is_some_and(|expires| expires > now)
            {
                return Ok((None, WorkflowLeaseClaim::Busy(row)));
            }
            row.lease_owner = Some(owner.into());
            row.lease_expires_at = Some(now + lease_for);
            row.updated_at = now;
            row.revision = next_revision(row.revision)?;
            Ok((Some(row.clone()), WorkflowLeaseClaim::Acquired(row)))
        })
        .await
        .map_err(Into::into)
    }

    /// Renewal changes the native version, but preserves the workflow lifecycle revision.
    async fn renew_workflow_at(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.change(WORKFLOWS, id, |previous: Option<WorkflowRun>| {
            let Some(mut row) = previous else {
                return Ok((None, false));
            };
            if row.lease_owner.as_deref() != Some(owner)
                || row.lease_expires_at.is_none_or(|expires| expires <= now)
            {
                return Ok((None, false));
            }
            row.lease_expires_at = Some(now + lease_for);
            row.updated_at = now;
            Ok((Some(row), true))
        })
        .await
        .map_err(Into::into)
    }
}

/// Workflow revisions retain the classic signed SQLite range rather than overflowing on corrupt input.
fn next_revision(revision: u64) -> std::result::Result<u64, StorageError> {
    revision
        .checked_add(1)
        .filter(|revision| *revision <= i64::MAX as u64)
        .ok_or_else(|| {
            StorageError::serialization("workflow revision exceeds classic signed range")
        })
}

/// Merge lifecycle fields without rewriting the immutable started-at timestamp.
fn apply_workflow(
    row: &mut WorkflowRun,
    upsert: &WorkflowRunUpsert,
    now: DateTime<Utc>,
) -> std::result::Result<(), StorageError> {
    row.definition_id = upsert.definition_id.clone();
    row.parent_thread_id = upsert
        .parent_thread_id
        .clone()
        .or(row.parent_thread_id.take());
    row.input = upsert.input.clone();
    row.phase_states = upsert.phase_states.clone();
    row.child_run_ids = upsert.child_run_ids.clone();
    row.status = upsert.status;
    row.summary = upsert.summary.clone().or(row.summary.take());
    row.updated_at = now;
    row.completed_at = upsert.completed_at.or(row.completed_at);
    row.revision = next_revision(row.revision)?;
    Ok(())
}

fn unsupported(operation: &str) -> super::LedgerError {
    StorageError::unsupported(
        Capability::ValidatedBatch,
        format!("run-ledger {operation} requires its native validated transaction implementation"),
    )
    .into()
}

#[async_trait::async_trait]
impl RunLedgerStore for DriverRunLedger {
    /// Implement [`RunLedgerStore::upsert_agent_run`] without a weaker fallback.
    async fn upsert_agent_run(&self, upsert: AgentRunUpsert) -> Result<AgentRun> {
        self.upsert_agent(upsert).await
    }

    /// Implement [`RunLedgerStore::upsert_workflow_run`] without a weaker fallback.
    async fn upsert_workflow_run(&self, upsert: WorkflowRunUpsert) -> Result<WorkflowRun> {
        let now = Utc::now();
        self.change(WORKFLOWS, &upsert.id, |previous: Option<WorkflowRun>| {
            let first = previous.is_none();
            let mut row = previous.unwrap_or(WorkflowRun {
                id: upsert.id.clone(),
                definition_id: upsert.definition_id.clone(),
                parent_thread_id: None,
                input: upsert.input.clone(),
                phase_states: upsert.phase_states.clone(),
                child_run_ids: upsert.child_run_ids.clone(),
                status: upsert.status,
                summary: None,
                started_at: upsert.started_at.unwrap_or(now),
                updated_at: now,
                completed_at: None,
                revision: 0,
                lease_owner: None,
                lease_expires_at: None,
            });
            row.definition_id = upsert.definition_id.clone();
            row.parent_thread_id = upsert.parent_thread_id.clone().or(row.parent_thread_id);
            row.input = upsert.input.clone();
            row.phase_states = upsert.phase_states.clone();
            row.child_run_ids = upsert.child_run_ids.clone();
            row.status = upsert.status;
            row.summary = upsert.summary.clone().or(row.summary);
            row.updated_at = now;
            row.completed_at = upsert.completed_at.or(row.completed_at);
            if !first {
                row.revision = next_revision(row.revision)?;
            }
            if row.status.is_terminal() {
                row.lease_owner = None;
                row.lease_expires_at = None;
            }
            Ok((Some(row.clone()), row))
        })
        .await
        .map_err(Into::into)
    }

    async fn try_claim_workflow_run(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<WorkflowLeaseClaim> {
        self.claim_workflow_at(id, owner, lease_for, Utc::now())
            .await
    }

    async fn compare_and_swap_workflow_run(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<Option<WorkflowRun>> {
        let now = Utc::now();
        self.change(WORKFLOWS, &upsert.id, |previous: Option<WorkflowRun>| {
            let Some(mut row) = previous else {
                return Ok((None, None));
            };
            if row.revision != expected_revision
                || row.lease_owner.as_deref() != Some(owner)
                || row.lease_expires_at.is_none_or(|expires| expires <= now)
            {
                return Ok((None, None));
            }
            apply_workflow(&mut row, &upsert, now)?;
            if row.status.is_terminal() {
                row.lease_owner = None;
                row.lease_expires_at = None;
            } else {
                row.lease_expires_at = Some(now + lease_for);
            }
            Ok((Some(row.clone()), Some(row)))
        })
        .await
        .map_err(Into::into)
    }

    async fn renew_workflow_run_lease(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<bool> {
        self.renew_workflow_at(id, owner, lease_for, Utc::now())
            .await
    }

    async fn compare_and_swap_workflow_run_lifecycle(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
    ) -> Result<Option<WorkflowRun>> {
        let now = Utc::now();
        self.change(WORKFLOWS, &upsert.id, |previous: Option<WorkflowRun>| {
            let Some(mut row) = previous else {
                return Ok((None, None));
            };
            if row.revision != expected_revision {
                return Ok((None, None));
            }
            apply_workflow(&mut row, &upsert, now)?;
            row.lease_owner = None;
            row.lease_expires_at = None;
            Ok((Some(row.clone()), Some(row)))
        })
        .await
        .map_err(Into::into)
    }

    /// Implement [`RunLedgerStore::append_run_event`] without a weaker fallback.
    async fn append_run_event(&self, event: RunEventAppend) -> Result<RunEvent> {
        self.commit_event(event).await
    }

    /// Implement [`RunLedgerStore::upsert_run_telemetry`] without a weaker fallback.
    async fn upsert_run_telemetry(&self, upsert: RunTelemetryUpsert) -> Result<RunTelemetry> {
        let now = Utc::now();
        self.merge(
            TELEMETRY,
            &upsert.run_id,
            |previous: Option<RunTelemetry>| {
                let mut row = previous.unwrap_or(RunTelemetry {
                    run_id: upsert.run_id.clone(),
                    input_tokens: 0,
                    output_tokens: 0,
                    cached_input_tokens: 0,
                    cost_usd: 0.0,
                    elapsed_ms: None,
                    tool_count: 0,
                    model: None,
                    provider: None,
                    error: None,
                    updated_at: Some(now),
                });
                row.input_tokens = upsert.input_tokens.unwrap_or(row.input_tokens);
                row.output_tokens = upsert.output_tokens.unwrap_or(row.output_tokens);
                row.cached_input_tokens = upsert
                    .cached_input_tokens
                    .unwrap_or(row.cached_input_tokens);
                row.cost_usd = upsert.cost_usd.unwrap_or(row.cost_usd);
                row.elapsed_ms = upsert.elapsed_ms.or(row.elapsed_ms);
                row.tool_count = upsert.tool_count.unwrap_or(row.tool_count);
                row.model = upsert.model.clone().or(row.model);
                row.provider = upsert.provider.clone().or(row.provider);
                row.error = upsert.error.clone().or(row.error);
                row.updated_at = Some(now);
                row
            },
        )
        .await
        .map_err(Into::into)
    }

    /// Implement [`RunLedgerStore::get_agent_run`] without a weaker fallback.
    async fn get_agent_run(&self, id: &str) -> Result<Option<AgentRun>> {
        self.agent(id).await
    }

    /// Implement [`RunLedgerStore::transition_agent_run_status`] without a weaker fallback.
    async fn transition_agent_run_status(
        &self,
        id: &str,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<AgentRun>> {
        self.transition_agent(id, None, status, error, completed_at, None).await.map(|result| match result {
            RunTransition::Applied(row) | RunTransition::StatusChanged(row) => Some(row),
            RunTransition::NotFound => None,
        })
    }

    /// Implement [`RunLedgerStore::transition_agent_run_status_from`] without a weaker fallback.
    async fn transition_agent_run_status_from(
        &self,
        id: &str,
        expected: AgentRunStatus,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
        event: Option<RunEventAppend>,
    ) -> Result<RunTransition> {
        self.transition_agent(id, Some(expected), status, error, completed_at, event).await
    }

    /// Implement [`RunLedgerStore::interrupt_orphaned_agent_runs`] without a weaker fallback.
    async fn interrupt_orphaned_agent_runs(&self) -> Result<usize> {
        self.interrupt_agents().await
    }

    /// Implement [`RunLedgerStore::list_agent_runs`] without a weaker fallback.
    async fn list_agent_runs(
        &self,
        request: &AgentRunListRequest,
    ) -> Result<AgentRunListResponse> {
        self.agents(request).await
    }

    /// Implement [`RunLedgerStore::list_recent_run_events`] without a weaker fallback.
    async fn list_recent_run_events(
        &self,
        request: &RunEventListRequest,
    ) -> Result<RunEventListResponse> {
        self.events(request).await
    }

    /// Implement [`RunLedgerStore::get_workflow_run`] without a weaker fallback.
    async fn get_workflow_run(&self, id: &str) -> Result<Option<WorkflowRun>> {
        Ok(self
            .read::<WorkflowRun>(WORKFLOWS, id)
            .await?
            .map(|row| row.doc))
    }

    /// Implement [`RunLedgerStore::list_workflow_runs`] without a weaker fallback.
    async fn list_workflow_runs(
        &self,
        request: &WorkflowRunListRequest,
    ) -> Result<WorkflowRunListResponse> {
        let filter = eq_nonblank(
            Filter::default(),
            "record.definitionId",
            request.definition_id.as_deref(),
        );
        let filter = eq_nonblank(filter, "record.status", request.status.as_deref());
        let filter = eq_nonblank(
            filter,
            "record.parentThreadId",
            request.parent_thread_id.as_deref(),
        );
        let (runs, count) = self
            .page(
                WORKFLOWS,
                filter,
                request.limit.unwrap_or(50).min(500),
                request.offset.unwrap_or(0),
            )
            .await?;
        Ok(WorkflowRunListResponse { runs, count })
    }

    async fn upsert_agent_team(&self, upsert: AgentTeamUpsert) -> Result<AgentTeam> {
        let now = Utc::now();
        Ok(self
            .merge(TEAMS, &upsert.id, |previous: Option<AgentTeam>| AgentTeam {
                id: upsert.id.clone(),
                parent_thread_id: upsert.parent_thread_id.clone().or_else(|| {
                    previous
                        .as_ref()
                        .and_then(|row| row.parent_thread_id.clone())
                }),
                lead_agent_id: upsert.lead_agent_id.clone(),
                status: upsert.status,
                summary: upsert
                    .summary
                    .clone()
                    .or_else(|| previous.as_ref().and_then(|row| row.summary.clone())),
                created_at: previous
                    .as_ref()
                    .map(|row| row.created_at)
                    .unwrap_or(upsert.created_at.unwrap_or(now)),
                updated_at: now,
                closed_at: upsert
                    .closed_at
                    .or_else(|| previous.as_ref().and_then(|row| row.closed_at)),
            })
            .await?)
    }

    async fn get_agent_team(&self, id: &str) -> Result<Option<AgentTeam>> {
        Ok(self.read(TEAMS, id).await?.map(|row| row.doc))
    }

    async fn list_agent_teams(
        &self,
        request: &AgentTeamListRequest,
    ) -> Result<AgentTeamListResponse> {
        let filter = eq_nonblank(
            Filter::default(),
            "record.parentThreadId",
            request.parent_thread_id.as_deref(),
        );
        let filter = eq_nonblank(filter, "record.status", request.status.as_deref());
        let (teams, count) = self
            .page(
                TEAMS,
                filter,
                request.limit.unwrap_or(50).min(500),
                request.offset.unwrap_or(0),
            )
            .await?;
        Ok(AgentTeamListResponse { teams, count })
    }

    async fn upsert_agent_team_member(
        &self,
        upsert: AgentTeamMemberUpsert,
    ) -> Result<AgentTeamMember> {
        let now = Utc::now();
        Ok(self
            .merge(MEMBERS, &upsert.id, |previous: Option<AgentTeamMember>| {
                AgentTeamMember {
                    id: upsert.id.clone(),
                    team_id: previous
                        .as_ref()
                        .map(|row| row.team_id.clone())
                        .unwrap_or_else(|| upsert.team_id.clone()),
                    name: upsert.name.clone(),
                    agent_id: upsert
                        .agent_id
                        .clone()
                        .or_else(|| previous.as_ref().and_then(|row| row.agent_id.clone())),
                    member_status: upsert.member_status,
                    current_task_id: upsert.current_task_id.clone().or_else(|| {
                        previous
                            .as_ref()
                            .and_then(|row| row.current_task_id.clone())
                    }),
                    worker_thread_id: upsert.worker_thread_id.clone().or_else(|| {
                        previous
                            .as_ref()
                            .and_then(|row| row.worker_thread_id.clone())
                    }),
                    run_id: upsert
                        .run_id
                        .clone()
                        .or_else(|| previous.as_ref().and_then(|row| row.run_id.clone())),
                    created_at: previous
                        .as_ref()
                        .map(|row| row.created_at)
                        .unwrap_or(upsert.created_at.unwrap_or(now)),
                    updated_at: now,
                }
            })
            .await?)
    }

    async fn get_agent_team_member(&self, id: &str) -> Result<Option<AgentTeamMember>> {
        Ok(self.read(MEMBERS, id).await?.map(|row| row.doc))
    }

    async fn list_agent_team_members(&self, team_id: &str) -> Result<Vec<AgentTeamMember>> {
        Ok(self
            .records(
                MEMBERS,
                Query::filter(Filter::eq("record.teamId", team_id)).sort(Sort::asc("createdSql")),
            )
            .await?)
    }

    async fn upsert_agent_team_task(&self, upsert: AgentTeamTaskUpsert) -> Result<AgentTeamTask> {
        let now = Utc::now();
        Ok(self
            .merge(TASKS, &upsert.id, |previous: Option<AgentTeamTask>| {
                let live = previous
                    .as_ref()
                    .filter(|_| upsert.status == AgentTeamTaskStatus::InProgress);
                AgentTeamTask {
                    id: upsert.id.clone(),
                    team_id: previous
                        .as_ref()
                        .map(|row| row.team_id.clone())
                        .unwrap_or_else(|| upsert.team_id.clone()),
                    title: upsert.title.clone(),
                    objective: upsert
                        .objective
                        .clone()
                        .or_else(|| previous.as_ref().and_then(|row| row.objective.clone())),
                    status: upsert.status,
                    owner_member_id: upsert.owner_member_id.clone().or_else(|| {
                        previous
                            .as_ref()
                            .and_then(|row| row.owner_member_id.clone())
                    }),
                    claimed_by_member_id: live.and_then(|row| row.claimed_by_member_id.clone()),
                    claim_token: live.and_then(|row| row.claim_token.clone()),
                    depends_on: upsert.depends_on.clone(),
                    gate_status: upsert
                        .gate_status
                        .clone()
                        .unwrap_or_else(|| "pending".into()),
                    gate_reason: upsert
                        .gate_reason
                        .clone()
                        .or_else(|| previous.as_ref().and_then(|row| row.gate_reason.clone())),
                    evidence: upsert.evidence.clone(),
                    source_run_id: upsert
                        .source_run_id
                        .clone()
                        .or_else(|| previous.as_ref().and_then(|row| row.source_run_id.clone())),
                    order_index: upsert.order_index,
                    created_at: previous
                        .as_ref()
                        .map(|row| row.created_at)
                        .unwrap_or(upsert.created_at.unwrap_or(now)),
                    updated_at: now,
                }
            })
            .await?)
    }

    async fn get_agent_team_task(&self, id: &str) -> Result<Option<AgentTeamTask>> {
        Ok(self.read(TASKS, id).await?.map(|row| row.doc))
    }

    async fn list_agent_team_tasks(&self, team_id: &str) -> Result<Vec<AgentTeamTask>> {
        Ok(self
            .records(
                TASKS,
                Query::filter(Filter::eq("record.teamId", team_id))
                    .sort(Sort::asc("record.orderIndex"))
                    .sort(Sort::asc("createdSql")),
            )
            .await?)
    }

    /// Implement [`RunLedgerStore::claim_agent_team_task`] without a weaker fallback.
    async fn claim_agent_team_task(
        &self,
        _team_id: &str,
        _task_id: &str,
        _member_id: &str,
        _claim_token: &str,
    ) -> Result<ClaimOutcome> {
        Err(unsupported("claim_agent_team_task"))
    }

    /// Implement [`RunLedgerStore::complete_agent_team_task`] without a weaker fallback.
    async fn complete_agent_team_task(
        &self,
        _team_id: &str,
        _task_id: &str,
        _member_id: &str,
        _evidence: &[String],
        _require_evidence: bool,
    ) -> Result<CompletionOutcome> {
        Err(unsupported("complete_agent_team_task"))
    }

    /// Implement [`RunLedgerStore::shutdown_agent_team_member`] without a weaker fallback.
    async fn shutdown_agent_team_member(
        &self,
        _team_id: &str,
        _member_id: &str,
    ) -> Result<Option<(AgentTeamMember, Vec<String>)>> {
        Err(unsupported("shutdown_agent_team_member"))
    }

    async fn mark_agent_team_member_running(
        &self,
        team_id: &str,
        member_id: &str,
        task_id: &str,
        worker_thread_id: &str,
        run_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        let now = Utc::now();
        self.change(MEMBERS, member_id, |previous: Option<AgentTeamMember>| {
            let Some(mut row) = previous.filter(|row| row.team_id == team_id) else {
                return Ok((None, None));
            };
            row.member_status = AgentTeamMemberStatus::Active;
            row.current_task_id = Some(task_id.into());
            row.worker_thread_id = Some(worker_thread_id.into());
            row.run_id = Some(run_id.into());
            row.updated_at = now;
            Ok((Some(row.clone()), Some(row)))
        })
        .await
        .map_err(Into::into)
    }

    async fn mark_agent_team_member_idle(
        &self,
        team_id: &str,
        member_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        let now = Utc::now();
        self.change(MEMBERS, member_id, |previous: Option<AgentTeamMember>| {
            let Some(mut row) = previous.filter(|row| row.team_id == team_id) else {
                return Ok((None, None));
            };
            row.member_status = AgentTeamMemberStatus::Idle;
            row.current_task_id = None;
            row.updated_at = now;
            Ok((Some(row.clone()), Some(row)))
        })
        .await
        .map_err(Into::into)
    }

    async fn release_agent_team_task(&self, team_id: &str, task_id: &str) -> Result<bool> {
        let now = Utc::now();
        self.change(TASKS, task_id, |previous: Option<AgentTeamTask>| {
            let Some(mut row) = previous.filter(|row| {
                row.team_id == team_id && row.status == AgentTeamTaskStatus::InProgress
            }) else {
                return Ok((None, false));
            };
            row.status = AgentTeamTaskStatus::Todo;
            row.claimed_by_member_id = None;
            row.claim_token = None;
            row.gate_status = "pending".into();
            row.gate_reason = None;
            row.updated_at = now;
            Ok((Some(row), true))
        })
        .await
        .map_err(Into::into)
    }

    async fn record_tool_started(&self, start: ToolEffectStart) -> Result<ToolEffectRow> {
        self.effect_start(start).await
    }

    async fn settle_tool_effect(&self, settle: ToolEffectSettle) -> Result<Option<ToolEffectRow>> {
        self.effect_settle(settle).await
    }

    async fn list_unresolved_tool_effects(&self, run_id: &str) -> Result<Vec<ToolEffectRow>> {
        self.effect_unresolved(run_id).await
    }

    async fn mark_tool_effect_interrupted(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<Option<ToolEffectRow>> {
        self.effect_settle(ToolEffectSettle {
            run_id: run_id.into(),
            call_id: call_id.into(),
            status: super::tool_effects::ToolEffectStatus::Interrupted,
            effect_summary: None,
        })
        .await
    }

    /// Implement [`RunLedgerStore::entry_append`] without a weaker fallback.
    async fn entry_append(
        &self,
        _session_id: &str,
        _parent_id: Option<&EntryId>,
        _kind: EntryKind,
    ) -> Result<EntryId> {
        Err(unsupported("entry_append"))
    }

    /// Implement [`RunLedgerStore::entry_append_to_head`] without a weaker fallback.
    async fn entry_append_to_head(&self, _session_id: &str, _kind: EntryKind) -> Result<EntryId> {
        Err(unsupported("entry_append_to_head"))
    }

    /// Implement [`RunLedgerStore::entry_head`] without a weaker fallback.
    async fn entry_head(&self, _session_id: &str) -> Result<Option<EntryId>> {
        Err(unsupported("entry_head"))
    }

    /// Implement [`RunLedgerStore::entry_get`] without a weaker fallback.
    async fn entry_get(&self, _session_id: &str, _id: &EntryId) -> Result<Option<Entry>> {
        Err(unsupported("entry_get"))
    }

    /// Implement [`RunLedgerStore::entry_ancestor_chain`] without a weaker fallback.
    async fn entry_ancestor_chain(&self, _session_id: &str, _tip: &EntryId) -> Result<Vec<Entry>> {
        Err(unsupported("entry_ancestor_chain"))
    }

    /// Implement [`RunLedgerStore::entry_tips`] without a weaker fallback.
    async fn entry_tips(&self, _session_id: &str) -> Result<Vec<EntryId>> {
        Err(unsupported("entry_tips"))
    }

    /// Implement [`RunLedgerStore::entry_branches`] without a weaker fallback.
    async fn entry_branches(&self, _session_id: &str) -> Result<Vec<Branch>> {
        Err(unsupported("entry_branches"))
    }

    /// Implement [`RunLedgerStore::entry_label`] without a weaker fallback.
    async fn entry_label(&self, _session_id: &str, _tip: &EntryId, _name: &str) -> Result<EntryId> {
        Err(unsupported("entry_label"))
    }

    /// Implement [`RunLedgerStore::entry_fork`] without a weaker fallback.
    async fn entry_fork(&self, _session_id: &str, _tip: &EntryId, _fork: Fork) -> Result<EntryId> {
        Err(unsupported("entry_fork"))
    }

    /// Implement [`RunLedgerStore::entry_rebuild_index`] without a weaker fallback.
    async fn entry_rebuild_index(&self, _session_id: &str) -> Result<()> {
        Err(unsupported("entry_rebuild_index"))
    }

    /// Implement [`RunLedgerStore::entry_import_legacy`] without a weaker fallback.
    async fn entry_import_legacy(&self, _session_id: &str, _entries: &[Entry]) -> Result<()> {
        Err(unsupported("entry_import_legacy"))
    }

    /// Implement [`RunLedgerStore::entry_build_context`] without a weaker fallback.
    async fn entry_build_context(&self, _session_id: &str, _tip: &EntryId) -> Result<Vec<Message>> {
        Err(unsupported("entry_build_context"))
    }

    /// Implement [`RunLedgerStore::prune_sessions_before`] without a weaker fallback.
    async fn prune_sessions_before(&self, _older_than: DateTime<Utc>) -> Result<usize> {
        Err(unsupported("prune_sessions_before"))
    }

    /// Implement [`RunLedgerStore::trim_session_messages`] without a weaker fallback.
    async fn trim_session_messages(&self, _session_id: &str, _keep_last: usize) -> Result<usize> {
        Err(unsupported("trim_session_messages"))
    }

    /// Implement [`RunLedgerStore::prune_tool_calls_before`] without a weaker fallback.
    async fn prune_tool_calls_before(&self, _older_than: DateTime<Utc>) -> Result<usize> {
        Err(unsupported("prune_tool_calls_before"))
    }

    /// Implement [`RunLedgerStore::prune_run_events_before`] without a weaker fallback.
    async fn prune_run_events_before(&self, _older_than: DateTime<Utc>) -> Result<usize> {
        Err(unsupported("prune_run_events_before"))
    }

    /// Implement [`RunLedgerStore::prune_run_telemetry_before`] without a weaker fallback.
    async fn prune_run_telemetry_before(&self, _older_than: DateTime<Utc>) -> Result<usize> {
        Err(unsupported("prune_run_telemetry_before"))
    }

    /// Implement [`RunLedgerStore::apply_retention`] without a weaker fallback.
    async fn apply_retention(&self, _older_than: DateTime<Utc>) -> Result<RetentionReport> {
        Err(unsupported("apply_retention"))
    }

    /// Implement [`RunLedgerStore::reindex_fts`] without a weaker fallback.
    async fn reindex_fts(&self) -> Result<usize> {
        Err(unsupported("reindex_fts"))
    }

    /// Implement [`RunLedgerStore::list_agent_work`] without a weaker fallback.
    async fn list_agent_work(
        &self,
        _limit: Option<u32>,
        _display_name: &(dyn for<'a> Fn(&'a str) -> Option<String> + Sync),
    ) -> Result<CommandCenterView> {
        Err(unsupported("list_agent_work"))
    }

    /// Implement [`RunLedgerStore::apply_control`] without a weaker fallback.
    async fn apply_control(
        &self,
        _run_id: &str,
        _verb: ControlVerb,
        _message: Option<&str>,
        _reason: Option<&str>,
        _display_name: &(dyn for<'a> Fn(&'a str) -> Option<String> + Sync),
    ) -> Result<AgentWorkRow> {
        Err(unsupported("apply_control"))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "agent_tests.rs"]
mod agent_tests;
