//! Run-ledger implementation over an explicitly scoped storage handle.
//!
//! Operations whose native atomic contracts are not implemented yet report a
//! typed unsupported error. They never approximate a transaction with weaker
//! individual writes or switch to a local SQLite database.

use tinystoragedrivers_core::{Capability, Scope, ScopedStorage, StorageError};

use chrono::{DateTime, Utc};
use tinyagents_harness::tinyinference_llm::Message;

use super::super::command_center::{AgentWorkRow, CommandCenterView, ControlVerb};
use super::super::ops::RunTransition;
use super::super::port::RunLedgerStore;
use super::super::tool_effects::{ToolEffectRow, ToolEffectSettle, ToolEffectStart};
use super::super::types::*;
use crate::entry_tree::{Branch, Entry, EntryId, EntryKind, Fork};
use crate::retention::RetentionReport;

/// Ledger storage bound to the supplied tenant/agent scope.
///
/// The host supplies an admitted handle. Records and model arguments never
/// select a namespace. This adapter does not open a workspace or local file.
#[derive(Debug, Clone)]
pub struct DriverRunLedger {
    scoped: ScopedStorage,
}

impl DriverRunLedger {
    /// Bind the already admitted scoped ports without opening any local database.
    pub fn new(scoped: ScopedStorage) -> Self {
        Self { scoped }
    }

    /// The immutable namespace this handle was supplied for.
    pub fn scope(&self) -> &Scope {
        self.scoped.scope()
    }
}

fn unsupported(operation: &str) -> super::super::LedgerError {
    StorageError::unsupported(
        Capability::ValidatedBatch,
        format!("run-ledger {operation} requires its native validated transaction implementation"),
    )
    .into()
}

#[async_trait::async_trait]
impl RunLedgerStore for DriverRunLedger {
    /// Implement [`RunLedgerStore::upsert_agent_run`] without a weaker fallback.
    async fn upsert_agent_run(&self, _upsert: AgentRunUpsert) -> Result<AgentRun> {
        Err(unsupported("upsert_agent_run"))
    }

    /// Implement [`RunLedgerStore::upsert_workflow_run`] without a weaker fallback.
    async fn upsert_workflow_run(&self, _upsert: WorkflowRunUpsert) -> Result<WorkflowRun> {
        Err(unsupported("upsert_workflow_run"))
    }

    /// Implement [`RunLedgerStore::try_claim_workflow_run`] without a weaker fallback.
    async fn try_claim_workflow_run(
        &self,
        _id: &str,
        _owner: &str,
        _lease_for: _chrono::Duration,
    ) -> Result<WorkflowLeaseClaim> {
        Err(unsupported("try_claim_workflow_run"))
    }

    /// Implement [`RunLedgerStore::compare_and_swap_workflow_run`] without a weaker fallback.
    async fn compare_and_swap_workflow_run(
        &self,
        _upsert: WorkflowRunUpsert,
        _expected_revision: u64,
        _owner: &str,
        _lease_for: _chrono::Duration,
    ) -> Result<Option<WorkflowRun>> {
        Err(unsupported("compare_and_swap_workflow_run"))
    }

    /// Implement [`RunLedgerStore::renew_workflow_run_lease`] without a weaker fallback.
    async fn renew_workflow_run_lease(
        &self,
        _id: &str,
        _owner: &str,
        _lease_for: _chrono::Duration,
    ) -> Result<bool> {
        Err(unsupported("renew_workflow_run_lease"))
    }

    /// Implement [`RunLedgerStore::compare_and_swap_workflow_run_lifecycle`] without a weaker fallback.
    async fn compare_and_swap_workflow_run_lifecycle(
        &self,
        _upsert: WorkflowRunUpsert,
        _expected_revision: u64,
    ) -> Result<Option<WorkflowRun>> {
        Err(unsupported("compare_and_swap_workflow_run_lifecycle"))
    }

    /// Implement [`RunLedgerStore::append_run_event`] without a weaker fallback.
    async fn append_run_event(&self, _event: RunEventAppend) -> Result<RunEvent> {
        Err(unsupported("append_run_event"))
    }

    /// Implement [`RunLedgerStore::upsert_run_telemetry`] without a weaker fallback.
    async fn upsert_run_telemetry(&self, _upsert: RunTelemetryUpsert) -> Result<RunTelemetry> {
        Err(unsupported("upsert_run_telemetry"))
    }

    /// Implement [`RunLedgerStore::get_agent_run`] without a weaker fallback.
    async fn get_agent_run(&self, _id: &str) -> Result<Option<AgentRun>> {
        Err(unsupported("get_agent_run"))
    }

    /// Implement [`RunLedgerStore::transition_agent_run_status`] without a weaker fallback.
    async fn transition_agent_run_status(
        &self,
        _id: &str,
        _status: AgentRunStatus,
        _error: Option<&str>,
        _completed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<AgentRun>> {
        Err(unsupported("transition_agent_run_status"))
    }

    /// Implement [`RunLedgerStore::transition_agent_run_status_from`] without a weaker fallback.
    async fn transition_agent_run_status_from(
        &self,
        _id: &str,
        _expected: AgentRunStatus,
        _status: AgentRunStatus,
        _error: Option<&str>,
        _completed_at: Option<DateTime<Utc>>,
        _event: Option<RunEventAppend>,
    ) -> Result<RunTransition> {
        Err(unsupported("transition_agent_run_status_from"))
    }

    /// Implement [`RunLedgerStore::interrupt_orphaned_agent_runs`] without a weaker fallback.
    async fn interrupt_orphaned_agent_runs(&self) -> Result<usize> {
        Err(unsupported("interrupt_orphaned_agent_runs"))
    }

    /// Implement [`RunLedgerStore::list_agent_runs`] without a weaker fallback.
    async fn list_agent_runs(
        &self,
        _request: &AgentRunListRequest,
    ) -> Result<AgentRunListResponse> {
        Err(unsupported("list_agent_runs"))
    }

    /// Implement [`RunLedgerStore::list_recent_run_events`] without a weaker fallback.
    async fn list_recent_run_events(
        &self,
        _request: &RunEventListRequest,
    ) -> Result<RunEventListResponse> {
        Err(unsupported("list_recent_run_events"))
    }

    /// Implement [`RunLedgerStore::get_workflow_run`] without a weaker fallback.
    async fn get_workflow_run(&self, _id: &str) -> Result<Option<WorkflowRun>> {
        Err(unsupported("get_workflow_run"))
    }

    /// Implement [`RunLedgerStore::list_workflow_runs`] without a weaker fallback.
    async fn list_workflow_runs(
        &self,
        _request: &WorkflowRunListRequest,
    ) -> Result<WorkflowRunListResponse> {
        Err(unsupported("list_workflow_runs"))
    }

    /// Implement [`RunLedgerStore::upsert_agent_team`] without a weaker fallback.
    async fn upsert_agent_team(&self, _upsert: AgentTeamUpsert) -> Result<AgentTeam> {
        Err(unsupported("upsert_agent_team"))
    }

    /// Implement [`RunLedgerStore::get_agent_team`] without a weaker fallback.
    async fn get_agent_team(&self, _id: &str) -> Result<Option<AgentTeam>> {
        Err(unsupported("get_agent_team"))
    }

    /// Implement [`RunLedgerStore::list_agent_teams`] without a weaker fallback.
    async fn list_agent_teams(
        &self,
        _request: &AgentTeamListRequest,
    ) -> Result<AgentTeamListResponse> {
        Err(unsupported("list_agent_teams"))
    }

    /// Implement [`RunLedgerStore::upsert_agent_team_member`] without a weaker fallback.
    async fn upsert_agent_team_member(
        &self,
        _upsert: AgentTeamMemberUpsert,
    ) -> Result<AgentTeamMember> {
        Err(unsupported("upsert_agent_team_member"))
    }

    /// Implement [`RunLedgerStore::get_agent_team_member`] without a weaker fallback.
    async fn get_agent_team_member(&self, _id: &str) -> Result<Option<AgentTeamMember>> {
        Err(unsupported("get_agent_team_member"))
    }

    /// Implement [`RunLedgerStore::list_agent_team_members`] without a weaker fallback.
    async fn list_agent_team_members(&self, _team_id: &str) -> Result<Vec<AgentTeamMember>> {
        Err(unsupported("list_agent_team_members"))
    }

    /// Implement [`RunLedgerStore::upsert_agent_team_task`] without a weaker fallback.
    async fn upsert_agent_team_task(&self, _upsert: AgentTeamTaskUpsert) -> Result<AgentTeamTask> {
        Err(unsupported("upsert_agent_team_task"))
    }

    /// Implement [`RunLedgerStore::get_agent_team_task`] without a weaker fallback.
    async fn get_agent_team_task(&self, _id: &str) -> Result<Option<AgentTeamTask>> {
        Err(unsupported("get_agent_team_task"))
    }

    /// Implement [`RunLedgerStore::list_agent_team_tasks`] without a weaker fallback.
    async fn list_agent_team_tasks(&self, _team_id: &str) -> Result<Vec<AgentTeamTask>> {
        Err(unsupported("list_agent_team_tasks"))
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

    /// Implement [`RunLedgerStore::mark_agent_team_member_running`] without a weaker fallback.
    async fn mark_agent_team_member_running(
        &self,
        _team_id: &str,
        _member_id: &str,
        _task_id: &str,
        _worker_thread_id: &str,
        _run_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        Err(unsupported("mark_agent_team_member_running"))
    }

    /// Implement [`RunLedgerStore::mark_agent_team_member_idle`] without a weaker fallback.
    async fn mark_agent_team_member_idle(
        &self,
        _team_id: &str,
        _member_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        Err(unsupported("mark_agent_team_member_idle"))
    }

    /// Implement [`RunLedgerStore::release_agent_team_task`] without a weaker fallback.
    async fn release_agent_team_task(&self, _team_id: &str, _task_id: &str) -> Result<bool> {
        Err(unsupported("release_agent_team_task"))
    }

    /// Implement [`RunLedgerStore::record_tool_started`] without a weaker fallback.
    async fn record_tool_started(&self, _start: ToolEffectStart) -> Result<ToolEffectRow> {
        Err(unsupported("record_tool_started"))
    }

    /// Implement [`RunLedgerStore::settle_tool_effect`] without a weaker fallback.
    async fn settle_tool_effect(&self, _settle: ToolEffectSettle) -> Result<Option<ToolEffectRow>> {
        Err(unsupported("settle_tool_effect"))
    }

    /// Implement [`RunLedgerStore::list_unresolved_tool_effects`] without a weaker fallback.
    async fn list_unresolved_tool_effects(&self, _run_id: &str) -> Result<Vec<ToolEffectRow>> {
        Err(unsupported("list_unresolved_tool_effects"))
    }

    /// Implement [`RunLedgerStore::mark_tool_effect_interrupted`] without a weaker fallback.
    async fn mark_tool_effect_interrupted(
        &self,
        _run_id: &str,
        _call_id: &str,
    ) -> Result<Option<ToolEffectRow>> {
        Err(unsupported("mark_tool_effect_interrupted"))
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
