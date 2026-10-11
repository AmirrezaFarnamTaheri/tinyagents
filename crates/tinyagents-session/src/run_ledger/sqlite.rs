//! Classic SQLite adapter for the storage-independent run-ledger contract.
//!
//! Delegates to existing operations so SQL migrations, immediate transactions,
//! ownership fences and read-back behavior remain unchanged. Construction does
//! not open or migrate a database; the first operation uses the existing shared
//! session database helper. No table or transcript is moved by this adapter.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use tinyagents_harness::error::Result;
use tinyagents_harness::tinyinference_llm::Message;

use super::command_center::{AgentWorkRow, CommandCenterView, ControlError, ControlVerb};
use super::ops::RunTransition;
use super::port::RunLedgerStore;
use super::tool_effects::{ToolEffectRow, ToolEffectSettle, ToolEffectStart};
use super::types::*;
use crate::entry_tree::{Branch, Entry, EntryId, EntryKind, Fork};
use crate::retention::RetentionReport;

/// A ledger bound to one classic workspace's shared session database.
///
/// Cheap to clone: connections are still cached by `crate::store`. This is a
/// desktop/local adapter, not a namespace-isolated shared-cloud implementation.
/// Its async methods execute the existing short SQLite transaction on the polling
/// thread; they do not introduce an async client or a blocking-runtime bridge.
#[derive(Debug, Clone)]
pub struct SqliteRunLedger {
    workspace_dir: PathBuf,
}

impl SqliteRunLedger {
    /// Bind a handle to the existing database below `workspace_dir`.
    pub fn new(workspace_dir: impl Into<PathBuf>) -> Self {
        Self {
            workspace_dir: workspace_dir.into(),
        }
    }
}

#[async_trait::async_trait]
impl RunLedgerStore for SqliteRunLedger {
    async fn upsert_agent_run(&self, upsert: AgentRunUpsert) -> Result<AgentRun> {
        super::ops::upsert_agent_run(&self.workspace_dir, upsert)
    }

    async fn upsert_workflow_run(&self, upsert: WorkflowRunUpsert) -> Result<WorkflowRun> {
        super::ops::upsert_workflow_run(&self.workspace_dir, upsert)
    }

    async fn try_claim_workflow_run(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<WorkflowLeaseClaim> {
        super::ops::try_claim_workflow_run(&self.workspace_dir, id, owner, lease_for)
    }

    async fn compare_and_swap_workflow_run(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<Option<WorkflowRun>> {
        super::ops::compare_and_swap_workflow_run(
            &self.workspace_dir,
            upsert,
            expected_revision,
            owner,
            lease_for,
        )
    }

    async fn renew_workflow_run_lease(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<bool> {
        super::ops::renew_workflow_run_lease(&self.workspace_dir, id, owner, lease_for)
    }

    async fn compare_and_swap_workflow_run_lifecycle(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
    ) -> Result<Option<WorkflowRun>> {
        super::ops::compare_and_swap_workflow_run_lifecycle(
            &self.workspace_dir,
            upsert,
            expected_revision,
        )
    }

    async fn append_run_event(&self, event: RunEventAppend) -> Result<RunEvent> {
        super::ops::append_run_event(&self.workspace_dir, event)
    }

    async fn upsert_run_telemetry(&self, upsert: RunTelemetryUpsert) -> Result<RunTelemetry> {
        super::ops::upsert_run_telemetry(&self.workspace_dir, upsert)
    }

    async fn get_agent_run(&self, id: &str) -> Result<Option<AgentRun>> {
        super::ops::get_agent_run(&self.workspace_dir, id)
    }

    async fn transition_agent_run_status(
        &self,
        id: &str,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<AgentRun>> {
        super::ops::transition_agent_run_status(
            &self.workspace_dir,
            id,
            status,
            error,
            completed_at,
        )
    }

    async fn transition_agent_run_status_from(
        &self,
        id: &str,
        expected: AgentRunStatus,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
        event: Option<RunEventAppend>,
    ) -> Result<RunTransition> {
        super::ops::transition_agent_run_status_from(
            &self.workspace_dir,
            id,
            expected,
            status,
            error,
            completed_at,
            event,
        )
    }

    async fn interrupt_orphaned_agent_runs(&self) -> Result<usize> {
        super::ops::interrupt_orphaned_agent_runs(&self.workspace_dir)
    }

    async fn list_agent_runs(&self, request: &AgentRunListRequest) -> Result<AgentRunListResponse> {
        super::ops::list_agent_runs(&self.workspace_dir, request)
    }

    async fn list_recent_run_events(
        &self,
        request: &RunEventListRequest,
    ) -> Result<RunEventListResponse> {
        super::ops::list_recent_run_events(&self.workspace_dir, request)
    }

    async fn get_workflow_run(&self, id: &str) -> Result<Option<WorkflowRun>> {
        super::ops::get_workflow_run(&self.workspace_dir, id)
    }

    async fn list_workflow_runs(
        &self,
        request: &WorkflowRunListRequest,
    ) -> Result<WorkflowRunListResponse> {
        super::ops::list_workflow_runs(&self.workspace_dir, request)
    }

    async fn upsert_agent_team(&self, upsert: AgentTeamUpsert) -> Result<AgentTeam> {
        super::ops::upsert_agent_team(&self.workspace_dir, upsert)
    }

    async fn get_agent_team(&self, id: &str) -> Result<Option<AgentTeam>> {
        super::ops::get_agent_team(&self.workspace_dir, id)
    }

    async fn list_agent_teams(
        &self,
        request: &AgentTeamListRequest,
    ) -> Result<AgentTeamListResponse> {
        super::ops::list_agent_teams(&self.workspace_dir, request)
    }

    async fn upsert_agent_team_member(
        &self,
        upsert: AgentTeamMemberUpsert,
    ) -> Result<AgentTeamMember> {
        super::ops::upsert_agent_team_member(&self.workspace_dir, upsert)
    }

    async fn get_agent_team_member(&self, id: &str) -> Result<Option<AgentTeamMember>> {
        super::ops::get_agent_team_member(&self.workspace_dir, id)
    }

    async fn list_agent_team_members(&self, team_id: &str) -> Result<Vec<AgentTeamMember>> {
        super::ops::list_agent_team_members(&self.workspace_dir, team_id)
    }

    async fn upsert_agent_team_task(&self, upsert: AgentTeamTaskUpsert) -> Result<AgentTeamTask> {
        super::ops::upsert_agent_team_task(&self.workspace_dir, upsert)
    }

    async fn get_agent_team_task(&self, id: &str) -> Result<Option<AgentTeamTask>> {
        super::ops::get_agent_team_task(&self.workspace_dir, id)
    }

    async fn list_agent_team_tasks(&self, team_id: &str) -> Result<Vec<AgentTeamTask>> {
        super::ops::list_agent_team_tasks(&self.workspace_dir, team_id)
    }

    async fn claim_agent_team_task(
        &self,
        team_id: &str,
        task_id: &str,
        member_id: &str,
        claim_token: &str,
    ) -> Result<ClaimOutcome> {
        super::ops::claim_agent_team_task(
            &self.workspace_dir,
            team_id,
            task_id,
            member_id,
            claim_token,
        )
    }

    async fn complete_agent_team_task(
        &self,
        team_id: &str,
        task_id: &str,
        member_id: &str,
        evidence: &[String],
        require_evidence: bool,
    ) -> Result<CompletionOutcome> {
        super::ops::complete_agent_team_task(
            &self.workspace_dir,
            team_id,
            task_id,
            member_id,
            evidence,
            require_evidence,
        )
    }

    async fn shutdown_agent_team_member(
        &self,
        team_id: &str,
        member_id: &str,
    ) -> Result<Option<(AgentTeamMember, Vec<String>)>> {
        super::ops::shutdown_agent_team_member(&self.workspace_dir, team_id, member_id)
    }

    async fn mark_agent_team_member_running(
        &self,
        team_id: &str,
        member_id: &str,
        task_id: &str,
        worker_thread_id: &str,
        run_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        super::ops::mark_agent_team_member_running(
            &self.workspace_dir,
            team_id,
            member_id,
            task_id,
            worker_thread_id,
            run_id,
        )
    }

    async fn mark_agent_team_member_idle(
        &self,
        team_id: &str,
        member_id: &str,
    ) -> Result<Option<AgentTeamMember>> {
        super::ops::mark_agent_team_member_idle(&self.workspace_dir, team_id, member_id)
    }

    async fn release_agent_team_task(&self, team_id: &str, task_id: &str) -> Result<bool> {
        super::ops::release_agent_team_task(&self.workspace_dir, team_id, task_id)
    }

    async fn record_tool_started(&self, start: ToolEffectStart) -> Result<ToolEffectRow> {
        super::tool_effects::record_tool_started(&self.workspace_dir, start)
    }

    async fn settle_tool_effect(&self, settle: ToolEffectSettle) -> Result<Option<ToolEffectRow>> {
        super::tool_effects::settle_tool_effect(&self.workspace_dir, settle)
    }

    async fn list_unresolved_tool_effects(&self, run_id: &str) -> Result<Vec<ToolEffectRow>> {
        super::tool_effects::list_unresolved_tool_effects(&self.workspace_dir, run_id)
    }

    async fn mark_tool_effect_interrupted(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<Option<ToolEffectRow>> {
        super::tool_effects::mark_interrupted(&self.workspace_dir, run_id, call_id)
    }

    async fn entry_append(
        &self,
        session_id: &str,
        parent_id: Option<&EntryId>,
        kind: EntryKind,
    ) -> Result<EntryId> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).append(parent_id, kind)
    }

    async fn entry_append_to_head(&self, session_id: &str, kind: EntryKind) -> Result<EntryId> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).append_to_head(kind)
    }

    async fn entry_head(&self, session_id: &str) -> Result<Option<EntryId>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).head()
    }

    async fn entry_get(&self, session_id: &str, id: &EntryId) -> Result<Option<Entry>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).get(id)
    }

    async fn entry_ancestor_chain(&self, session_id: &str, tip: &EntryId) -> Result<Vec<Entry>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).ancestor_chain(tip)
    }

    async fn entry_tips(&self, session_id: &str) -> Result<Vec<EntryId>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).tips()
    }

    async fn entry_branches(&self, session_id: &str) -> Result<Vec<Branch>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).branches()
    }

    async fn entry_label(&self, session_id: &str, tip: &EntryId, name: &str) -> Result<EntryId> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).label(tip, name)
    }

    async fn entry_fork(&self, session_id: &str, tip: &EntryId, fork: Fork) -> Result<EntryId> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).fork(tip, fork)
    }

    async fn entry_rebuild_index(&self, session_id: &str) -> Result<()> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).rebuild_index()
    }

    async fn entry_import_legacy(&self, session_id: &str, entries: &[Entry]) -> Result<()> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).import_legacy(entries)
    }

    async fn entry_build_context(&self, session_id: &str, tip: &EntryId) -> Result<Vec<Message>> {
        crate::entry_tree::EntryTree::new(&self.workspace_dir, session_id).build_context(tip)
    }

    async fn prune_sessions_before(&self, older_than: DateTime<Utc>) -> Result<usize> {
        crate::retention::prune_sessions_before(&self.workspace_dir, older_than)
    }

    async fn trim_session_messages(&self, session_id: &str, keep_last: usize) -> Result<usize> {
        crate::retention::trim_session_messages(&self.workspace_dir, session_id, keep_last)
    }

    async fn prune_tool_calls_before(&self, older_than: DateTime<Utc>) -> Result<usize> {
        crate::retention::prune_tool_calls_before(&self.workspace_dir, older_than)
    }

    async fn prune_run_events_before(&self, older_than: DateTime<Utc>) -> Result<usize> {
        crate::retention::prune_run_events_before(&self.workspace_dir, older_than)
    }

    async fn prune_run_telemetry_before(&self, older_than: DateTime<Utc>) -> Result<usize> {
        crate::retention::prune_run_telemetry_before(&self.workspace_dir, older_than)
    }

    async fn apply_retention(&self, older_than: DateTime<Utc>) -> Result<RetentionReport> {
        crate::retention::apply_retention(&self.workspace_dir, older_than)
    }

    async fn reindex_fts(&self) -> Result<usize> {
        crate::retention::reindex_fts(&self.workspace_dir)
    }

    async fn list_agent_work(
        &self,
        limit: Option<u32>,
        display_name: &(dyn Fn(&str) -> Option<String> + Sync),
    ) -> Result<CommandCenterView> {
        super::command_center::list_agent_work(&self.workspace_dir, limit, display_name)
    }

    async fn apply_control(
        &self,
        run_id: &str,
        verb: ControlVerb,
        message: Option<&str>,
        reason: Option<&str>,
        display_name: &(dyn Fn(&str) -> Option<String> + Sync),
    ) -> std::result::Result<AgentWorkRow, ControlError> {
        super::command_center::apply_control(
            &self.workspace_dir,
            run_id,
            verb,
            message,
            reason,
            display_name,
        )
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
