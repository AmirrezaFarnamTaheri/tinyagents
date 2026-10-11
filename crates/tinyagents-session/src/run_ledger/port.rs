//! Object-safe durable run-ledger storage contract.
//!
//! Every method mirrors an existing SQLite operation with its workspace bound
//! to the handle. Return values, partial-update rules, event sequences (starting
//! at one), and claim/lifecycle fencing retain their original meaning.
//! Implementations must preserve transaction boundaries: a status CAS and its
//! optional event commit together, task dependency checks and claims are atomic,
//! and workflow writes validate revision, live owner and lease expiry together.
//!
//! This port does not supply a driver implementation or migrate any data.
//! In particular, retention includes the original session/message tables and
//! must not silently omit them. An implementation that cannot meet a method's
//! contract must return an error instead of using another backend.

use chrono::{DateTime, Utc};
use tinyagents_harness::error::Result;
use tinyagents_harness::tinyinference_llm::Message;

use super::command_center::{AgentWorkRow, CommandCenterView, ControlError, ControlVerb};
use super::ops::RunTransition;
use super::tool_effects::{ToolEffectRow, ToolEffectSettle, ToolEffectStart};
use super::types::*;
use crate::entry_tree::{Branch, Entry, EntryId, EntryKind, Fork};
use crate::retention::RetentionReport;

/// Bound storage for runs, workflows, coordination, effects and entry trees.
///
/// Handles are selected by the host, never by a run's payload or tool arguments.
/// Shared deployments must bind their tenant/agent namespace before returning a
/// handle. The classic orphan sweep assumes exclusive process ownership; shared
/// backends must not interrupt another live owner's work at startup.
///
/// Storage operations are async so driver implementations can await their
/// ports without blocking a caller runtime. Pure recovery classifiers remain
/// synchronous. Callback parameters are shared safely across awaited operations.
#[async_trait::async_trait]
pub trait RunLedgerStore: Send + Sync {
    /// The bound-handle form of [`super::ops::upsert_agent_run`], with the same semantics.
    async fn upsert_agent_run(&self, upsert: AgentRunUpsert) -> Result<AgentRun>;

    /// The bound-handle form of [`super::ops::upsert_workflow_run`], with the same semantics.
    async fn upsert_workflow_run(&self, upsert: WorkflowRunUpsert) -> Result<WorkflowRun>;

    /// The bound-handle form of [`super::ops::try_claim_workflow_run`], with the same semantics.
    async fn try_claim_workflow_run(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<WorkflowLeaseClaim>;

    /// The bound-handle form of [`super::ops::compare_and_swap_workflow_run`], with the same semantics.
    async fn compare_and_swap_workflow_run(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<Option<WorkflowRun>>;

    /// The bound-handle form of [`super::ops::renew_workflow_run_lease`], with the same semantics.
    async fn renew_workflow_run_lease(
        &self,
        id: &str,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<bool>;

    /// The bound-handle form of [`super::ops::compare_and_swap_workflow_run_lifecycle`], with the same semantics.
    async fn compare_and_swap_workflow_run_lifecycle(
        &self,
        upsert: WorkflowRunUpsert,
        expected_revision: u64,
    ) -> Result<Option<WorkflowRun>>;

    /// The bound-handle form of [`super::ops::append_run_event`], with the same semantics.
    async fn append_run_event(&self, event: RunEventAppend) -> Result<RunEvent>;

    /// The bound-handle form of [`super::ops::upsert_run_telemetry`], with the same semantics.
    async fn upsert_run_telemetry(&self, upsert: RunTelemetryUpsert) -> Result<RunTelemetry>;

    /// The bound-handle form of [`super::ops::get_agent_run`], with the same semantics.
    async fn get_agent_run(&self, id: &str) -> Result<Option<AgentRun>>;

    /// The bound-handle form of [`super::ops::transition_agent_run_status`], with the same semantics.
    async fn transition_agent_run_status(
        &self,
        id: &str,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
    ) -> Result<Option<AgentRun>>;

    /// The bound-handle form of [`super::ops::transition_agent_run_status_from`], with the same semantics.
    /// Status change and optional event are one atomic commit; a rejected CAS writes neither.
    async fn transition_agent_run_status_from(
        &self,
        id: &str,
        expected: AgentRunStatus,
        status: AgentRunStatus,
        error: Option<&str>,
        completed_at: Option<DateTime<Utc>>,
        event: Option<RunEventAppend>,
    ) -> Result<RunTransition>;

    /// The bound-handle form of [`super::ops::interrupt_orphaned_agent_runs`], with the same semantics.
    /// Only valid under exclusive process ownership; never sweep live shared owners.
    async fn interrupt_orphaned_agent_runs(&self) -> Result<usize>;

    /// The bound-handle form of [`super::ops::list_agent_runs`], with the same semantics.
    async fn list_agent_runs(&self, request: &AgentRunListRequest) -> Result<AgentRunListResponse>;

    /// The bound-handle form of [`super::ops::list_recent_run_events`], with the same semantics.
    async fn list_recent_run_events(
        &self,
        request: &RunEventListRequest,
    ) -> Result<RunEventListResponse>;

    /// The bound-handle form of [`super::ops::get_workflow_run`], with the same semantics.
    async fn get_workflow_run(&self, id: &str) -> Result<Option<WorkflowRun>>;

    /// The bound-handle form of [`super::ops::list_workflow_runs`], with the same semantics.
    async fn list_workflow_runs(
        &self,
        request: &WorkflowRunListRequest,
    ) -> Result<WorkflowRunListResponse>;

    /// The bound-handle form of [`super::ops::upsert_agent_team`], with the same semantics.
    async fn upsert_agent_team(&self, upsert: AgentTeamUpsert) -> Result<AgentTeam>;

    /// The bound-handle form of [`super::ops::get_agent_team`], with the same semantics.
    async fn get_agent_team(&self, id: &str) -> Result<Option<AgentTeam>>;

    /// The bound-handle form of [`super::ops::list_agent_teams`], with the same semantics.
    async fn list_agent_teams(
        &self,
        request: &AgentTeamListRequest,
    ) -> Result<AgentTeamListResponse>;

    /// The bound-handle form of [`super::ops::upsert_agent_team_member`], with the same semantics.
    async fn upsert_agent_team_member(
        &self,
        upsert: AgentTeamMemberUpsert,
    ) -> Result<AgentTeamMember>;

    /// The bound-handle form of [`super::ops::get_agent_team_member`], with the same semantics.
    async fn get_agent_team_member(&self, id: &str) -> Result<Option<AgentTeamMember>>;

    /// The bound-handle form of [`super::ops::list_agent_team_members`], with the same semantics.
    async fn list_agent_team_members(&self, team_id: &str) -> Result<Vec<AgentTeamMember>>;

    /// The bound-handle form of [`super::ops::upsert_agent_team_task`], with the same semantics.
    async fn upsert_agent_team_task(&self, upsert: AgentTeamTaskUpsert) -> Result<AgentTeamTask>;

    /// The bound-handle form of [`super::ops::get_agent_team_task`], with the same semantics.
    async fn get_agent_team_task(&self, id: &str) -> Result<Option<AgentTeamTask>>;

    /// The bound-handle form of [`super::ops::list_agent_team_tasks`], with the same semantics.
    async fn list_agent_team_tasks(&self, team_id: &str) -> Result<Vec<AgentTeamTask>>;

    /// The bound-handle form of [`super::ops::claim_agent_team_task`], with the same semantics.
    async fn claim_agent_team_task(
        &self,
        team_id: &str,
        task_id: &str,
        member_id: &str,
        claim_token: &str,
    ) -> Result<ClaimOutcome>;

    /// The bound-handle form of [`super::ops::complete_agent_team_task`], with the same semantics.
    async fn complete_agent_team_task(
        &self,
        team_id: &str,
        task_id: &str,
        member_id: &str,
        evidence: &[String],
        require_evidence: bool,
    ) -> Result<CompletionOutcome>;

    /// The bound-handle form of [`super::ops::shutdown_agent_team_member`], with the same semantics.
    async fn shutdown_agent_team_member(
        &self,
        team_id: &str,
        member_id: &str,
    ) -> Result<Option<(AgentTeamMember, Vec<String>)>>;

    /// The bound-handle form of [`super::ops::mark_agent_team_member_running`], with the same semantics.
    async fn mark_agent_team_member_running(
        &self,
        team_id: &str,
        member_id: &str,
        task_id: &str,
        worker_thread_id: &str,
        run_id: &str,
    ) -> Result<Option<AgentTeamMember>>;

    /// The bound-handle form of [`super::ops::mark_agent_team_member_idle`], with the same semantics.
    async fn mark_agent_team_member_idle(
        &self,
        team_id: &str,
        member_id: &str,
    ) -> Result<Option<AgentTeamMember>>;

    /// The bound-handle form of [`super::ops::release_agent_team_task`], with the same semantics.
    async fn release_agent_team_task(&self, team_id: &str, task_id: &str) -> Result<bool>;

    /// The bound-handle form of [`super::tool_effects::record_tool_started`], with the same semantics.
    async fn record_tool_started(&self, start: ToolEffectStart) -> Result<ToolEffectRow>;

    /// The bound-handle form of [`super::tool_effects::settle_tool_effect`], with the same semantics.
    async fn settle_tool_effect(&self, settle: ToolEffectSettle) -> Result<Option<ToolEffectRow>>;

    /// The bound-handle form of [`super::tool_effects::list_unresolved_tool_effects`], with the same semantics.
    async fn list_unresolved_tool_effects(&self, run_id: &str) -> Result<Vec<ToolEffectRow>>;

    /// The bound-handle form of [`super::tool_effects::mark_interrupted`], with the same semantics.
    async fn mark_tool_effect_interrupted(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> Result<Option<ToolEffectRow>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::append`], with the same semantics.
    async fn entry_append(
        &self,
        session_id: &str,
        parent_id: Option<&EntryId>,
        kind: EntryKind,
    ) -> Result<EntryId>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::append_to_head`], with the same semantics.
    async fn entry_append_to_head(&self, session_id: &str, kind: EntryKind) -> Result<EntryId>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::head`], with the same semantics.
    async fn entry_head(&self, session_id: &str) -> Result<Option<EntryId>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::get`], with the same semantics.
    async fn entry_get(&self, session_id: &str, id: &EntryId) -> Result<Option<Entry>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::ancestor_chain`], with the same semantics.
    async fn entry_ancestor_chain(&self, session_id: &str, tip: &EntryId) -> Result<Vec<Entry>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::tips`], with the same semantics.
    async fn entry_tips(&self, session_id: &str) -> Result<Vec<EntryId>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::branches`], with the same semantics.
    async fn entry_branches(&self, session_id: &str) -> Result<Vec<Branch>>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::label`], with the same semantics.
    async fn entry_label(&self, session_id: &str, tip: &EntryId, name: &str) -> Result<EntryId>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::fork`], with the same semantics.
    async fn entry_fork(&self, session_id: &str, tip: &EntryId, fork: Fork) -> Result<EntryId>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::rebuild_index`], with the same semantics.
    async fn entry_rebuild_index(&self, session_id: &str) -> Result<()>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::import_legacy`], with the same semantics.
    async fn entry_import_legacy(&self, session_id: &str, entries: &[Entry]) -> Result<()>;

    /// The bound-handle form of [`crate::entry_tree::EntryTree::build_context`], with the same semantics.
    async fn entry_build_context(&self, session_id: &str, tip: &EntryId) -> Result<Vec<Message>>;

    /// The bound-handle form of [`crate::retention::prune_sessions_before`], with the same semantics.
    async fn prune_sessions_before(&self, older_than: DateTime<Utc>) -> Result<usize>;

    /// The bound-handle form of [`crate::retention::trim_session_messages`], with the same semantics.
    async fn trim_session_messages(&self, session_id: &str, keep_last: usize) -> Result<usize>;

    /// The bound-handle form of [`crate::retention::prune_tool_calls_before`], with the same semantics.
    async fn prune_tool_calls_before(&self, older_than: DateTime<Utc>) -> Result<usize>;

    /// The bound-handle form of [`crate::retention::prune_run_events_before`], with the same semantics.
    async fn prune_run_events_before(&self, older_than: DateTime<Utc>) -> Result<usize>;

    /// The bound-handle form of [`crate::retention::prune_run_telemetry_before`], with the same semantics.
    async fn prune_run_telemetry_before(&self, older_than: DateTime<Utc>) -> Result<usize>;

    /// The bound-handle form of [`crate::retention::apply_retention`], with the same semantics.
    /// Includes classic sessions/messages/tool calls as well as events and telemetry.
    async fn apply_retention(&self, older_than: DateTime<Utc>) -> Result<RetentionReport>;

    /// The bound-handle form of [`crate::retention::reindex_fts`], with the same semantics.
    async fn reindex_fts(&self) -> Result<usize>;

    /// The bound-handle form of [`super::command_center::list_agent_work`], with the same semantics.
    async fn list_agent_work(
        &self,
        limit: Option<u32>,
        display_name: &(dyn for<'a> Fn(&'a str) -> Option<String> + Sync),
    ) -> Result<CommandCenterView>;

    /// The bound-handle form of [`super::command_center::apply_control`], with the same semantics.
    async fn apply_control(
        &self,
        run_id: &str,
        verb: ControlVerb,
        message: Option<&str>,
        reason: Option<&str>,
        display_name: &(dyn for<'a> Fn(&'a str) -> Option<String> + Sync),
    ) -> std::result::Result<AgentWorkRow, ControlError>;

    /// Pure recovery classification, preserving the existing missing-effect policy.
    fn classify_recovery_with(
        &self,
        tail: &[super::recovery::DanglingToolCall],
        effects: &[ToolEffectRow],
        missing: super::recovery::MissingEffectRow,
    ) -> Vec<super::recovery::CallRecovery> {
        super::recovery::classify_recovery_with(tail, effects, missing)
    }

    /// Classify recovery under the strict policy that every failed start write aborts execution.
    fn classify_recovery(
        &self,
        tail: &[super::recovery::DanglingToolCall],
        effects: &[ToolEffectRow],
    ) -> Vec<super::recovery::CallRecovery> {
        super::recovery::classify_recovery(tail, effects)
    }

    /// Return the most cautious verdict from an already classified recovery tail.
    fn overall_recovery(
        &self,
        classified: &[super::recovery::CallRecovery],
    ) -> super::recovery::RecoveryClass {
        super::recovery::overall_recovery(classified)
    }
}
