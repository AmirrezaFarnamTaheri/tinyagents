//! Destination-side records for host-owned migrations.
//!
//! These types contain decoded rows only. Hosts own source authorization,
//! discovery, decoding, and policy; this crate preserves the supplied records
//! while writing them into the bound ledger scope.

use serde::{Deserialize, Serialize};

use super::tool_effects::ToolEffectRow;
use super::types::{
    AgentRun, AgentTeam, AgentTeamMember, AgentTeamTask, RunEvent, RunTelemetry, WorkflowRun,
};

/// Per-family tally from an idempotent import operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportCounts {
    /// Rows supplied by the host.
    pub read: usize,
    /// Rows written by this call.
    pub imported: usize,
    /// Rows already present and left untouched. Rows that conflict with
    /// different stored data (for example, a session id already used by
    /// another session) are neither imported nor present: they are the
    /// remainder, `read - imported - already_present`.
    pub already_present: usize,
}

impl ImportCounts {
    pub(super) fn new(read: usize, imported: usize) -> Self {
        Self::with_skipped(read, imported, 0)
    }

    /// Counts for a batch where `skipped` rows conflicted and were left out.
    pub(super) fn with_skipped(read: usize, imported: usize, skipped: usize) -> Self {
        Self {
            read,
            imported,
            already_present: read.saturating_sub(imported + skipped),
        }
    }
}

/// Agent-family rows and the stable source identity for one atomic import.
#[derive(Debug, Clone, Default)]
pub struct AgentFamilyImport {
    /// Stable operation key; reusing it with another fingerprint is an error.
    pub operation_id: String,
    /// Stable digest of the source snapshot represented by these rows.
    pub source_fingerprint: String,
    pub runs: Vec<AgentRun>,
    pub events: Vec<RunEvent>,
    pub telemetry: Vec<RunTelemetry>,
}

/// Counts committed with an [`AgentFamilyImport`] receipt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFamilyImportReport {
    pub agent_runs: ImportCounts,
    pub events: ImportCounts,
    pub telemetry: ImportCounts,
}

/// Fully decoded legacy rows outside the receipt-backed agent family.
#[derive(Debug, Clone, Default)]
pub struct LegacyLedgerRows {
    pub workflow_runs: Vec<WorkflowRun>,
    pub teams: Vec<AgentTeam>,
    pub members: Vec<AgentTeamMember>,
    pub tasks: Vec<AgentTeamTask>,
    pub tool_effects: Vec<ToolEffectRow>,
}

/// Per-family counts from a retryable, only-if-absent ledger import.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyLedgerRowsReport {
    pub workflow_runs: ImportCounts,
    pub teams: ImportCounts,
    pub members: ImportCounts,
    pub tasks: ImportCounts,
    pub tool_effects: ImportCounts,
}
