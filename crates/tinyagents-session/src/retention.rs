//! What a retention pass removed.
//!
//! Retention itself is a set of operations on the run ledger
//! ([`RunLedgerStore::prune_sessions_before`](crate::run_ledger::RunLedgerStore::prune_sessions_before),
//! `trim_session_messages`, `prune_tool_calls_before`, `prune_run_events_before`,
//! `prune_run_telemetry_before`, `apply_retention` and `reindex_fts`), because
//! the records they bound live wherever the host opened its storage. Every one
//! takes an explicit bound (`older_than` / `keep_last`) and returns the number
//! of rows removed; nothing runs on a schedule or on its own, because retention
//! is a policy decision the host makes.
//!
//! # Atomicity
//!
//! Each batch a pass applies is atomic and every pass is idempotent. A pass
//! over a large history is split into batches the backend can take, so it is
//! not one transaction: children are deleted before the record that owns them,
//! so a pass that stops partway never leaves a parent claiming vanished
//! children, and running it again finishes the work.

/// What a single retention pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetentionReport {
    /// Sessions deleted (their messages and tool calls cascade).
    pub sessions: usize,
    /// Messages deleted directly (not via a session cascade).
    pub messages: usize,
    /// Tool calls deleted directly (not via a session cascade).
    pub tool_calls: usize,
    /// Run-ledger events deleted.
    pub run_events: usize,
    /// Run-telemetry rows deleted.
    pub run_telemetry: usize,
}

impl RetentionReport {
    /// Total rows removed across every table.
    pub fn total(&self) -> usize {
        self.sessions + self.messages + self.tool_calls + self.run_events + self.run_telemetry
    }
}
