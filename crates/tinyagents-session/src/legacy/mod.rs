//! Read-only, id-preserving import of a classic workspace.
//!
//! Before the storage-port migration a workspace kept three kinds of state in
//! files: a SQLite `session_db/sessions.db` (session history, the run ledger,
//! entry trees), JSONL transcripts under `session_raw/`, and per-turn
//! snapshots as JSON under `memory/conversations/turn_states/`. This module
//! moves all of it into the storage ports.
//!
//! # Guarantees
//!
//! - **Read-only.** A source file is never written, migrated, checkpointed or
//!   repaired. A SQLite source is opened immutable and must be quiescent and
//!   checkpointed ([`QuiescentSource`]).
//! - **Id-preserving.** Session ids, message and tool-call ids, event
//!   sequences, entry ids and ordinals, transcript stems and turn request ids
//!   all keep the value they had.
//! - **Only-if-absent.** A record the target already holds is never
//!   overwritten, so an edit made after an earlier import survives a re-run,
//!   and running the import twice changes nothing the second time.
//! - **Resumable.** The import is not one transaction. A pass that stops
//!   partway is finished by running it again.
//!
//! # Entry points
//!
//! - [`import_workspace`]: everything for one agent, from a workspace root.
//! - [`import_sessions_db`](crate::run_ledger::DriverRunLedger::import_legacy_database):
//!   the database alone, into a ledger handle.
//! - [`transcripts::import_transcripts`] and [`turn_states::import_turn_states`]:
//!   the file stores alone.

pub(crate) mod rows;
pub mod source;
pub mod transcripts;
pub mod turn_states;
mod workspace;

pub use crate::run_ledger::drivers::{ImportCounts, LegacyDatabaseReport};
pub use source::{LegacySourceSnapshot, QuiescentSource, SourceOwner, open_source};
pub use transcripts::{TranscriptImportReport, import_transcripts};
pub use turn_states::{TurnStateImportReport, import_turn_states};
pub use workspace::{LegacyWorkspaceReport, WorkspaceLayout, import_workspace};

#[cfg(test)]
#[path = "files_tests.rs"]
mod files_tests;
#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
