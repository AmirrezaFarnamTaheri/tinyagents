//! Session-persistence test building blocks.
//!
//! This is the session-layer counterpart to the harness
//! [`testkit`](tinyagents_harness::testkit) and the graph
//! [`testkit`](../../tinyagents_graph/testkit/index.html): reusable doubles and
//! **conformance** (contract) suites so a downstream backend author can prove
//! their implementation behaves like the bundled ones, and the bundled ones
//! can be pinned against each other.
//!
//! # Conformance suites
//!
//! See [`conformance`] for the run-ledger and transcript-history contract
//! suites. Call them from a `#[test]` against any implementation:
//!
//! ```rust
//! use std::sync::Arc;
//! use tinyagents_session::run_ledger::DriverRunLedger;
//! use tinyagents_session::testkit::conformance::run_ledger_conformance;
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! # tokio::runtime::Builder::new_multi_thread().build().unwrap().block_on(async {
//! let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
//! run_ledger_conformance(Arc::new(DriverRunLedger::new(storage))).await;
//! # });
//! ```
//!
//! # In-memory transcript double
//!
//! [`InMemoryTranscriptHistory`] implements
//! [`TranscriptHistory`](crate::transcript::TranscriptHistory) purely in
//! memory, so the transcript-history conformance suite (and any test that
//! needs a cheap stand-in for a real `session_raw/*.jsonl` file) does not have
//! to touch a filesystem.

pub mod conformance;

mod in_memory_transcript;
pub use in_memory_transcript::InMemoryTranscriptHistory;

/// Creates an empty driver-backed transcript locator over in-memory storage.
///
/// Use this when a test needs the same locator interface used by production
/// session stores, but should not touch the filesystem.
pub fn in_memory_transcript() -> std::sync::Arc<dyn crate::transcript::TranscriptLocator> {
    crate::transcript::test_locator::driver_locator()
}

#[cfg(test)]
#[path = "in_memory_transcript_tests.rs"]
mod tests;
