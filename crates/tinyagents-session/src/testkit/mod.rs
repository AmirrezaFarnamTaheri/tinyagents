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
//! use tinystoragedrivers_testkit::memory_backend;
//! use tinyagents_storage_kit::{Scope, StorageBackend};
//!
//! # tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap().block_on(async {
//! let storage = memory_backend().for_scope(&Scope::new("local").unwrap()).unwrap();
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

/// Legacy-compatible constructor for tests that need a real, six-port memory
/// backend. This adapter is compiled only with the session testkit.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryStorage(std::sync::Arc<tinystoragedrivers_testkit::MemoryBackend>);

#[cfg(test)]
impl MemoryStorage {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
impl tinyagents_storage_kit::StorageBackend for MemoryStorage {
    fn for_scope(
        &self,
        scope: &tinyagents_storage_kit::Scope,
    ) -> tinyagents_storage_kit::error::Result<tinyagents_storage_kit::ScopedStorage> {
        tinyagents_storage_kit::StorageBackend::for_scope(self.0.as_ref(), scope)
    }
}

mod in_memory_transcript;
pub use in_memory_transcript::InMemoryTranscriptHistory;

/// Creates an empty driver-backed transcript locator over in-memory storage.
///
/// Use this when a test needs the same locator interface used by production
/// session stores, but should not touch the filesystem.
#[cfg(test)]
pub fn in_memory_transcript() -> std::sync::Arc<dyn crate::transcript::TranscriptLocator> {
    use crate::port::{DriverSessionStores, SessionStoreProvider};
    use std::sync::Arc;
    use tinystoragedrivers_testkit::memory_backend;
    use tinyagents_storage_kit::{Scope, StorageBackend};

    DriverSessionStores::new(
        Arc::new(memory_backend()),
        Scope::new("local").unwrap(),
    )
    .expect("driver session stores over memory")
    .for_agent("test")
    .transcripts
}

#[cfg(test)]
#[path = "in_memory_transcript_tests.rs"]
mod tests;
