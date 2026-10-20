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
//! use tinyagents_storage_kit::{Scope, ScopedStorage};
//!
//! # tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap().block_on(async {
//! let backend = memory_backend();
//! let scope = Scope::new("test-ledger").unwrap();
//! let storage = ScopedStorage::new(backend.for_scope(&scope)?, scope);
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

/// Makes a test-only host factory from explicit agent-to-scope bindings.
///
/// Scope labels in this map are arbitrary test values; this helper does not
/// encode or validate product identity conventions.
#[cfg(test)]
pub(crate) fn mapped_storage_factory(
    backend: std::sync::Arc<dyn tinystoragedrivers_api::StorageBackend>,
    scopes: std::collections::HashMap<String, tinyagents_storage_kit::Scope>,
) -> crate::port::AgentStorageFactory {
    std::sync::Arc::new(move |agent_id| {
        let scope = scopes.get(agent_id).cloned().ok_or_else(|| {
            tinyagents_storage_kit::StorageError::invalid_input("test agent scope is not mapped")
        })?;
        let storage = backend.for_scope(&scope)?;
        Ok(tinyagents_storage_kit::ScopedStorage::new(storage, scope))
    })
}

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

    pub(crate) fn for_scope(
        &self,
        scope: &tinyagents_storage_kit::Scope,
    ) -> tinyagents_storage_kit::error::Result<tinyagents_storage_kit::ScopedStorage> {
        let storage = tinystoragedrivers_api::StorageBackend::for_scope(self, scope)?;
        Ok(tinyagents_storage_kit::ScopedStorage::new(storage, scope.clone()))
    }
}

#[cfg(test)]
impl tinystoragedrivers_api::StorageBackend for MemoryStorage {
    fn capabilities(&self) -> tinyagents_storage_kit::Capabilities {
        tinystoragedrivers_api::StorageBackend::capabilities(self.0.as_ref())
    }

    fn for_scope(
        &self,
        scope: &tinyagents_storage_kit::Scope,
    ) -> tinyagents_storage_kit::error::Result<tinystoragedrivers_api::Storage> {
        tinystoragedrivers_api::StorageBackend::for_scope(self.0.as_ref(), scope)
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
    use tinyagents_storage_kit::Scope;

    let scope = Scope::new("test-transcript").unwrap();
    let factory = mapped_storage_factory(
        Arc::new(memory_backend()),
        [("test".to_owned(), scope)].into_iter().collect(),
    );
    DriverSessionStores::new(factory)
    .expect("driver session stores over memory")
    .for_agent("test")
    .transcripts
}

#[cfg(test)]
#[path = "in_memory_transcript_tests.rs"]
mod tests;
