//! The bundle of stores one agent persists through.

use std::sync::Arc;

use tinyagents_harness::store::{AppendStore, Store};

use super::TurnStates;
use crate::transcript::TranscriptLocator;

/// Every store one agent uses, as handed out by
/// [`SessionStoreProvider::for_agent`](super::SessionStoreProvider::for_agent).
///
/// Cloning is cheap: each field is a shared handle.
#[derive(Clone)]
pub struct AgentStores {
    /// The agent's conversation transcripts: resume reads, turn commits and
    /// compaction generations.
    pub transcripts: Arc<dyn TranscriptLocator>,
    /// Snapshots of the agent's in-flight turns.
    pub turn_states: Arc<dyn TurnStates>,
    /// Namespaced key-value records: run status, goals, todos, session
    /// descriptors.
    pub kv: Arc<dyn Store>,
    /// Append-only event streams: each run's journal.
    pub journal: Arc<dyn AppendStore>,
}

/// A set of agent stores whose handles retain the backend admitted at open.
#[derive(Clone)]
pub struct SessionStoreLease {
    stores: AgentStores,
}

impl SessionStoreLease {
    /// The stores pinned by this lease.
    pub fn stores(&self) -> &AgentStores {
        &self.stores
    }

    pub(crate) fn pinned(stores: AgentStores) -> Self {
        Self { stores }
    }
}

/// Why a provider could not open an explicitly pinned set of session stores.
#[derive(Debug, thiserror::Error)]
pub enum SessionStoreLeaseError {
    /// The provider cannot guarantee that its handles retain one backend.
    #[error("provider cannot guarantee pinned session stores")]
    Unsupported,
    /// Opening the pinned stores failed.
    #[error("opening pinned session stores failed: {0}")]
    Open(#[source] tinystoragedrivers_core::StorageError),
}

impl std::fmt::Debug for AgentStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentStores")
            .field("transcripts", &self.transcripts.destination_key())
            .finish_non_exhaustive()
    }
}

/// An agent's driver-backed stores for one workspace.
///
/// Cloning shares one open backend; the backend closes when the last clone
/// drops.
#[derive(Clone)]
#[cfg(feature = "legacy-import")]
pub struct WorkspaceStores {
    pub(super) inner: Arc<WorkspaceInner>,
}

#[cfg(feature = "legacy-import")]
pub(super) struct WorkspaceInner {
    pub(super) provider: Arc<super::DriverSessionStores>,
    pub(super) stores: AgentStores,
    pub(super) outcome: crate::legacy::OnceOutcome,
}

#[cfg(feature = "legacy-import")]
impl std::fmt::Debug for WorkspaceStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceStores")
            .field("outcome", &self.inner.outcome)
            .finish_non_exhaustive()
    }
}
