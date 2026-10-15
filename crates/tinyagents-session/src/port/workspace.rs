//! Transcripts for a host that only has a workspace directory.
//!
//! [`WorkspaceStores::open`] replaces `FileTranscriptLocator::new(workspace)`:
//! it opens a SQLite storage backend at `<workspace>/session_store/` (an
//! internal driver location, not part of the public surface), serves the
//! agent's transcripts through [`DriverSessionStores`], and imports the
//! workspace's classic `session_raw/*.jsonl` files once. Callers receive a
//! `TranscriptLocator` port; they never name a transcript file.
//!
//! Handles are cached per `(workspace, agent)` so a call site that used to
//! construct a file locator on every use does not reopen the database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use tinystoragedrivers_core::StorageBackend;
use tinystoragedrivers_sqlite::SqliteStorage;

use super::{AgentStores, DriverSessionStores};
use crate::legacy::{OnceOutcome, import_transcripts_once};
use crate::transcript::TranscriptLocator;

/// Directory under the workspace holding the internal SQLite driver files.
pub const STORE_DIR: &str = "session_store";

/// An agent's driver-backed stores for one workspace.
#[derive(Clone)]
pub struct WorkspaceStores {
    provider: Arc<DriverSessionStores>,
    stores: AgentStores,
    outcome: Arc<OnceOutcome>,
}

impl std::fmt::Debug for WorkspaceStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceStores")
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

type Cache = Mutex<HashMap<(PathBuf, String), WorkspaceStores>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

impl WorkspaceStores {
    /// Opens (or reuses) the stores of `agent_id` for `workspace_dir`, running
    /// the one-time legacy JSONL import on first open.
    ///
    /// # Errors
    ///
    /// The SQLite store cannot be opened, or the import is refused. Nothing is
    /// cached then, so the next call retries.
    pub fn open(workspace_dir: &Path, agent_id: &str) -> anyhow::Result<Self> {
        let key = (workspace_dir.to_path_buf(), agent_id.to_string());
        if let Some(found) = cache()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(found.clone());
        }
        let backend: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::open(workspace_dir.join(STORE_DIR))?);
        let provider = Arc::new(DriverSessionStores::new(backend)?);
        let stores = provider.try_for_agent(agent_id)?;
        let outcome = import_transcripts_once(&provider, agent_id, workspace_dir)?;
        tracing::debug!(
            target: "tinyagents_session::port::workspace",
            agent_id,
            "[session-store] workspace stores opened"
        );
        let opened = Self {
            provider,
            stores,
            outcome: Arc::new(outcome),
        };
        Ok(cache()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_insert(opened)
            .clone())
    }

    /// The transcript port.
    pub fn transcripts(&self) -> Arc<dyn TranscriptLocator> {
        Arc::clone(&self.stores.transcripts)
    }

    /// Every store of the agent.
    pub fn stores(&self) -> &AgentStores {
        &self.stores
    }

    /// The provider the stores come from.
    pub fn provider(&self) -> &Arc<DriverSessionStores> {
        &self.provider
    }

    /// What the first open's legacy import did.
    pub fn import_outcome(&self) -> &OnceOutcome {
        &self.outcome
    }
}

#[cfg(test)]
#[path = "workspace_tests.rs"]
mod tests;
