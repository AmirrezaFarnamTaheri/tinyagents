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
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use tinystoragedrivers_sqlite::SqliteStorage;
use tinystoragedrivers_types::StorageBackend;

use super::AgentStores;
use super::types::WorkspaceInner;
use super::{DriverSessionStores, WorkspaceStores};
use crate::legacy::{OnceOutcome, import_transcripts_once};
use crate::transcript::TranscriptLocator;

/// Directory under the workspace holding the internal SQLite driver files.
pub const STORE_DIR: &str = "session_store";

type Cache = Mutex<HashMap<(PathBuf, String), Weak<WorkspaceInner>>>;

/// Weak entries: a backend is released once every handle drops, while live
/// handles are still shared.
fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Serialises first opens so two threads never run the legacy import for the
/// same workspace concurrently (one could see the other's half-written
/// transcript as complete).
static OPEN_LOCK: Mutex<()> = Mutex::new(());

fn cached(key: &(PathBuf, String)) -> Option<WorkspaceStores> {
    let mut map = cache().lock().unwrap_or_else(PoisonError::into_inner);
    match map.get(key).and_then(Weak::upgrade) {
        Some(inner) => Some(WorkspaceStores { inner }),
        None => {
            map.remove(key);
            None
        }
    }
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
        if let Some(found) = cached(&key) {
            return Ok(found);
        }
        let _opening = OPEN_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        // Another thread may have finished while this one waited.
        if let Some(found) = cached(&key) {
            return Ok(found);
        }
        let backend: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::open(workspace_dir.join(STORE_DIR))?);
        let provider = Arc::new(DriverSessionStores::new(
            backend,
            tinystoragedrivers_types::Scope::local(),
        )?);
        let stores = provider.try_for_agent(agent_id)?;
        let outcome = import_transcripts_once(&provider, agent_id, workspace_dir)?;
        tracing::debug!(
            target: "tinyagents_session::port::workspace",
            agent_id,
            "[session-store] workspace stores opened"
        );
        // An import that skipped an unreadable file is not complete: do not
        // cache it, so the next open retries once the file is fixed.
        let complete = matches!(
            outcome,
            OnceOutcome::AlreadyImported | OnceOutcome::Imported { marked: true, .. }
        );
        let inner = Arc::new(WorkspaceInner {
            provider,
            stores,
            outcome,
        });
        if complete {
            cache()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key, Arc::downgrade(&inner));
        }
        Ok(Self { inner })
    }

    /// The transcript port.
    pub fn transcripts(&self) -> Arc<dyn TranscriptLocator> {
        Arc::clone(&self.inner.stores.transcripts)
    }

    /// Every store of the agent.
    pub fn stores(&self) -> &AgentStores {
        &self.inner.stores
    }

    /// The provider the stores come from.
    pub fn provider(&self) -> &Arc<DriverSessionStores> {
        &self.inner.provider
    }

    /// What the first open's legacy import did.
    pub fn import_outcome(&self) -> &OnceOutcome {
        &self.inner.outcome
    }
}

#[cfg(test)]
#[path = "workspace_tests.rs"]
mod tests;
