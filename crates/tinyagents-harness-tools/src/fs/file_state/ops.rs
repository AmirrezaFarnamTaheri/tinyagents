//! Operational API for the file state coordinator.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime};
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::types::{FileStateCoordinator, ReadStamp, writers_after_read};

// ── Singleton ────────────────────────────────────────────────────────────

static GLOBAL: OnceLock<Arc<FileStateCoordinator>> = OnceLock::new();

/// Initialise the process-global coordinator when `enabled`. Safe to call
/// multiple times; only the first enabling call wins. Passing `false` leaves the
/// guard off, so [`try_global`] keeps returning `None` and every tracking
/// operation stays a no-op.
pub fn init_global(enabled: bool) {
    if !enabled {
        tracing::debug!("[file_state] guard disabled by host");
        return;
    }
    let _ = GLOBAL.set(Arc::new(FileStateCoordinator::new()));
    tracing::debug!("[file_state] coordinator initialised");
}

/// Returns the global coordinator, or `None` when disabled / not yet initialised.
pub fn try_global() -> Option<Arc<FileStateCoordinator>> {
    GLOBAL.get().cloned()
}

// ── Read tracking ────────────────────────────────────────────────────────

/// Record that `agent_id` read `resolved_path`.
///
/// `read_started` must be captured with [`Instant::now`] *before* the file
/// is opened, not after its contents were read. A sibling write that lands
/// while the read is in flight is then ordered after the read and reported
/// stale by [`check_stale_read`], instead of being silently absorbed.
///
/// ```
/// use std::path::PathBuf;
/// use std::time::{Instant, SystemTime};
/// use tinytools_std::file_state::record_read;
///
/// let path = PathBuf::from("/workspace/notes.txt");
/// let read_started = Instant::now();
/// // ... open and read the file, then stat it for its mtime ...
/// record_read("agent-1", path, SystemTime::now(), false, read_started);
/// ```
pub fn record_read(
    agent_id: &str,
    resolved_path: PathBuf,
    mtime: SystemTime,
    partial: bool,
    read_started: Instant,
) {
    let Some(coord) = try_global() else { return };
    coord.record_read(agent_id, resolved_path, mtime, partial, read_started);
}

impl FileStateCoordinator {
    /// Record a read on this coordinator; [`record_read`] delegates here.
    pub(crate) fn record_read(
        &self,
        agent_id: &str,
        resolved_path: PathBuf,
        mtime: SystemTime,
        partial: bool,
        read_started: Instant,
    ) {
        tracing::trace!(
            agent = agent_id,
            path = %resolved_path.display(),
            partial,
            "[file_state] record_read"
        );
        self.reads.write().insert(
            (agent_id.to_string(), resolved_path),
            ReadStamp {
                mtime,
                timestamp: read_started,
                partial,
            },
        );
    }
}

// ── Write tracking ───────────────────────────────────────────────────────

/// Record that `agent_id` wrote `resolved_path`.
pub fn record_write(agent_id: &str, resolved_path: PathBuf) {
    let Some(coord) = try_global() else { return };
    coord.record_write(agent_id, resolved_path);
}

impl FileStateCoordinator {
    /// Record a write on this coordinator; [`record_write`] delegates here.
    pub(crate) fn record_write(&self, agent_id: &str, resolved_path: PathBuf) {
        tracing::trace!(
            agent = agent_id,
            path = %resolved_path.display(),
            "[file_state] record_write"
        );
        let now = Instant::now();
        self.writes
            .write()
            .entry(resolved_path.clone())
            .or_default()
            .insert(agent_id.to_string(), now);
        // Also update this agent's own read stamp so its own subsequent
        // writes don't trigger self-staleness.
        self.reads.write().insert(
            (agent_id.to_string(), resolved_path),
            ReadStamp {
                mtime: SystemTime::now(),
                timestamp: now,
                partial: false,
            },
        );
    }
}

// ── Staleness checks ─────────────────────────────────────────────────────

/// Check whether `agent_id`'s view of `resolved_path` is stale because
/// another agent wrote to it after this agent's last read. Returns an
/// error message when stale, `None` when safe.
#[must_use]
pub fn check_stale_read(agent_id: &str, resolved_path: &Path) -> Option<String> {
    try_global()?.check_stale_read(agent_id, resolved_path)
}

impl FileStateCoordinator {
    /// [`check_stale_read`] against this coordinator.
    pub(crate) fn check_stale_read(&self, agent_id: &str, resolved_path: &Path) -> Option<String> {
        let reads = self.reads.read();
        let writes = self.writes.read();
        let read_key = (agent_id.to_string(), resolved_path.to_path_buf());
        let read_stamp = reads.get(&read_key)?;
        let path_writers = writes.get(resolved_path)?;
        // Name the most recent of the writers that landed after the read;
        // on a tie, the alphabetically first, so the message is deterministic.
        let (latest, _) = writers_after_read(path_writers, agent_id, read_stamp.timestamp)
            .max_by_key(|&(name, written_at)| (written_at, std::cmp::Reverse(name)))?;
        let display_path = resolved_path.display();
        Some(format!(
            "Stale read: file '{display_path}' was modified by agent '{latest}' after your last \
             read. Re-read the file before editing."
        ))
    }
}

/// Check whether `agent_id`'s last read of `resolved_path` was partial.
/// Returns an error message when partial, `None` when safe.
#[must_use]
pub fn check_partial_read(agent_id: &str, resolved_path: &Path) -> Option<String> {
    let coord = try_global()?;
    let reads = coord.reads.read();
    let read_key = (agent_id.to_string(), resolved_path.to_path_buf());
    let read_stamp = reads.get(&read_key)?;
    if read_stamp.partial {
        let display_path = resolved_path.display();
        Some(format!(
            "Partial read: your last read of '{display_path}' was partial (paginated). \
             Perform a full read before overwriting."
        ))
    } else {
        None
    }
}

// ── Path locking ─────────────────────────────────────────────────────────

/// Acquire an async lock on `resolved_path` for a read-modify-write
/// section. Returns an `OwnedMutexGuard` that releases when dropped.
/// Returns `None` when the coordinator is disabled.
pub async fn acquire_path_lock(resolved_path: &Path) -> Option<OwnedMutexGuard<()>> {
    let coord = try_global()?;
    let mutex = {
        let mut locks = coord.path_locks.write();
        locks
            .entry(resolved_path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    Some(mutex.lock_owned().await)
}

// ── Parent reminder ──────────────────────────────────────────────────────

/// Return resolved paths that `parent_agent_id` had previously read but
/// were subsequently written by any agent in `child_agent_ids`.
#[must_use]
pub fn parent_stale_files(parent_agent_id: &str, child_agent_ids: &[String]) -> Vec<PathBuf> {
    try_global().map_or_else(Vec::new, |coord| {
        coord.parent_stale_files(parent_agent_id, child_agent_ids)
    })
}

impl FileStateCoordinator {
    /// [`parent_stale_files`] against this coordinator.
    pub(crate) fn parent_stale_files(
        &self,
        parent_agent_id: &str,
        child_agent_ids: &[String],
    ) -> Vec<PathBuf> {
        self.stale_reads(parent_agent_id, |writer| {
            child_agent_ids.iter().any(|child| child == writer)
        })
    }
}
