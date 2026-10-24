//! Core types for the file state coordinator.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;

/// Snapshot of a single file read by an agent.
#[derive(Debug, Clone)]
pub struct ReadStamp {
    /// Filesystem mtime at the moment of the read.
    pub mtime: SystemTime,
    /// Monotonic clock timestamp taken just before the read's I/O began, so
    /// any write that lands during the read orders after it.
    pub timestamp: Instant,
    /// Whether the read was partial (paginated / offset+limit).
    pub partial: bool,
}

/// For one path: each agent that wrote it, mapped to the monotonic instant
/// of that agent's latest write.
pub(crate) type PathWriters = HashMap<String, Instant>;

/// Process-global coordinator that tracks file reads and writes across
/// all agents in the process. Thread-safe via `RwLock`.
#[derive(Debug)]
pub struct FileStateCoordinator {
    /// Per-agent, per-resolved-path read stamps.
    /// Key: `(agent_id, canonical_path)`.
    pub(crate) reads: RwLock<HashMap<(String, PathBuf), ReadStamp>>,

    /// Per-resolved-path writers, each with its own latest write instant.
    /// A later write by one agent never erases another agent's entry, so a
    /// staleness check sees every writer, not just the most recent.
    pub(crate) writes: RwLock<HashMap<PathBuf, PathWriters>>,

    /// Per-resolved-path async mutex for serialising read-modify-write
    /// sections (used by `edit` and `apply_patch`).
    pub(crate) path_locks: RwLock<HashMap<PathBuf, Arc<Mutex<()>>>>,
}

impl Default for FileStateCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl FileStateCoordinator {
    #[must_use]
    /// Create an empty coordinator.
    pub fn new() -> Self {
        Self {
            reads: RwLock::new(HashMap::new()),
            writes: RwLock::new(HashMap::new()),
            path_locks: RwLock::new(HashMap::new()),
        }
    }

    /// Return the set of resolved paths that `parent_agent_id` has read
    /// but were subsequently written by any other agent.
    #[must_use]
    pub fn stale_reads_for_parent(&self, parent_agent_id: &str) -> Vec<PathBuf> {
        self.stale_reads(parent_agent_id, |_| true)
    }

    /// Paths `reader` read that some other agent accepted by `counts` wrote
    /// after that read. Sorted.
    pub(crate) fn stale_reads(&self, reader: &str, counts: impl Fn(&str) -> bool) -> Vec<PathBuf> {
        let reads = self.reads.read();
        let writes = self.writes.read();
        let mut stale: Vec<PathBuf> = reads
            .iter()
            .filter(|((agent_id, _), _)| agent_id == reader)
            .filter(|((_, path), read_stamp)| {
                writes.get(path).is_some_and(|writers| {
                    writers_after_read(writers, reader, read_stamp.timestamp)
                        .any(|(writer, _)| counts(writer))
                })
            })
            .map(|((_, path), _)| path.clone())
            .collect();
        stale.sort();
        stale
    }

    /// Collect every path written by each agent in `agent_ids`, keyed by
    /// agent. A path appears under every listed agent that ever wrote it,
    /// even when a different agent wrote it afterwards. Paths are sorted;
    /// agents with no writes are absent.
    #[must_use]
    pub fn paths_written_by(&self, agent_ids: &[String]) -> HashMap<String, Vec<PathBuf>> {
        let writes = self.writes.read();
        let mut result: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for (path, writers) in writes.iter() {
            for agent_id in agent_ids.iter().filter(|id| writers.contains_key(*id)) {
                result
                    .entry(agent_id.clone())
                    .or_default()
                    .push(path.clone());
            }
        }
        for paths in result.values_mut() {
            paths.sort();
        }
        result
    }
}

/// The agents other than `reader` whose latest write to a path came after
/// `read_at`, each with that write's instant.
pub(crate) fn writers_after_read<'a>(
    writers: &'a PathWriters,
    reader: &'a str,
    read_at: Instant,
) -> impl Iterator<Item = (&'a str, Instant)> + 'a {
    writers
        .iter()
        .filter(move |(writer, written_at)| writer.as_str() != reader && **written_at > read_at)
        .map(|(writer, written_at)| (writer.as_str(), *written_at))
}
