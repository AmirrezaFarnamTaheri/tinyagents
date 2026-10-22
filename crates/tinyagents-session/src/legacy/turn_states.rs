//! Import of classic per-turn snapshot files into a [`TurnStates`] store.
//!
//! The classic layout is
//! `<workspace>/memory/conversations/turn_states/<hex(thread_id)>/<hex(request_id)>.json`,
//! with an older flat form `turn_states/<hex(thread_id)>.json` that holds the
//! thread's latest turn. This reader walks both **without** the classic
//! store's behaviour of migrating the flat form in place: nothing under the
//! source is created, renamed or deleted.
//!
//! A snapshot keeps its thread and request ids. One the destination already
//! holds (same thread, same request) is left as it is, so an edit made after an
//! earlier import survives a re-run. A file that does not parse is counted and
//! skipped rather than blocking the rest, because snapshots are rebuildable
//! progress records and one half-written file must not hold back a migration.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::port::TurnStates;
use crate::turn_state::TurnState;

const LOG_PREFIX: &str = "[session_db:import:turn_states]";
const TURN_STATE_DIR: &str = "turn_states";

/// What a turn-state import did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnStateImportReport {
    /// Snapshot files that parsed.
    pub read: usize,
    /// Snapshots written by this pass.
    pub imported: usize,
    /// Snapshots the destination already had, left untouched.
    pub already_present: usize,
    /// Files that did not parse, skipped.
    pub unreadable: usize,
}

/// The directory a workspace keeps its turn-state snapshots in.
pub fn turn_state_dir(workspace_dir: &Path) -> PathBuf {
    workspace_dir
        .join("memory")
        .join("conversations")
        .join(TURN_STATE_DIR)
}

fn read_snapshot(path: &Path) -> Result<TurnState, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map_err(|e| e.to_string())
}

/// Every snapshot file under `dir`: per-turn files in thread directories and
/// legacy flat files, in a deterministic order.
fn snapshot_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            let mut inner: Vec<_> = std::fs::read_dir(&path)?.collect::<Result<_, _>>()?;
            inner.sort_by_key(|entry| entry.file_name());
            files.extend(
                inner
                    .into_iter()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("json")),
            );
        } else if path.extension().and_then(|e| e.to_str()) == Some("json") {
            files.push(path);
        }
    }
    Ok(files)
}

/// Import the snapshots under `<workspace_dir>/memory/conversations/turn_states/`.
///
/// A workspace with no turn-state directory imports nothing.
///
/// # Errors
///
/// The source directory cannot be listed, or the destination refuses a write.
pub fn import_turn_states(
    workspace_dir: &Path,
    destination: &dyn TurnStates,
) -> anyhow::Result<TurnStateImportReport> {
    let dir = turn_state_dir(workspace_dir);
    let mut report = TurnStateImportReport::default();
    if !dir.is_dir() {
        return Ok(report);
    }
    for path in snapshot_files(&dir)? {
        let state = match read_snapshot(&path) {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    "{LOG_PREFIX} skipping unreadable snapshot {}: {error}",
                    path.display()
                );
                report.unreadable += 1;
                continue;
            }
        };
        report.read += 1;
        let present = destination
            .get_turn(&state.thread_id, &state.request_id)
            .map_err(anyhow::Error::msg)?
            .is_some();
        if present {
            report.already_present += 1;
            continue;
        }
        destination.put(&state).map_err(anyhow::Error::msg)?;
        report.imported += 1;
    }
    tracing::info!(
        "{LOG_PREFIX} read={} imported={} already_present={} unreadable={}",
        report.read,
        report.imported,
        report.already_present,
        report.unreadable
    );
    Ok(report)
}
