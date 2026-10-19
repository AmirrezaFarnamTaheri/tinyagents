//! Process-wide file state coordinator for cross-agent staleness detection.
//!
//! Parallel subagents and worker threads share a workspace. Without
//! coordination one worker can read a file, a sibling can edit it, and
//! the first worker can later write based on stale content. This module
//! tracks per-agent read stamps and, for every path, each agent's latest
//! write stamp so that write tools can detect the conflict and return a
//! model-facing error requiring the agent to re-read. A read counts as stale
//! when *any* other agent wrote after it, not only the most recent writer.
//!
//! A read tool captures `Instant::now()` *before* it opens the file and hands
//! that stamp to [`record_read`] afterwards, so a sibling write racing the
//! read's I/O is ordered after the read and still reported stale.
//!
//! The guard is opt-in for the process: the host calls [`init_global`] with
//! `enabled` (typically derived from its own configuration or environment).
//! Until it does, or when it passes `false`, every operation is a no-op.

mod agent_context;
mod ops;
mod types;

pub use agent_context::{current_file_state_agent_id, with_file_state_agent_id};
pub use ops::{
    acquire_path_lock, check_partial_read, check_stale_read, init_global, parent_stale_files,
    record_read, record_write, try_global,
};
pub use types::{FileStateCoordinator, ReadStamp};

#[cfg(test)]
mod test;
