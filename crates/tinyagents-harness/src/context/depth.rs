//! Serializable recursion facts scoped across a live run or transport ingress.

use std::future::Future;

use serde::{Deserialize, Serialize};

use super::RunConfig;
use crate::error::Result;

/// Depth and inclusive child-depth ceiling of the active run tree.
///
/// This data-only snapshot can cross a transport boundary without retaining
/// run state, cancellation or stores. The receiving host must still validate
/// untrusted input and apply its own policy ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDepth {
    /// Zero-based depth of the current run; a root run is zero.
    pub depth: usize,
    /// Inclusive maximum child depth permitted by the run's configuration.
    pub max_depth: usize,
}

impl From<&RunConfig> for RunDepth {
    fn from(config: &RunConfig) -> Self {
        Self {
            depth: config.depth(),
            max_depth: config.max_depth(),
        }
    }
}

impl RunDepth {
    /// Derives the next depth through the same guard as `RunConfig::child`.
    ///
    /// # Errors
    ///
    /// Returns `TinyAgentsError::SubAgentDepth` when the next child would
    /// exceed this tree's ceiling.
    pub fn checked_child(self) -> Result<Self> {
        Ok(Self {
            depth: RunConfig::checked_child_depth(self.depth, self.max_depth)?,
            max_depth: self.max_depth,
        })
    }
}

tokio::task_local! {
    static CURRENT_RUN_DEPTH: RunDepth;
}

/// Snapshot of the active run's recursion facts, or `None` outside a run.
///
/// The harness installs this from its actual `RunConfig` around the driver.
/// Transport adapters can read it to stamp an outgoing hop and install a
/// validated incoming value with `with_run_depth`. Independent tasks do not
/// inherit this scope automatically.
#[must_use]
pub fn current_run_depth() -> Option<RunDepth> {
    CURRENT_RUN_DEPTH.try_with(|depth| *depth).ok()
}

/// Polls `future` with explicit recursion facts installed for this task.
///
/// Nested scopes restore their parent, including on cancellation or panic.
/// This sets transport context; it does not replace the run's configuration
/// or independently admit a child. Use `RunDepth::checked_child` for admission.
pub async fn with_run_depth<F: Future>(depth: RunDepth, future: F) -> F::Output {
    CURRENT_RUN_DEPTH.scope(depth, future).await
}
