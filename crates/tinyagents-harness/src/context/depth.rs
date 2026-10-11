//! Serializable recursion facts scoped across a live run or transport ingress.

use std::future::Future;

use super::RunDepth;

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
