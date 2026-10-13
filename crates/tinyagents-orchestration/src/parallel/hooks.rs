//! Host seams for a parallel run: progress, approval and configured
//! concurrency. All defaults are no-ops so a host opts in to only what it needs.

use std::future::Future;

use tinyagents_harness::{CancellationToken, Result};

use super::staging::Worker;
use super::workers::run;

/// Callbacks a host attaches to a parallel run. Methods are synchronous and
/// must not block; they are invoked on the branch's task.
pub trait RunHooks<P, R>: Send + Sync {
    /// Approval gate, consulted just before a worker starts. `Err(reason)`
    /// skips execution and the worker's result is built by the `denied`
    /// closure given to [`run_with_hooks`].
    fn approve(&self, _index: usize, _worker: &Worker<P>) -> std::result::Result<(), String> {
        Ok(())
    }
    /// Progress: a worker is about to execute (after approval).
    fn on_start(&self, _index: usize, _worker: &Worker<P>) {}
    /// Progress: a worker finished (including a denied one).
    fn on_finish(&self, _index: usize, _result: &R) {}
}

/// Clamp a host-configured concurrency to the batch size. `configured == 0`
/// means "one slot per worker"; a non-zero value is never larger than `n`.
pub fn resolve_concurrency(configured: usize, n: usize) -> usize {
    let n = n.max(1);
    if configured == 0 {
        n
    } else {
        configured.min(n)
    }
}

/// [`run`] with host hooks. Results stay in input order. A worker the hook
/// denies is not executed; `denied(worker, reason)` supplies its result.
pub async fn run_with_hooks<P, C, R, B, F, Fut, D>(
    workers: Vec<Worker<P>>,
    configured_concurrency: usize,
    cancel: CancellationToken,
    hooks: &dyn RunHooks<P, R>,
    branch_ctx: B,
    exec: F,
    denied: D,
) -> Result<Vec<R>>
where
    B: Fn(usize) -> C,
    F: Fn(Worker<P>, C) -> Fut,
    Fut: Future<Output = R>,
    D: Fn(Worker<P>, String) -> R,
{
    let concurrency = resolve_concurrency(configured_concurrency, workers.len());
    // `branch_ctx` carries the index; tag it so hooks see it too. Hooks run on
    // the branch's own task, so `on_finish` fires as each worker completes.
    run(
        workers,
        concurrency,
        cancel,
        |i| (i, branch_ctx(i)),
        |w, (idx, c)| {
            let denial = hooks.approve(idx, &w);
            let denied_result = match denial {
                Ok(()) => {
                    hooks.on_start(idx, &w);
                    None
                }
                Err(reason) => {
                    tracing::debug!(index = idx, "[parallel] worker denied by approval hook");
                    Some(denied(w.clone_for_denial(), reason))
                }
            };
            let fut = denied_result.is_none().then(|| exec(w, c));
            async move {
                let r = match (denied_result, fut) {
                    (Some(r), _) => r,
                    (None, Some(fut)) => fut.await,
                    (None, None) => unreachable!("one of denied/exec is always set"),
                };
                hooks.on_finish(idx, &r);
                r
            }
        },
    )
    .await
}
