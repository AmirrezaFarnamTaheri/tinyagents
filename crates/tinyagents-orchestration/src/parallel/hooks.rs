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
    // `branch_ctx` carries the index; tag workers so hooks see it too.
    let indexed: Vec<(usize, Worker<P>)> = workers.into_iter().enumerate().collect();
    let out = run_indexed(indexed, concurrency, cancel, branch_ctx, |idx, w, c| {
        let denial = hooks.approve(idx, &w);
        let fut = match denial {
            Ok(()) => {
                hooks.on_start(idx, &w);
                Some(exec(w, c))
            }
            Err(reason) => {
                tracing::debug!(index = idx, "[parallel] worker denied by approval hook");
                let r = denied(w, reason);
                return Either::Ready(idx, r);
            }
        };
        Either::Run(idx, fut.expect("set above"))
    })
    .await?;
    for (i, r) in out.iter().enumerate() {
        hooks.on_finish(i, r);
    }
    Ok(out)
}

enum Either<R, Fut> {
    Ready(usize, R),
    Run(usize, Fut),
}

async fn run_indexed<P, C, R, B, G, Fut>(
    indexed: Vec<(usize, Worker<P>)>,
    concurrency: usize,
    cancel: CancellationToken,
    branch_ctx: B,
    prep: G,
) -> Result<Vec<R>>
where
    B: Fn(usize) -> C,
    G: Fn(usize, Worker<P>, C) -> Either<R, Fut>,
    Fut: Future<Output = R>,
{
    // Reuse `run` over a unit worker list so ordering, serial fallback and
    // cancellation semantics are exactly those of `run`.
    let workers: Vec<Worker<P>> = indexed.into_iter().map(|(_, w)| w).collect();
    run(
        workers,
        concurrency,
        cancel,
        |i| (i, branch_ctx(i)),
        |w, (i, c)| {
            let step = prep(i, w, c);
            async move {
                match step {
                    Either::Ready(_, r) => r,
                    Either::Run(_, fut) => fut.await,
                }
            }
        },
    )
    .await
}
