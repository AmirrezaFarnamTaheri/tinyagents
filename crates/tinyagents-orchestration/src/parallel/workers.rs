//! Running staged workers: serial fallback for shared-workspace writers,
//! bounded `map_reduce` fan-out otherwise.

use std::future::Future;

use tinyagents_graph::parallel::{map_reduce, FailurePolicy, ParallelOptions};
use tinyagents_harness::{CancellationToken, Result, TinyAgentsError};

use super::staging::{Worker, WorkerDispatchMode};

/// Run `workers` and return one result per worker, in input order.
///
/// `branch_ctx` is called once per worker to build that branch's own context
/// (per-branch isolation: no state is shared between branches unless the host
/// puts it in the context deliberately). If any worker is
/// [`WorkerDispatchMode::SerialSharedWorkspaceWrite`] the whole batch runs one
/// at a time, checking `cancel` before each; otherwise up to `max_concurrency`
/// (0 = one slot per worker) run at once. Cancellation surfaces as
/// [`TinyAgentsError::Cancelled`].
pub async fn run<P, C, R, B, F, Fut>(
    workers: Vec<Worker<P>>,
    max_concurrency: usize,
    cancel: CancellationToken,
    branch_ctx: B,
    exec: F,
) -> Result<Vec<R>>
where
    B: Fn(usize) -> C,
    F: Fn(Worker<P>, C) -> Fut,
    Fut: Future<Output = R>,
{
    let n = workers.len();
    let serial = workers
        .iter()
        .any(|w| w.dispatch_mode == WorkerDispatchMode::SerialSharedWorkspaceWrite);
    if serial {
        tracing::debug!(
            workers = n,
            "[parallel] serial fallback for shared-workspace writers"
        );
        let mut results = Vec::with_capacity(n);
        for (i, worker) in workers.into_iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(TinyAgentsError::Cancelled);
            }
            results.push(exec(worker, branch_ctx(i)).await);
        }
        return Ok(results);
    }

    let concurrency = if max_concurrency == 0 {
        n.max(1)
    } else {
        max_concurrency
    };
    tracing::debug!(workers = n, concurrency, "[parallel] bounded fan-out");
    let options = ParallelOptions::default()
        .with_max_concurrency(concurrency)
        .with_failure_policy(FailurePolicy::CollectAll)
        .with_cancellation(cancel);
    let outcome = map_reduce(workers, options, |i, worker| {
        let fut = exec(worker, branch_ctx(i));
        async move { Ok(fut.await) }
    })
    .await?;

    let mut results = Vec::with_capacity(n);
    for item in outcome.outcomes {
        match item.result {
            Ok(v) => results.push(v),
            Err(err) => {
                return Err(TinyAgentsError::Graph(format!(
                    "parallel fan-out: worker {} failed: {err}",
                    item.index
                )))
            }
        }
    }
    if results.len() != n {
        return Err(TinyAgentsError::Graph(format!(
            "parallel fan-out: expected {n} result(s), got {}",
            results.len()
        )));
    }
    Ok(results)
}
