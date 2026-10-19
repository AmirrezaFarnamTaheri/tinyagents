//! Generic child-turn sequencing around host-supplied model, transcript, and
//! cap-summary adapters.
//!
//! The host executes a turn with its selected tool dialect and returns only
//! the accepted typed conversation. This driver owns cap-summary accounting,
//! failure-history persistence, final-message sequencing, and the rule that
//! deferred terminal effects happen only after the transcript commit succeeds.

use async_trait::async_trait;
use tinyagents_harness::cost::{CallCost, CostTally};
use tinyagents_harness::CancellationToken;
use tinyagents_tasks::DeliveryFence;
use tinyinference_llm::usage::UsageTotals;
use std::{future::Future, sync::Arc};

/// Usage and pricing accumulated by one child graph turn.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ChildTurnAccounting {
    /// All provider-reported token fields and call totals.
    pub usage: UsageTotals,
    /// Host-priced child calls, including unknown-cost propagation.
    pub cost: CostTally,
    /// Model calls counted by the host turn. Cap-summary calls are excluded.
    pub model_calls: usize,
}

/// The host's cap-summary result. Pricing stays host-owned; the driver folds
/// the reported call cost and complete usage into the child turn.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildTurnCapSummary {
    /// Final checkpoint or question shown to the parent.
    pub text: String,
    /// Usage reported by the summary model call.
    pub usage: UsageTotals,
    /// Price resolved by the host for that summary call.
    pub cost: CallCost,
}

/// A successfully executed child turn before optional cap summarization.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildTurnRun<Message> {
    /// Accepted transcript history in the host's native typed representation.
    /// Provider-rejected or malformed suffixes must not be included.
    pub accepted_history: Vec<Message>,
    /// Visible turn output before an optional cap summary.
    pub output: String,
    /// Model calls made by the child turn.
    pub model_calls: usize,
    /// Provider usage and host-priced cost from child calls.
    pub accounting: ChildTurnAccounting,
    /// Whether the model-call cap stopped the turn with work pending.
    pub hit_cap: bool,
    /// Whether a tool requested an early exit such as awaiting user input.
    pub early_exit: bool,
}

/// Turn state returned after summary, transcript commit, and deferred effects.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildTurnResult<Message> {
    /// Accepted history, including any final checkpoint/question message.
    pub accepted_history: Vec<Message>,
    /// Final visible output.
    pub output: String,
    /// Folded child and summary accounting.
    pub accounting: ChildTurnAccounting,
    /// Whether the child stopped at its model-call cap.
    pub hit_cap: bool,
    /// Whether the child stopped at an early-exit tool.
    pub early_exit: bool,
}

/// A failed model turn with only the accepted transcript available for recovery.
#[derive(Clone, Debug, PartialEq)]
pub struct ChildTurnFailure<Message, Error> {
    /// Original model, tool, or turn error.
    pub error: Error,
    /// Accepted typed history to persist before returning the error.
    pub accepted_history: Vec<Message>,
    /// Accounting for calls completed before failure.
    pub accounting: ChildTurnAccounting,
}

/// Cancellation and ownership capability retained for the whole child turn.
#[derive(Clone)]
pub struct ChildTurnControl {
    cancellation: CancellationToken,
    fence: Arc<dyn DeliveryFence>,
}

impl ChildTurnControl {
    /// Creates a turn control from the cancellation token and durable fence
    /// acquired by the caller. A turn must never reacquire after fence loss.
    pub fn new(cancellation: CancellationToken, fence: Arc<dyn DeliveryFence>) -> Self {
        Self { cancellation, fence }
    }

    /// Returns the cancellation token passed to host provider/tool work.
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn ensure_admitted<E>(&self) -> Result<(), ChildTurnError<E>> {
        if self.cancellation.is_cancelled() {
            Err(ChildTurnError::Cancelled)
        } else if !self.fence.admitted() {
            self.cancellation.cancel();
            Err(ChildTurnError::LeaseLost)
        } else {
            Ok(())
        }
    }

    async fn race<T, E>(
        &self,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, ChildTurnError<E>> {
        self.ensure_admitted()?;
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(ChildTurnError::Cancelled),
            _ = self.fence.lost() => {
                self.cancellation.cancel();
                Err(ChildTurnError::LeaseLost)
            }
            result = future => {
                self.ensure_admitted()?;
                result.map_err(ChildTurnError::Host)
            }
        }
    }
}

/// Failure from host work or loss of the turn's cancellation/fencing authority.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ChildTurnError<Error> {
    /// A provider, transcript, or deferred host operation failed.
    #[error("child turn host operation failed: {0}")]
    Host(Error),
    /// The parent explicitly cancelled this child turn.
    #[error("child turn cancelled")]
    Cancelled,
    /// The durable thread/turn lease was lost and cannot be reacquired.
    #[error("child turn lease lost")]
    LeaseLost,
}

/// Host-owned ports used by [`drive_child_turn`].
#[async_trait]
pub trait ChildTurnHost: Send {
    /// Typed conversation message representation selected by the host.
    type Message: Clone + Send + Sync;
    /// Host error type covering model, summary, and persistence failures.
    type Error: Send;

    /// Run the provider/model loop and return accepted typed history. On
    /// failure, retain and return the accepted-history snapshot for recovery.
    async fn run_turn(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<ChildTurnRun<Self::Message>, ChildTurnFailure<Self::Message, Self::Error>>;

    /// Produce a resumable checkpoint only when the child hit its cap.
    async fn summarize_cap(
        &mut self,
        run: &ChildTurnRun<Self::Message>,
    ) -> Result<ChildTurnCapSummary, Self::Error>;

    /// Append a final assistant checkpoint or early-exit question using the
    /// selected dialect's typed transcript representation.
    fn append_final_message(
        &mut self,
        history: &mut Vec<Self::Message>,
        text: &str,
    ) -> Result<(), Self::Error>;

    /// Commit successful transcript and metadata before deferred terminal work.
    async fn persist_success(
        &mut self,
        result: &ChildTurnResult<Self::Message>,
    ) -> Result<(), Self::Error>;

    /// Persist accepted partial history before the original turn error returns.
    async fn persist_failure(
        &mut self,
        history: &[Self::Message],
        accounting: &ChildTurnAccounting,
    ) -> Result<(), Self::Error>;

    /// Perform deferred terminal effects after a successful transcript commit.
    async fn after_commit(
        &mut self,
        result: &ChildTurnResult<Self::Message>,
        cancellation: CancellationToken,
    ) -> Result<(), Self::Error>;
}

/// Drives one host-configured child turn while centralizing summary accounting
/// and durable commit ordering.
pub async fn drive_child_turn<H: ChildTurnHost>(
    host: &mut H,
    control: ChildTurnControl,
) -> Result<ChildTurnResult<H::Message>, ChildTurnError<H::Error>> {
    let result = match control
        .race(host.run_turn(control.cancellation()))
        .await?
    {
        Ok(run) => run,
        Err(failure) => {
            control
                .race(host.persist_failure(&failure.accepted_history, &failure.accounting))
                .await?;
            return Err(ChildTurnError::Host(failure.error));
        }
    };
    let mut run = result;
    // This is the one canonical child-turn model-call counter. A cap summary
    // contributes usage and price, but not to this host-defined count.
    run.accounting.model_calls = run.model_calls;

    if run.hit_cap {
        let summary = control.race(host.summarize_cap(&run)).await?;
        run.output = summary.text;
        run.accounting.usage.calls = run
            .accounting
            .usage
            .calls
            .saturating_add(summary.usage.calls);
        run.accounting.usage.usage += summary.usage.usage;
        run.accounting.cost.add(summary.cost);
    }

    if (run.hit_cap || run.early_exit) && !run.output.trim().is_empty() {
        control.ensure_admitted()?;
        host.append_final_message(&mut run.accepted_history, &run.output)?;
    }

    let result = ChildTurnResult {
        accepted_history: run.accepted_history,
        output: run.output,
        accounting: run.accounting,
        hit_cap: run.hit_cap,
        early_exit: run.early_exit,
    };
    control.race(host.persist_success(&result)).await?;
    control.race(host.after_commit(&result, control.cancellation()))
        .await?;
    Ok(result)
}

#[cfg(test)]
#[path = "child_turn_tests.rs"]
mod tests;
