//! Generic child-turn sequencing around host-supplied model, transcript, and
//! cap-summary adapters.
//!
//! The host executes a turn with its selected tool dialect and returns only
//! the accepted typed conversation. This driver owns cap-summary accounting,
//! failure-history persistence, final-message sequencing, and the rule that
//! deferred terminal effects happen only after the transcript commit succeeds.

use async_trait::async_trait;
use tinyagents_harness::cost::{CallCost, CostTally};
use tinyinference_llm::usage::UsageTotals;

/// Usage and pricing accumulated by one child graph turn.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ChildTurnAccounting {
    /// All provider-reported token fields and call totals.
    pub usage: UsageTotals,
    /// Host-priced child calls, including unknown-cost propagation.
    pub cost: CostTally,
    /// Model calls counted by the turn driver, including cap summarization.
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
    /// Calls made while producing this summary.
    pub model_calls: usize,
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
    ) -> Result<(), Self::Error>;
}

/// Drives one host-configured child turn while centralizing summary accounting
/// and durable commit ordering.
pub async fn drive_child_turn<H: ChildTurnHost>(
    host: &mut H,
) -> Result<ChildTurnResult<H::Message>, H::Error> {
    let mut run = match host.run_turn().await {
        Ok(run) => run,
        Err(failure) => {
            host.persist_failure(&failure.accepted_history, &failure.accounting)
                .await?;
            return Err(failure.error);
        }
    };

    if run.hit_cap {
        let summary = host.summarize_cap(&run).await?;
        run.output = summary.text;
        run.accounting.usage.calls = run.accounting.usage.calls.saturating_add(summary.usage.calls);
        run.accounting.usage.usage += summary.usage.usage;
        run.accounting.cost.add(summary.cost);
        run.accounting.model_calls = run
            .accounting
            .model_calls
            .saturating_add(summary.model_calls);
    }

    if (run.hit_cap || run.early_exit) && !run.output.trim().is_empty() {
        host.append_final_message(&mut run.accepted_history, &run.output)?;
    }

    let result = ChildTurnResult {
        accepted_history: run.accepted_history,
        output: run.output,
        accounting: run.accounting,
        hit_cap: run.hit_cap,
        early_exit: run.early_exit,
    };
    host.persist_success(&result).await?;
    host.after_commit(&result).await?;
    Ok(result)
}

#[cfg(test)]
#[path = "child_turn_tests.rs"]
mod tests;
