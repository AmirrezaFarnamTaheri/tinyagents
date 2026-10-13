//! The contract behind a tool-output summary stage.
//!
//! When a tool returns a huge payload, a compression stage decides whether it
//! is worth summarizing, writes the extraction prompt, caches the result and
//! keeps the original recoverable. What it cannot do is call a model: that is
//! the host's, and it must run under the turn that made the tool call so it
//! inherits the turn's model, cancellation, thread and lineage.
//!
//! [`PayloadSummarizer`] is that seam. The host's output stage asks it to
//! [`prepare`](PayloadSummarizer::prepare) a call bound to the current turn and
//! hands the prepared call to the compression stage, which runs it only if it
//! decides to summarize. The prepared-call type is the host's (it names the
//! compression stage's wire types), so it is an associated type here and this
//! crate stays free of that dependency. Model choice stays with the host.

use crate::context::{RunConfig, RunContext};

/// Supplies the model call behind a payload summary stage.
pub trait PayloadSummarizer<Ctx: Send + Sync>: Send + Sync {
    /// The bound, not-yet-run model call the compression stage drives.
    type Prepared;
    /// Failure to bind a call (for example the turn carries no parent context).
    type Error;

    /// Bind one model call to `parent_ctx`'s turn. Called before the result is
    /// handed to the compression stage; the call runs only if it asks.
    fn prepare(&self, parent_ctx: &RunContext<Ctx>) -> Result<Self::Prepared, Self::Error>;
}

/// Run config for the unary child a summary call runs in: `max_iterations`
/// model calls, eight tool calls per model call (at least eight), the parent's
/// depth ceiling, and a per-turn output cap. Pair it with
/// [`RunContext::child`], which links cancellation to the parent.
#[must_use]
pub fn summary_child_config(
    parent_ctx: &RunContext<impl Send + Sync>,
    agent_id: &str,
    max_iterations: usize,
    max_output_tokens: u32,
) -> RunConfig {
    RunConfig::new(format!("{agent_id}-summary"))
        .with_max_model_calls(max_iterations)
        .with_max_tool_calls(max_iterations.saturating_mul(8).max(8))
        .with_max_depth(parent_ctx.config.max_depth())
        .with_max_turn_output_tokens(max_output_tokens)
}
