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

use std::sync::Arc;

use tinyinference_llm::message::Message;
use tinyinference_llm::model::ChatModel;

use crate::context::{RunConfig, RunContext};
use crate::runtime::{AgentHarness, InvalidArgsPolicy, RunPolicy, UnknownToolPolicy};

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

/// Run one unary summary call as the already-derived `child_context` (see
/// [`summary_child_config`] and [`RunContext::child`]) and return its text.
///
/// The harness is built with a single attempt and tool/argument errors
/// returned to the model, `model` registered under `model_name`, and the
/// run pinned to the unary path: a streaming child would put its deltas on the
/// shared event sink as the parent's text. `max_model_calls` bounds the loop
/// (tool calls are capped at eight per model call, at least eight).
///
/// The child runs on `AgentHarness<(), Ctx>`: the state is `()` because the
/// summary run is its own harness, and `child_context` must have been derived
/// for that pairing.
///
/// # Errors
///
/// The child run's error, or a validation error when it produced no text (an
/// empty summary must not replace the original payload).
pub async fn run_unary_summary<Ctx: Send + Sync + 'static>(
    child_context: RunContext<Ctx>,
    model_name: &str,
    model: Arc<dyn ChatModel<()>>,
    max_model_calls: usize,
    system: String,
    prompt: String,
) -> crate::error::Result<String> {
    let mut policy = RunPolicy::default();
    policy.limits.max_model_calls = max_model_calls;
    policy.limits.max_tool_calls = max_model_calls.saturating_mul(8).max(8);
    policy.retry.max_attempts = 1;
    policy.unknown_tool = UnknownToolPolicy::ReturnToolError;
    policy.invalid_args = InvalidArgsPolicy::ReturnToolError;

    let mut harness: AgentHarness<(), Ctx> = AgentHarness::new();
    harness.with_policy(policy);
    harness
        .register_model(model_name, model)
        .set_default_model(model_name);
    let run = harness
        .invoke_in_context(
            &(),
            child_context,
            vec![Message::system(system), Message::user(prompt)],
        )
        .await?;
    match run.text() {
        Some(text) if !text.trim().is_empty() => Ok(text),
        _ => Err(crate::error::TinyAgentsError::Validation(
            "summary run produced no text".into(),
        )),
    }
}

#[cfg(test)]
#[path = "payload_summarizer_tests.rs"]
mod tests;
