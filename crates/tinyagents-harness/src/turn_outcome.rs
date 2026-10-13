//! Host-free pieces of a turn's outcome: per-tool-call execution records, the
//! shared sinks middleware write into while the turn runs, and the
//! out-of-output-budget classification of a final response.
//!
//! A host builds its own outcome type (it carries product fields such as cost
//! tallies and routes) from these plus the [`AgentRun`](crate::middleware::AgentRun).

use std::sync::{Arc, Mutex};

use tinyinference_llm::model::ModelResponse;

/// Whether a run's final response is a reply that ran out of output tokens
/// before producing anything: `finish_reason = length`, no visible text and no
/// tool call.
///
/// The model spent its whole output budget reasoning, even after the harness's
/// truncated-empty retries and nudge, so the closing call must say the budget
/// ran out rather than claim the model finished.
#[must_use]
pub fn ended_out_of_output_budget(final_response: Option<&ModelResponse>) -> bool {
    final_response.is_some_and(|response| {
        crate::finish_reason::is_length_stop(response.finish_reason.as_deref())
            && response.message.tool_calls.is_empty()
            && response.text().trim().is_empty()
    })
}

/// One tool call's execution outcome, captured at the tool boundary before the
/// harness folds the result into a message that drops its failure flag.
#[derive(Debug, Clone)]
pub struct ToolCallOutcome {
    /// Provider call id.
    pub call_id: String,
    /// Tool name.
    pub name: String,
    /// The exact structured arguments supplied for this invocation, captured
    /// while the native call is still available so post-commit hooks never
    /// reconstruct arguments from provider prose.
    pub arguments: serde_json::Value,
    /// Mirrors the absence of a tool-result error flag.
    pub success: bool,
    /// The (possibly summarized or capped) result text.
    pub content: String,
    /// Measured wall-clock runtime for this concrete tool invocation.
    pub duration_ms: u64,
}

/// Shared sink a capture middleware appends each tool call's outcome to,
/// drained into the turn outcome.
pub type ToolOutcomeSink = Arc<Mutex<Vec<ToolCallOutcome>>>;

/// Shared slot a repeated-failure breaker writes its root-cause halt summary
/// into when it trips. The turn overrides its final text with it so a
/// no-progress halt surfaces the cause instead of an empty last reply.
pub type HaltSummarySlot = Arc<Mutex<Option<String>>>;

/// A fresh empty [`ToolOutcomeSink`].
#[must_use]
pub fn new_tool_outcome_sink() -> ToolOutcomeSink {
    Arc::default()
}

/// A fresh empty [`HaltSummarySlot`].
#[must_use]
pub fn new_halt_summary_slot() -> HaltSummarySlot {
    Arc::default()
}

/// Drain the sink's outcomes, tolerating a poisoned lock.
#[must_use]
pub fn drain_tool_outcomes(sink: &ToolOutcomeSink) -> Vec<ToolCallOutcome> {
    std::mem::take(
        &mut *sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

/// Take the breaker's halt summary, if it tripped.
#[must_use]
pub fn take_halt_summary(slot: &HaltSummarySlot) -> Option<String> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

#[cfg(test)]
#[path = "turn_outcome_tests.rs"]
mod tests;
