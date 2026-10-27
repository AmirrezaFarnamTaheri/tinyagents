//! Host-free finalisation of a completed harness run: decide how the turn
//! ended (cap, in-loop wrap-up, circuit-breaker halt, truncation), choose the
//! turn's final text, and emit the audit logs for compaction and the frozen
//! prompt prefix.
//!
//! The host keeps what is product-shaped: the durable journal stamp, the
//! terminal progress event, cost accounting and its own outcome type.

use tinyinference_llm::message::Message;

use crate::middleware::AgentRun;
use crate::summarization::SummaryRecord;
use crate::turn_outcome::ended_out_of_output_budget;
use crate::turn_stop::TurnStop;

/// What the host knows about the run beyond the [`AgentRun`] itself.
#[derive(Debug, Clone)]
pub struct TurnEndInputs<'a> {
    /// The model-call cap the run was configured with.
    pub max_iterations: usize,
    /// Whether the cap pauses gracefully (the session path) rather than
    /// failing the run.
    pub pause_at_cap: bool,
    /// An early-exit tool fired: a clean pause that takes precedence over a
    /// cap.
    pub early_exit: bool,
    /// The repeated-failure breaker's root-cause summary, when it tripped.
    pub breaker_halt: Option<&'a str>,
    /// An in-loop final-call wrap-up concluded the capped turn.
    pub wrap_up_injected: bool,
}

/// How a successfully returned run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnEnd {
    /// The run stopped at the model-call cap with work still pending.
    pub hit_cap: bool,
    /// The run ended on a reply that ran out of output tokens while reasoning.
    pub truncated: bool,
}

/// Whether the run stopped at its model-call cap.
///
/// The harness sets `final_response` only when the loop finishes naturally, so
/// a cap pauser that stops the loop mid-work leaves it `None`. An early exit is
/// a clean pause and a breaker halt is its own cause; both win over the cap.
/// The in-loop wrap-up is the primary tell: the turn reached its cap *and*
/// answered from inside the loop, so `final_response` is `Some` and the
/// `model_calls >= max_iterations` arm cannot see it. That arm still covers a
/// run without the wrap-up, and one whose concluding call itself requested a
/// tool.
#[must_use]
pub fn detect_hit_cap(run: &AgentRun, inputs: &TurnEndInputs<'_>) -> bool {
    detect_hit_cap_with_cause(run, inputs, None)
}

/// Typed-cause-aware form of [`detect_hit_cap`]. A classified breaker cause
/// takes precedence over the model-call cap even if no note was rendered.
#[must_use]
pub fn detect_hit_cap_with_cause(
    run: &AgentRun,
    inputs: &TurnEndInputs<'_>,
    failure_cause: Option<&crate::no_progress::ClassifiedStopCause>,
) -> bool {
    inputs.pause_at_cap
        && !inputs.early_exit
        && inputs.breaker_halt.is_none()
        && failure_cause.is_none()
        && (inputs.wrap_up_injected
            || (run.model_calls >= inputs.max_iterations && run.final_response.is_none()))
}

/// Classify how `run` ended.
#[must_use]
pub fn classify_turn_end(run: &AgentRun, inputs: &TurnEndInputs<'_>) -> TurnEnd {
    TurnEnd {
        hit_cap: detect_hit_cap(run, inputs),
        truncated: ended_out_of_output_budget(run.final_response.as_ref()),
    }
}

/// The content-free [`TurnStop`] for a turn that ended early, if it did. There
/// is no deadline wind-down on this path: only a session turn carries one, and
/// it records the stop through its own sidecar.
#[must_use]
pub fn turn_stop_for(
    breaker_halt: Option<&str>,
    wind_down: bool,
    hit_cap: bool,
    fallback_class: impl FnOnce(&str) -> &'static str,
) -> Option<TurnStop> {
    TurnStop::classify(breaker_halt, wind_down, hit_cap, fallback_class)
}

/// Content-free stop for a turn, preferring the classifier-owned cause over
/// any human-readable breaker note.
#[must_use]
pub fn turn_stop_for_with_cause(
    breaker_halt: Option<&str>,
    failure_cause: Option<crate::no_progress::ClassifiedStopCause>,
    wind_down: bool,
    hit_cap: bool,
    fallback_class: impl FnOnce(&str) -> &'static str,
) -> Option<TurnStop> {
    TurnStop::classify_with_cause(
        breaker_halt,
        failure_cause,
        wind_down,
        hit_cap,
        fallback_class,
    )
}

/// The turn's final text. An early-exit tool's captured question stands in for
/// the paused assistant turn (which carries the tool call, not an answer); a
/// breaker halt then overrides either so callers with no breaker awareness
/// still surface the no-progress cause rather than an empty last reply.
#[must_use]
pub fn select_turn_text(
    run: &AgentRun,
    early_exit_question: Option<String>,
    breaker_halt: Option<&str>,
) -> String {
    if let Some(summary) = breaker_halt {
        return summary.to_owned();
    }
    match early_exit_question {
        Some(question) => question,
        None => run.text().unwrap_or_default(),
    }
}

/// Byte length of each leading system message: the frozen prompt prefix, per
/// tier segment. A working prefix cache shows `cached_input_tokens` rising to
/// roughly this on the second turn.
#[must_use]
pub fn system_segment_bytes(messages: &[Message]) -> Vec<usize> {
    messages
        .iter()
        .take_while(|message| matches!(message, Message::System(_)))
        .map(|message| message.text().len())
        .collect()
}

/// Log each compaction the run performed with its full provenance (source ids,
/// before and after token estimates, policy reason), so every compaction is
/// auditable and not only its net token saving. Greppable via `[context]`.
pub fn log_compaction_provenance(model: &str, records: &[SummaryRecord]) {
    if records.is_empty() {
        return;
    }
    tracing::info!(
        model,
        compactions = records.len(),
        "[context] turn performed {} context compaction(s); surfacing provenance",
        records.len()
    );
    for (idx, record) in records.iter().enumerate() {
        let provenance = &record.provenance;
        tracing::info!(
            model,
            compaction = idx + 1,
            of = records.len(),
            source_count = provenance.source_ids.len(),
            source_ids = ?provenance.source_ids,
            from_tokens = provenance.original_token_estimate,
            to_tokens = provenance.summary_token_estimate,
            saved_tokens = provenance
                .original_token_estimate
                .saturating_sub(provenance.summary_token_estimate),
            reason = %provenance.reason,
            "[context] compaction provenance: folded {} source message(s) ({} -> {} tokens)",
            provenance.source_ids.len(),
            provenance.original_token_estimate,
            provenance.summary_token_estimate,
        );
    }
}

#[cfg(test)]
#[path = "turn_run_finalize_tests.rs"]
mod tests;
