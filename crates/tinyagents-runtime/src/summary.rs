use crate::PostCommitObservation;

/// Builds the content-minimized learning summary from captured execution facts.
pub fn turn_summary<C: Clone + Send + Sync + 'static>(
    observation: &PostCommitObservation<C>,
) -> Option<tinyagents_harness::host::TurnSummary> {
    use sha2::{Digest, Sha256};
    let thread = observation.thread_id.as_deref()?.trim();
    if thread.is_empty() { return None; }
    let usage = observation.usage.as_ref().map_or_else(Default::default, |usage| {
        tinyinference_llm::usage::Usage {
            input_tokens: usage.usage.input,
            output_tokens: usage.usage.output,
            total_tokens: usage.usage.input.saturating_add(usage.usage.output),
            cache_read_tokens: usage.usage.cached_input,
            context_window_tokens: (usage.usage.context_window > 0)
                .then_some(usage.usage.context_window),
            ..Default::default()
        }
    });
    let mut summary = tinyagents_harness::host::TurnSummary::new(
        tinyagents_harness::ids::ThreadId::from(thread),
        observation.agent_id.as_deref()?,
    ).with_text(
        observation.input.text(),
        observation.receipt.outcome.output.clone().unwrap_or_default(),
    ).with_usage(usage);
    for call in &observation.tool_outcomes {
        let bytes = serde_json::to_vec(&call.arguments).ok()?;
        let digest = Sha256::digest(bytes);
        let args_digest = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        summary.record_tool_outcome(tinyagents_harness::host::ToolOutcomeRecord {
            name: call.name.clone(),
            args_digest,
            success: call.success,
            failure_class: (!call.success).then(|| {
                tinyagents_harness::host::classify_tool_failure(&call.content).to_owned()
            }),
            duration_ms: call.duration_ms,
        });
    }
    Some(summary)
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
