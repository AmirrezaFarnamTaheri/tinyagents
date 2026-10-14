//! The tool-output ladder's driver: plan the stages, run the host's
//! compaction, bound the result with the shared cap (spilling the full body to
//! an artifact), and prefix the disclosure last.
//!
//! The host supplies only what is its own through [`OutputProcessor`]: the
//! content-aware compaction (a summary model, a compactor) and what to do once
//! a body has been persisted (index it, emit an event). Ordering, exemptions
//! and the persistence call live here, so every host gets the same ladder.

use async_trait::async_trait;

use super::output_cap::{OutputCapPolicy, disclose, plan_output_stages, should_compact};
use crate::artifacts::tool_results::{
    ToolResultArtifactOutcome, ToolResultArtifactStore, apply_per_result_persistence,
};

/// What the compaction stage hands back.
#[derive(Clone, Debug, Default)]
pub struct CompactedOutput {
    /// The compacted text (the input, when nothing was compacted).
    pub text: String,
    /// Byte count a summary replaced, stated in the closing disclosure.
    pub summarized_from_bytes: Option<usize>,
    /// A "summary unavailable" notice, prefixed after every cap has run.
    pub notice: Option<String>,
}

/// The host's side of the ladder.
#[async_trait]
pub trait OutputProcessor: Send {
    /// Compact `content` (summary, content-aware compaction). Called only when
    /// the plan allows it; `focus` is a caller question only a summary answers.
    async fn compact(
        &mut self,
        tool: &str,
        content: String,
        focus: Option<String>,
    ) -> CompactedOutput;

    /// A body was spilled to an artifact; index it, announce it.
    async fn persisted(
        &mut self,
        _tool: &str,
        _call_id: &str,
        _outcome: &ToolResultArtifactOutcome,
    ) {
    }
}

/// One result going through the ladder.
pub struct OutputLadderInput<'a> {
    /// Tool name.
    pub tool: &'a str,
    /// Call id, recorded on a persisted artifact.
    pub call_id: &'a str,
    /// The model-facing text the tool returned.
    pub content: String,
    /// A caller question that overrides a tool's declared cap for compaction.
    pub focus: Option<String>,
    /// The host wants compaction for this call (feature on, not raw, ...).
    pub wants_compaction: bool,
    /// Where an oversized body spills; `None` truncates inline.
    pub store: Option<&'a ToolResultArtifactStore>,
    /// Label in the "summary of N bytes" disclosure.
    pub summary_label: &'a str,
}

/// Run the ladder: compact (when planned), bound with the cap, disclose.
pub async fn run_output_ladder(
    policy: &dyn OutputCapPolicy,
    processor: &mut dyn OutputProcessor,
    input: OutputLadderInput<'_>,
) -> String {
    let OutputLadderInput {
        tool,
        call_id,
        mut content,
        focus,
        wants_compaction,
        store,
        summary_label,
    } = input;
    let plan = plan_output_stages(policy, tool, content.len(), store.is_some());
    let full_output = plan.keep_full_output.then(|| content.clone());

    let mut notice = None;
    let mut summarized_from_bytes = None;
    if should_compact(&plan, focus.is_some(), wants_compaction) {
        let compacted = processor
            .compact(tool, std::mem::take(&mut content), focus)
            .await;
        content = compacted.text;
        notice = compacted.notice;
        summarized_from_bytes = compacted.summarized_from_bytes;
    }

    // A body the policy would not keep whole is never spilled: either the
    // untouched original is eligible (`full_output`), or the body that would
    // be stored (the compacted one) is itself within the persistable limit.
    let store = store.filter(|_| {
        full_output.is_some() || content.len() as u64 <= policy.max_persistable_bytes()
    });
    if !plan.truncation_exempt && plan.budget_bytes > 0 {
        let (capped, outcome) = apply_per_result_persistence(
            std::mem::take(&mut content),
            full_output,
            store,
            tool,
            Some(call_id),
            plan.budget_bytes,
        )
        .await;
        if outcome.persisted {
            tracing::info!(
                tool,
                from_bytes = outcome.original_bytes,
                to_bytes = outcome.final_bytes,
                "[harness::output_ladder] persisted oversized output"
            );
            processor.persisted(tool, call_id, &outcome).await;
        } else if outcome.original_bytes != outcome.final_bytes {
            tracing::debug!(
                tool,
                from_bytes = outcome.original_bytes,
                to_bytes = outcome.final_bytes,
                "[harness::output_ladder] truncated tool output"
            );
        }
        content = capped;
    }

    disclose(
        content,
        notice.as_deref(),
        summarized_from_bytes,
        summary_label,
    )
}

#[cfg(test)]
#[path = "output_ladder_tests.rs"]
mod tests;
