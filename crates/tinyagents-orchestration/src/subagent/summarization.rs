//! Orchestration adapter for call-cap summaries.

use async_trait::async_trait;
use tinyagents_harness::summarization::{CapToolResult, build_cap_digest, cap_summary_fallback};

/// Host model seam used only when a child reaches its call cap.
#[async_trait]
pub trait CapSummaryModel: Send + Sync {
    /// Produces a concise progress summary, or an error when unavailable.
    async fn summarize(&self, max_calls: usize, digest: &str) -> Result<String, String>;
}

/// The result of preparing the parent's cap-hit checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapSummary {
    /// Progress text returned to the parent.
    pub text: String,
    /// Whether a model produced this text; false means deterministic fallback.
    pub model_summarized: bool,
    /// The bounded tool/result digest supplied to the model or fallback.
    pub digest: String,
}

/// Summarizes completed work at the call cap, always preserving a deterministic
/// incomplete checkpoint if the model fails or returns no prose.
pub async fn summarize_at_cap(
    model: Option<&dyn CapSummaryModel>,
    max_calls: usize,
    results: &[CapToolResult<'_>],
    per_result_chars: usize,
) -> CapSummary {
    let digest = build_cap_digest(results, per_result_chars);
    let summary = match model {
        Some(model) => model.summarize(max_calls, &digest).await.ok(),
        None => None,
    }
    .filter(|text| !text.trim().is_empty());
    match summary {
        Some(text) => CapSummary {
            text,
            model_summarized: true,
            digest,
        },
        None => CapSummary {
            text: cap_summary_fallback(max_calls, &digest),
            model_summarized: false,
            digest,
        },
    }
}

#[cfg(test)]
#[path = "summarization_tests.rs"]
mod tests;
