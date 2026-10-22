//! The context ladder: the order in which the reduction steps that keep a
//! request inside the model's context window must fire, and the pure sizing
//! rules the steps share.
//!
//! `before_model` runs in registration order, so registration order IS firing
//! order, and each step only matters when the one above was not enough:
//!
//! 1. compression folds the older slice into a task-aware summary;
//! 2. microcompact blanks older tool bodies if still over;
//! 3. the wrap-up undoes (2) for a capped turn's concluding call;
//! 4. the artifact contents list says where offloaded results went;
//! 5. the trim evicts oldest whole messages if still over.
//!
//! Microcompact once ran ahead of compression, so the summarizer was handed a
//! transcript whose tool bodies were already blanked and asked for results it
//! could no longer see. [`ContextLadder::install`] makes that ordering the only
//! one a host can express. Which steps exist, the summarizer behind
//! compression, and the prompt text stay with the host.

use std::sync::Arc;

use crate::middleware::{ContextCompressionMiddleware, MicrocompactMiddleware, Middleware};
use crate::runtime::AgentHarness;
use crate::summarization::{
    DEFAULT_SUMMARIZE_KEEP_LAST, SummarizationPolicy, summarization_policy,
};

use super::{
    ArtifactIndexTocMiddleware, FinalCallWrapUpMiddleware, ImageAwareMessageTrimMiddleware,
    legacy_max_input_tokens,
};

/// Window assumed for sizing microcompact when the model's context window is
/// unknown. Deliberately conservative: small enough that a runaway transcript is
/// blanked well before a real model overflows, large enough that a short run
/// stays append-only and prompt-cache eligible.
pub const UNKNOWN_WINDOW_FALLBACK_TOKENS: u64 = 64_000;

/// Share of the context window a compaction keeps verbatim as its tail.
pub const COMPACTION_TAIL_WINDOW_FRACTION: f64 = 0.30;

/// The summarization policy for a turn, or `None` when compaction has nothing
/// to size against.
///
/// Normally `min(80% of the window, 350k tokens)` ([`summarization_policy`]).
/// `trigger_override` pins the trigger to an absolute token count instead and
/// enables compaction even when the window is unknown. The kept tail is sized
/// by [`with_turn_aware_tail`].
#[must_use]
pub fn compression_policy(
    context_window: Option<u64>,
    trigger_override: Option<u64>,
) -> Option<SummarizationPolicy> {
    let window = context_window.filter(|w| *w > 0);
    match trigger_override.filter(|t| *t > 0) {
        Some(tokens) => {
            tracing::info!(
                trigger_tokens = tokens,
                context_window = ?window,
                "[context_ladder] compaction trigger override active"
            );
            let base = window
                .map(summarization_policy)
                .unwrap_or(SummarizationPolicy {
                    keep_last: DEFAULT_SUMMARIZE_KEEP_LAST,
                    ..SummarizationPolicy::default()
                });
            Some(base.with_trigger_override(tokens))
        }
        None => window.map(summarization_policy),
    }
    .map(|policy| with_turn_aware_tail(policy, window))
}

/// Size the kept tail in tokens and pin the turn's user message.
///
/// Split by count, a long tool-driven turn that crosses the trigger mid-turn
/// folds its only user message into the summary and the agent loses the task.
/// This keeps ~[`COMPACTION_TAIL_WINDOW_FRACTION`] of the window verbatim,
/// capped at half the trigger so a capped trigger still has room to fold, and
/// pins the turn's user message to the front of that tail. With no known window
/// the tail is that fraction of the trigger.
#[must_use]
pub fn with_turn_aware_tail(
    mut policy: SummarizationPolicy,
    window: Option<u64>,
) -> SummarizationPolicy {
    let trigger = policy.trigger_budget();
    let keep_tokens = match window {
        Some(window) => ((window as f64 * COMPACTION_TAIL_WINDOW_FRACTION) as u64).min(trigger / 2),
        None => (trigger as f64 * COMPACTION_TAIL_WINDOW_FRACTION) as u64,
    };
    tracing::debug!(
        context_window = ?window,
        trigger,
        keep_tokens,
        "[context_ladder] token tail with the turn's user message pinned"
    );
    policy.keep_recent_tokens = Some(keep_tokens);
    policy.pin_turn_user_message = true;
    policy
}

/// The microcompact step, sized behind the trim.
///
/// The token budget is the trim's allowance. Without a window it is sized
/// against [`UNKNOWN_WINDOW_FALLBACK_TOKENS`] rather than left unset: a bare
/// microcompact blanks every old tool result on every call, and each blanking
/// rewrites an already-sent prefix position, so the provider's prompt cache
/// misses from there on. Events are on, so the blanking is never silent.
#[must_use]
pub fn microcompact_for_window(
    keep_recent: usize,
    placeholder: impl Into<String>,
    context_window: Option<u64>,
) -> MicrocompactMiddleware {
    let window = context_window
        .filter(|w| *w > 0)
        .unwrap_or(UNKNOWN_WINDOW_FALLBACK_TOKENS);
    MicrocompactMiddleware::new(keep_recent, placeholder)
        .with_token_budget(legacy_max_input_tokens(window).max(1))
        .with_events(true)
}

/// The steps of a turn's context ladder; `None` leaves a step out.
///
/// Fields are listed in firing order. Install with [`ContextLadder::install`].
#[derive(Default)]
pub struct ContextLadder {
    /// Summarizing compression, registered as lifecycle and model middleware so
    /// both the threshold path and the overflow-retry path share one fold.
    pub compression: Option<Arc<ContextCompressionMiddleware>>,
    /// Blanks older tool bodies when compression was not enough.
    pub microcompact: Option<MicrocompactMiddleware>,
    /// Concludes a capped turn on its last permitted call.
    pub wrap_up: Option<Arc<FinalCallWrapUpMiddleware>>,
    /// Lists where offloaded results went; counted by the trim.
    pub artifact_toc: Option<ArtifactIndexTocMiddleware>,
    /// Last resort: evicts whole messages.
    pub trim: Option<ImageAwareMessageTrimMiddleware>,
}

impl ContextLadder {
    /// Register the present steps on `harness` in firing order. Returns the
    /// compression and wrap-up handles so the run loop can read them after the
    /// drive future returns.
    pub fn install<Ctx: Send + Sync + 'static>(
        self,
        harness: &mut AgentHarness<(), Ctx>,
    ) -> (
        Option<Arc<ContextCompressionMiddleware>>,
        Option<Arc<FinalCallWrapUpMiddleware>>,
    ) {
        if let Some(mw) = &self.compression {
            harness.push_middleware(mw.clone() as Arc<dyn Middleware<(), Ctx>>);
            harness.push_model_middleware(mw.clone());
        }
        if let Some(mw) = self.microcompact {
            harness.push_middleware(Arc::new(mw));
        }
        if let Some(mw) = &self.wrap_up {
            harness.push_middleware(mw.clone() as Arc<dyn Middleware<(), Ctx>>);
        }
        if let Some(mw) = self.artifact_toc {
            harness.push_middleware(Arc::new(mw));
        }
        if let Some(mw) = self.trim {
            harness.push_middleware(Arc::new(mw));
        }
        tracing::debug!("[context_ladder] installed in firing order");
        (self.compression, self.wrap_up)
    }
}
