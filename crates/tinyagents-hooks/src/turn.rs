//! Parallel post-turn learning dispatch.
use std::sync::Arc;

use tinyagents_harness::host::{LearningSink, TurnSummary};

/// Dispatch a completed turn to every learning sink concurrently.
///
/// Sink failures are advisory: they are logged and never change the already
/// committed turn. The summary is cloned per sink so callbacks may run
/// independently without sharing mutable data.
///
/// The generic hook crate does not provide a host dispatch context. Hosts
/// needing one should bind it into their `LearningSink` adapter when they
/// construct the sink; this function only schedules the harness contract.
pub fn fire_learning_hooks(hooks: &[Arc<dyn LearningSink>], summary: TurnSummary) {
    for hook in hooks {
        let hook = Arc::clone(hook);
        let summary = summary.clone();
        tokio::spawn(async move {
            if let Err(error) = hook.on_turn_complete(&summary).await {
                tracing::warn!(hook = "learning_sink", %error, "post-turn learning hook failed");
            }
        });
    }
}

#[cfg(test)]
#[path = "turn_tests.rs"]
mod tests;
