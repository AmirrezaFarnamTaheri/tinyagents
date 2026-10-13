//! Observe-only token-accounting parity check.

use async_trait::async_trait;

use crate::context::RunContext;
use crate::error::Result as TaResult;
use crate::middleware::{AgentRun, Middleware};

use crate::middleware::library::BudgetTracker;

/// Compares a [`BudgetTracker`]'s token accounting against the authoritative
/// [`AgentRun::usage`] at the end of a run and logs parity or divergence.
/// Observe-only: never fails the run, never enforces a cap.
pub struct BudgetParityObserver {
    tracker: BudgetTracker,
}

impl BudgetParityObserver {
    /// Compare against `tracker`, the observe-only budget middleware's handle.
    pub fn new(tracker: BudgetTracker) -> Self {
        Self { tracker }
    }

    /// Whether the tracker's token totals match `run`'s.
    pub fn matches(&self, run: &AgentRun) -> bool {
        let crate_usage = self.tracker.snapshot().usage;
        let (l, c) = (&run.usage.usage, &crate_usage.usage);
        l.input_tokens == c.input_tokens
            && l.output_tokens == c.output_tokens
            && l.cache_read_tokens == c.cache_read_tokens
            && l.cache_creation_tokens == c.cache_creation_tokens
            && l.reasoning_tokens == c.reasoning_tokens
            && l.total_tokens == c.total_tokens
            && run.usage.calls == crate_usage.calls
    }
}

#[async_trait]
impl<Ctx: Send + Sync + 'static> Middleware<(), Ctx> for BudgetParityObserver {
    fn name(&self) -> &str {
        "budget_parity"
    }

    fn is_observer(&self) -> bool {
        true
    }

    async fn after_agent(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        run: &mut AgentRun,
    ) -> TaResult<()> {
        if self.matches(run) {
            tracing::debug!(
                calls = run.usage.calls,
                "[budget_parity] token accounting matches"
            );
        } else {
            tracing::warn!(
                local_calls = run.usage.calls,
                tracked_calls = self.tracker.snapshot().usage.calls,
                "[budget_parity] divergence: tracker token accounting differs from AgentRun.usage"
            );
        }
        Ok(())
    }
}

