//! Cost-based stop policy.
use async_trait::async_trait;
use tinyagents_harness::cost::TurnCost;

use super::{StopDecision, StopHook, TurnState};

/// Pause a turn once its known cumulative cost reaches a configured USD cap.
#[derive(Debug, Clone, Copy)]
pub struct BudgetStopHook {
    /// Cumulative USD charge at which the turn pauses.
    pub max_usd: f64,
}

impl BudgetStopHook {
    /// Create a cost cap. Non-finite or non-positive values stop immediately
    /// with a configuration error reason.
    pub fn new(max_usd: f64) -> Self { Self { max_usd } }
}

#[async_trait]
impl StopHook<TurnCost> for BudgetStopHook {
    fn name(&self) -> &str { "budget" }

    async fn check(&self, state: &TurnState<'_, TurnCost>) -> StopDecision {
        if !self.max_usd.is_finite() || self.max_usd <= 0.0 {
            return StopDecision::Stop { reason: format!("invalid budget cap configured: max_usd={}", self.max_usd) };
        }
        let spent = state.cost.cost.known_usd;
        if spent >= self.max_usd {
            StopDecision::Stop { reason: format!("turn cost ${spent:.4} reached cap ${:.4}", self.max_usd) }
        } else {
            StopDecision::Continue
        }
    }
}

#[cfg(test)]
#[path = "budget_tests.rs"]
mod tests;
