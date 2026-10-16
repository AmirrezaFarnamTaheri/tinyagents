//! Thread-goal budget policy, enabled by the `goals` feature.
use std::sync::Arc;

use async_trait::async_trait;
use tinyagents_graph::goals::{BudgetVerdict, GoalBudgetGuard, ThreadGoal};
use tinyagents_harness::cost::TurnCost;
use tinyagents_harness::store::Store;

use super::{StopDecision, StopHook, TurnState};

/// Pause while a stored active thread goal would exceed its token budget.
#[derive(Clone)]
pub struct GoalBudgetStopHook {
    store: Arc<dyn Store>,
    guard: GoalBudgetGuard,
}

impl GoalBudgetStopHook {
    /// Create a hook for an active, budgeted goal; return `None` otherwise.
    pub fn for_goal(store: Arc<dyn Store>, goal: &ThreadGoal) -> Option<Self> {
        Some(Self {
            store,
            guard: GoalBudgetGuard::for_goal(goal)?,
        })
    }
}

#[async_trait]
impl StopHook<TurnCost> for GoalBudgetStopHook {
    fn name(&self) -> &str {
        "thread_goal_budget"
    }

    async fn check(&self, state: &TurnState<'_, TurnCost>) -> StopDecision {
        let in_flight = state
            .cost
            .input_tokens
            .saturating_add(state.cost.output_tokens);
        match self.guard.check(&self.store, in_flight).await {
            Ok(BudgetVerdict::Stop { reason }) => StopDecision::Stop { reason },
            Ok(BudgetVerdict::Continue) => StopDecision::Continue,
            Err(error) => {
                tracing::debug!(%error, "goal budget lookup failed; continuing");
                StopDecision::Continue
            }
        }
    }
}
