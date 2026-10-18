//! Shared run ledger and conservative physical-call reservation bounds.

use std::sync::Arc;

use tinyinference_llm::model::ChatModel;
use tinyinference_llm::model::budget::{Budget, BudgetedModel, CallBudget};

/// Budget state shared by the concrete models used for one turn and its child
/// agents.
///
/// Wrap each prebuilt route before retry, fallback, and cache middleware. The
/// cache then prevents replayed responses from reaching the budgeted model,
/// while retries and fallback attempts each pass through it and settle once.
#[derive(Debug, Clone)]
pub struct ModelBudget {
    /// Shared ledger. Clones and child ledgers retain the same accounting tree.
    pub ledger: Budget,
    /// Conservative reservation for one physical provider call.
    pub call: CallBudget,
}

impl ModelBudget {
    /// Creates a shared budget for the supplied run ledger and call bound.
    pub fn new(ledger: Budget, call: CallBudget) -> Self {
        Self { ledger, call }
    }

    /// Waits when live reservations temporarily consume available capacity.
    /// Settled spend that exceeds the budget still fails immediately.
    pub fn wait_for_capacity(mut self) -> Self {
        self.ledger = self.ledger.wait_for_capacity();
        self
    }

    /// Wraps a concrete model with charge normalization and ledger accounting.
    ///
    /// Apply this below retry/fallback and above the provider; response caching
    /// should remain outside it so a cache hit does not reserve or settle twice.
    pub fn wrap_model(&self, model: Arc<dyn ChatModel<()>>) -> Arc<dyn ChatModel<()>> {
        let model = crate::observability::turn_observer::observe_physical_dispatches(model);
        let charged = Arc::new(super::GatewayChargeModel::new(model));
        Arc::new(BudgetedModel::new(charged, self.ledger.clone(), self.call))
    }
}

#[cfg(test)]
#[path = "model_budget_tests.rs"]
mod tests;
