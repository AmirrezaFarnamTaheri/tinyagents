//! Policies evaluated after each completed model response.

mod hooks;
mod latch;
mod budget;

pub use hooks::{StopDecision, StopHook, StopHookMetrics, StopHookMiddleware, StopReport, TurnState};
pub use latch::StopLatch;
pub use budget::BudgetStopHook;

impl StopHookMetrics for tinyagents_harness::cost::TurnCost {
    fn record_model(&mut self, model: &str, response: &tinyinference_llm::model::ModelResponse) {
        let Some(recovered) = tinyinference_llm::usage::recover_usage(response) else { return; };
        self.add_priced_call(model, &tinyagents_harness::cost::CallUsage {
            usage: recovered.usage,
            charged_amount_usd: recovered.charged_amount_usd.unwrap_or(0.0),
            cost_is_estimate: false,
            charge_reported: recovered.charged_amount_usd.is_some(),
        });
    }
}
