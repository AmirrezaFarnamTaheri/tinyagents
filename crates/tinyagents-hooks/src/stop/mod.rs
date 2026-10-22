//! Policies evaluated after each completed model response.

mod budget;
#[cfg(feature = "goals")]
mod goal_budget;
pub mod halt_summary;
mod hooks;
mod latch;

pub use budget::BudgetStopHook;
#[cfg(feature = "goals")]
pub use goal_budget::GoalBudgetStopHook;
pub use halt_summary::{
    HaltSummary, TerminalInferenceFailureKind, terminal_inference_failure_kind,
};
pub use hooks::{
    StopDecision, StopHook, StopHookMetrics, StopHookMiddleware, StopReport, TurnState,
};
pub use latch::StopLatch;

impl StopHookMetrics for tinyagents_harness::cost::TurnCost {
    fn record_model(&mut self, model: &str, response: &tinyinference_llm::model::ModelResponse) {
        let Some(recovered) = tinyinference_llm::usage::recover_usage(response) else {
            return;
        };
        self.add_priced_call(
            model,
            &tinyagents_harness::cost::CallUsage {
                usage: recovered.usage,
                charged_amount_usd: recovered.charged_amount_usd.unwrap_or(0.0),
                cost_is_estimate: false,
                charge_reported: recovered.charged_amount_usd.is_some(),
            },
        );
    }
}
