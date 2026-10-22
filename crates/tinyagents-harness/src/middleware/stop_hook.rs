//! Host-supplied stop votes evaluated after each model response.

use async_trait::async_trait;
use tinyinference_llm::model::ModelResponse;

use crate::cost::{CallUsage, TurnCost};

/// Snapshot passed to a host stop hook after one model call.
pub struct TurnState<'a, T> {
    /// One-based model-call number for this run.
    pub iteration: u32,
    /// Configured maximum model calls for this run.
    pub max_iterations: u32,
    /// Accumulated metrics, including this response.
    pub cost: &'a T,
    /// Model label supplied when the middleware was constructed.
    pub model: &'a str,
}

/// A host decision to continue or pause the run after a model call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopDecision {
    /// Continue to the next safe point in the agent loop.
    Continue,
    /// Pause the run before its next model or tool call.
    Stop { reason: String },
}

/// Host policy evaluated after each model response.
#[async_trait]
pub trait StopHook<T: Send + Sync>: Send + Sync {
    /// Stable label used in stop reports.
    fn name(&self) -> &str;

    /// Decide whether the run may continue with the supplied snapshot.
    async fn check(&self, state: &TurnState<'_, T>) -> StopDecision;
}

/// Metrics that a stop-hook middleware can accumulate from model responses.
pub trait StopHookMetrics: Clone + Send + Sync + 'static {
    /// Fold one completed model response into the metrics.
    fn record_model(&mut self, model: &str, response: &ModelResponse);
}

impl StopHookMetrics for TurnCost {
    fn record_model(&mut self, model: &str, response: &ModelResponse) {
        let usage = response.usage.unwrap_or_default();
        let charged_amount_usd = usage
            .charged_amount_nanos
            .filter(|amount| *amount >= 0)
            .map(|nanos| nanos as f64 / 1_000_000_000.0)
            .or_else(|| {
                usage
                    .charged_amount
                    .filter(|amount| amount.micros >= 0)
                    .map(|amount| amount.micros as f64 / 1_000_000.0)
            });
        self.add_priced_call(
            model,
            &CallUsage {
                usage,
                charged_amount_usd: charged_amount_usd.unwrap_or_default(),
                cost_is_estimate: false,
                charge_reported: charged_amount_usd.is_some(),
            },
        );
    }
}

/// A stop vote and the model-call context in which it was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopReport {
    /// Name of the hook that requested the stop.
    pub hook: String,
    /// One-based model-call number.
    pub iteration: u32,
    /// Model label supplied to the middleware.
    pub model: String,
    /// Reason returned by the hook.
    pub reason: String,
}

