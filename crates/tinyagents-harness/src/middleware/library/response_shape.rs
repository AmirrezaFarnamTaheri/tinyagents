//! Per-turn response shape: a response format and an output cap applied to
//! every model call of a turn, plus a report of the final call (finish reason,
//! answering model, reasoning tokens, summed usage and cost).
//!
//! The shape is an explicit value, [`ResponseShapeScope`], that the host
//! builds and hands to [`install_response_shape`]. There is no task-local: a
//! host that wants an ambient shape reads its own ambient state on the turn's
//! task and passes the scope here, and the middleware holds the `Arc` from
//! then on, so a model call that runs on another task still sees it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyinference_llm::model::{ModelRequest, ModelResponse, ResponseFormat, ToolChoice};

use crate::context::RunContext;
use crate::error::{Result as TaResult, TinyAgentsError};
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::runtime::AgentHarness;
use crate::structured::OutputValidator;
use tinytools::ToolResult;

/// Validates terminal provider text without retrieving external resources.
pub trait ResponseValidator: Send + Sync {
    /// Returns a safe classification, never response content.
    fn validate(&self, text: &str, finish_reason: Option<&str>) -> Result<(), String>;
}

/// What a host asks of every model call in one turn.
#[derive(Clone, Default)]
pub struct ResponseShape {
    /// Host validation of the original terminal text, before any repair.
    pub validator: Option<Arc<dyn ResponseValidator>>,
    /// Bounded number of output repair attempts.
    pub structured_retries: u8,
    /// Sent as `response_format` on every call of the turn's tool loop.
    pub response_format: Option<ResponseFormat>,
    /// Replaces the turn's per-call output cap.
    pub max_output_tokens: Option<u32>,
    /// Provider routing/reasoning options, applied to every model call.
    pub provider_options: serde_json::Value,
    /// Require a successful tool execution before accepting terminal output.
    pub require_tool_call: bool,
    /// Nucleus sampling probability applied to every call of this turn.
    pub top_p: Option<f64>,
}

impl std::fmt::Debug for ResponseShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseShape")
            .field("response_format", &self.response_format)
            .field("max_output_tokens", &self.max_output_tokens)
            .field("validator", &self.validator.is_some())
            .field("structured_retries", &self.structured_retries)
            .finish()
    }
}

/// What the turn's final model call reported.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FinalResponse {
    /// Reported accounting captured before the harness can refuse an answer.
    pub usage: Option<ResponseUsage>,
    /// Whether any call omitted its charged cost.
    pub unknown_cost: bool,
    /// Strict validation classification of the last terminal answer.
    pub validation_error: Option<String>,
    /// Number of terminal answer attempts (excluding tool calls).
    pub structured_attempts: u16,
    /// Whether the harness ended with a structured validation error.
    pub structured_failed: bool,
    /// The provider's finish reason for the last call (`stop`, `length`, ...).
    pub finish_reason: Option<String>,
    /// The model the provider says answered the last call.
    pub answered_model: Option<String>,
    /// Reasoning tokens summed over every call of the turn.
    pub reasoning_tokens: u64,
}

/// Usage summed over physical responses, including rejected terminal answers.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResponseUsage {
    /// Prompt tokens reported by the provider.
    pub input_tokens: u64,
    /// Generated tokens reported by the provider.
    pub output_tokens: u64,
    /// Cached prompt tokens.
    pub cached_tokens: u64,
    /// Reasoning tokens.
    pub reasoning_tokens: u64,
    /// Charged cost, unknown if any response omitted it.
    pub cost_usd: Option<f64>,
    /// At least one call supplied a billing receipt, including a malformed one.
    /// Distinguishes absent receipts from invalid authoritative billing data.
    pub has_cost_receipt: bool,
}

/// A shape and the report slot its turn fills.
#[derive(Debug, Default)]
pub struct ResponseShapeScope {
    shape: ResponseShape,
    report: Mutex<FinalResponse>,
    tool_succeeded: AtomicBool,
}

impl ResponseShapeScope {
    /// A scope asking for `shape`.
    #[must_use]
    pub fn new(shape: ResponseShape) -> Arc<Self> {
        Arc::new(Self {
            shape,
            report: Mutex::default(),
            tool_succeeded: AtomicBool::new(false),
        })
    }

    /// The shape this scope asks for.
    #[must_use]
    pub fn shape(&self) -> &ResponseShape {
        &self.shape
    }

    /// What the turn's calls have reported so far.
    #[must_use]
    pub fn report(&self) -> FinalResponse {
        self.lock_report().clone()
    }

    fn lock_report(&self) -> std::sync::MutexGuard<'_, FinalResponse> {
        self.report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Push the shaping middleware onto `harness` for `scope`. When the shape has
/// a validator, also arms the loop's bounded output repair with a permissive
/// extraction schema (the host validator applies the complete schema).
pub fn install_response_shape<Ctx: Send + Sync + 'static>(
    harness: &mut AgentHarness<(), Ctx>,
    scope: Arc<ResponseShapeScope>,
) {
    tracing::debug!(
        format = scope.shape.response_format.is_some(),
        max_output_tokens = ?scope.shape.max_output_tokens,
        "[tinyagents] response shape installed"
    );
    if scope.shape.validator.is_some() {
        let mut policy = harness.policy().clone();
        policy.default_response_format = Some(ResponseFormat::JsonSchema {
            name: "host_answer".into(),
            schema: serde_json::json!({}),
        });
        policy.output_retry.max_attempts = scope.shape.structured_retries;
        // Strict output validation owns the complete repair allowance;
        // empty-response recovery must not issue additional model calls.
        policy.truncated_empty_retries = 0;
        policy.truncated_empty_nudges = 0;
        policy.output_retry.message_template =
            "Return complete JSON matching the requested schema.".into();
        harness.with_policy(policy);
        harness.with_output_validator(Arc::new(StrictValidator(Arc::clone(&scope))));
    }
    harness.push_middleware(Arc::new(ResponseShapeMiddleware(scope)));
}

/// Applies the shape to each request and folds each response into the report.
struct ResponseShapeMiddleware(Arc<ResponseShapeScope>);

#[async_trait]
impl<Ctx: Send + Sync + 'static> Middleware<(), Ctx> for ResponseShapeMiddleware {
    fn name(&self) -> &str {
        "response_shape"
    }

    async fn before_model(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        request: &mut ModelRequest,
    ) -> TaResult<()> {
        let shape = &self.0.shape;
        if shape.require_tool_call && !self.0.tool_succeeded.load(Ordering::Acquire) {
            if request.tools.is_empty() {
                return Err(TinyAgentsError::StructuredOutput(
                    "required_tool_call_has_no_tools".into(),
                ));
            }
            // Tool selection must remain free to emit function calls: asking
            // for the final JSON shape here can make a model skip exploration.
            request.response_format = None;
            request.tool_choice = ToolChoice::Required;
        } else if let Some(format) = &shape.response_format {
            request.response_format = Some(format.clone());
        }
        if !shape.provider_options.is_null() {
            request.provider_options = shape.provider_options.clone();
        }
        if let Some(cap) = shape.max_output_tokens {
            request.max_tokens = Some(cap);
        }
        if let Some(top_p) = shape.top_p {
            request.top_p = Some(top_p);
        }
        Ok(())
    }

    async fn after_model(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        response: &mut ModelResponse,
    ) -> TaResult<()> {
        record(&self.0, response);
        if response.tool_calls().is_empty() {
            if let Some(validator) = &self.0.shape.validator {
                let error = validator
                    .validate(&response.text(), response.finish_reason.as_deref())
                    .err();
                let mut report = self.0.lock_report();
                report.structured_attempts = report.structured_attempts.saturating_add(1);
                report.validation_error = error;
            }
        }
        if self.0.shape.require_tool_call
            && !self.0.tool_succeeded.load(Ordering::Acquire)
            && response.tool_calls().is_empty()
        {
            self.0.lock_report().validation_error = Some("RequiredToolCallMissing".into());
            // tool_choice is advisory on compatible gateways. Enforce the
            // host requirement even when a provider ignores that wire hint.
            return Err(TinyAgentsError::StructuredOutput(
                "required_tool_call_missing".into(),
            ));
        }
        Ok(())
    }

    async fn after_tool(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        _invocation: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> TaResult<()> {
        if !result.is_error {
            self.0.tool_succeeded.store(true, Ordering::Release);
        }
        Ok(())
    }

    async fn on_error(&self, _ctx: &mut RunContext<Ctx>, error: &TinyAgentsError) -> TaResult<()> {
        if matches!(error, TinyAgentsError::StructuredOutput(_)) {
            self.0.lock_report().structured_failed = true;
        }
        Ok(())
    }

    fn is_observer(&self) -> bool {
        true
    }
}

struct StrictValidator(Arc<ResponseShapeScope>);

#[async_trait]
impl<Ctx: Send + Sync + 'static> OutputValidator<(), Ctx> for StrictValidator {
    async fn validate(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        _output: &serde_json::Value,
    ) -> TaResult<()> {
        match self.0.report().validation_error {
            Some(reason) => Err(TinyAgentsError::ModelRetry(reason)),
            None => Ok(()),
        }
    }
}

/// Fold one completed call into the report: the last call's finish reason
/// and model win; reasoning tokens add up (a cache replay spent none).
fn record(scope: &ResponseShapeScope, response: &ModelResponse) {
    let mut report = scope.lock_report();
    report.finish_reason = response.finish_reason.clone();
    report.answered_model = response
        .raw
        .as_ref()
        .and_then(|raw| raw.get("model"))
        .and_then(serde_json::Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            response
                .resolved_route
                .as_ref()
                .map(|route| route.model.clone())
                .or_else(|| response.resolved_model.as_ref().map(|m| m.name.clone()))
                .filter(|model| !model.trim().is_empty())
        });
    if response.served_from_cache {
        return;
    }
    let raw_cost = response.raw.as_ref().and_then(|raw| {
        // Presence selects the authoritative source; malformed billing
        // must remain unknown rather than fall through to an estimate.
        if let Some(buyer) = raw.pointer("/usage/buyer_cost_micro") {
            Some(buyer.as_f64().map(|value| value / 1_000_000.0))
        } else {
            raw.pointer("/usage/cost").map(serde_json::Value::as_f64)
        }
    });
    let cost = raw_cost
        .unwrap_or_else(|| {
            response
                .usage
                .and_then(|usage| usage.charged_amount)
                .map(|amount| amount.micros as f64 / 1_000_000.0)
        })
        .filter(|cost| cost.is_finite() && *cost >= 0.0);
    report.unknown_cost |= cost.is_none();
    let unknown_cost = report.unknown_cost;
    if response.usage.is_some() || cost.is_some() {
        let total = report.usage.get_or_insert_with(ResponseUsage::default);
        total.has_cost_receipt |= raw_cost.is_some()
            || response
                .usage
                .and_then(|usage| usage.charged_amount)
                .is_some();
        total.cost_usd = if unknown_cost {
            None
        } else {
            Some(total.cost_usd.unwrap_or_default() + cost.unwrap_or_default())
        };
        if let Some(usage) = &response.usage {
            total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
            total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
            total.cached_tokens = total.cached_tokens.saturating_add(usage.cache_read_tokens);
            total.reasoning_tokens = total
                .reasoning_tokens
                .saturating_add(usage.reasoning_tokens);
        }
    }
    if let Some(usage) = &response.usage {
        report.reasoning_tokens += usage.reasoning_tokens;
    }
}

#[cfg(test)]
#[path = "response_shape_tests.rs"]
mod tests;
