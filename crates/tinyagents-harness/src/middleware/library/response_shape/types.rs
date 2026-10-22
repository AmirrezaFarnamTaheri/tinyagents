//! Value types of the response-shape feature: the shape a host asks for, the
//! scope that carries it through a turn, and the report the turn fills.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use tinyinference_llm::model::ResponseFormat;

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
    pub(super) shape: ResponseShape,
    pub(super) report: Mutex<FinalResponse>,
    pub(super) tool_succeeded: AtomicBool,
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

    pub(super) fn lock_report(&self) -> std::sync::MutexGuard<'_, FinalResponse> {
        self.report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
