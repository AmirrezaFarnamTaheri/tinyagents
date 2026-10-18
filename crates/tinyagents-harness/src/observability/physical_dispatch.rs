//! Content-free hooks at the provider invocation boundary.
use std::sync::Arc;
use std::time::{Duration, Instant};

use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};

/// Metadata available when one provider invocation is admitted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PhysicalDispatchMetadata {
    /// Requested model identifier.
    pub requested_model: Option<String>,
    /// Harness call identifier, when supplied.
    pub call_id: Option<String>,
}

/// Outcome of an admitted provider invocation. It contains no request or response content.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalDispatchOutcome {
    /// Whether the provider call returned or was interrupted.
    pub status: PhysicalDispatchStatus,
    /// Time spent in the provider invocation.
    pub duration: Duration,
    /// Provider-reported usage, when returned.
    pub usage: Option<PhysicalDispatchUsage>,
    /// Provider-reported answering model.
    pub answered_model: Option<String>,
    /// Provider finish reason.
    pub finish_reason: Option<String>,
}

/// Terminal state of a physical provider invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalDispatchStatus {
    /// The provider returned a response, including refusal responses.
    Succeeded,
    /// The provider returned a refusal response.
    Refused,
    /// The provider returned an error.
    Failed,
    /// The future was dropped before returning.
    Cancelled,
}

/// Provider-reported usage, without raw metadata.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PhysicalDispatchUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Cached input tokens.
    pub cached_tokens: u64,
    /// Reasoning tokens.
    pub reasoning_tokens: u64,
    /// Provider charge in USD, if reported.
    pub cost_usd: Option<f64>,
}

/// Terminal callback paired with one start callback.
pub trait PhysicalDispatchTerminal: Send {
    /// Report the invocation's sole terminal outcome.
    fn on_terminal(self: Box<Self>, outcome: PhysicalDispatchOutcome);
}

/// Synchronous observer called only after provider-level admission succeeds.
pub trait PhysicalDispatchObserver: Send + Sync {
    /// Record a start and return its terminal callback.
    fn on_start(&self, metadata: &PhysicalDispatchMetadata) -> Box<dyn PhysicalDispatchTerminal>;
}

/// A provider wrapper that reports each admitted invocation exactly once.
pub struct ObservedChatModel<State: Send + Sync> {
    inner: Arc<dyn ChatModel<State>>,
    observer: Arc<dyn PhysicalDispatchObserver>,
}

impl<State: Send + Sync> ObservedChatModel<State> {
    /// Wrap a concrete provider. Put budget admission outside this wrapper.
    pub fn new(
        inner: Arc<dyn ChatModel<State>>,
        observer: Arc<dyn PhysicalDispatchObserver>,
    ) -> Self {
        Self { inner, observer }
    }
}

impl<State: Send + Sync> std::fmt::Debug for ObservedChatModel<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservedChatModel").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<State: Send + Sync> ChatModel<State> for ObservedChatModel<State> {
    fn profile(&self) -> Option<&tinyinference_llm::model::ModelProfile> {
        self.inner.profile()
    }

    fn supports_input(
        &self,
        modality: tinyinference_llm::model::InputModality,
        mime: &str,
        source: tinyinference_llm::model::InputSource,
    ) -> bool {
        self.inner.supports_input(modality, mime, source)
    }

    fn cache_identity(&self) -> Option<String> {
        self.inner.cache_identity()
    }

    async fn invoke(
        &self,
        state: &State,
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        let metadata = PhysicalDispatchMetadata {
            requested_model: request.model.clone(),
            call_id: request
                .correlation
                .as_ref()
                .map(|correlation| correlation.call_id.clone()),
        };
        let guard = PhysicalDispatchGuard::new(self.observer.on_start(&metadata));
        let result = self.inner.invoke(state, request).await;
        let response = result.as_ref().ok();
        let raw = response.and_then(|response| response.raw.as_ref());
        let usage = response
            .and_then(|response| response.usage.as_ref())
            .map(|usage| PhysicalDispatchUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cached_tokens: usage.cache_read_tokens,
                reasoning_tokens: usage.reasoning_tokens,
                cost_usd: usage
                    .charged_amount
                    .as_ref()
                    .map(|amount| amount.micros as f64 / 1_000_000.0)
                    .or_else(|| {
                        raw.and_then(|raw| raw.pointer("/usage/cost"))
                            .and_then(serde_json::Value::as_f64)
                    }),
            });
        let status = match response.and_then(|response| response.finish_reason.as_deref()) {
            Some("refusal" | "content_filter") => PhysicalDispatchStatus::Refused,
            _ if result.is_ok() => PhysicalDispatchStatus::Succeeded,
            _ => PhysicalDispatchStatus::Failed,
        };
        guard.finish(PhysicalDispatchOutcome {
            status,
            duration: Duration::ZERO,
            usage,
            answered_model: raw
                .and_then(|raw| raw.get("model"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            finish_reason: response.and_then(|response| response.finish_reason.clone()),
        });
        result
    }
}

/// Terminal guard that records cancellation when the invocation future drops.
pub struct PhysicalDispatchGuard {
    terminal: Option<Box<dyn PhysicalDispatchTerminal>>,
    started: Instant,
}

impl PhysicalDispatchGuard {
    /// Begin a dispatch and retain its paired terminal callback.
    pub fn new(terminal: Box<dyn PhysicalDispatchTerminal>) -> Self {
        Self {
            terminal: Some(terminal),
            started: Instant::now(),
        }
    }

    /// Finish with a provider result. Dropping an unfinished guard reports cancellation.
    pub fn finish(mut self, mut outcome: PhysicalDispatchOutcome) {
        outcome.duration = self.started.elapsed();
        if let Some(terminal) = self.terminal.take() {
            terminal.on_terminal(outcome);
        }
    }
}

impl Drop for PhysicalDispatchGuard {
    fn drop(&mut self) {
        if let Some(terminal) = self.terminal.take() {
            terminal.on_terminal(PhysicalDispatchOutcome {
                status: PhysicalDispatchStatus::Cancelled,
                duration: self.started.elapsed(),
                usage: None,
                answered_model: None,
                finish_reason: None,
            });
        }
    }
}

#[cfg(test)]
#[path = "physical_dispatch_tests.rs"]
mod tests;
