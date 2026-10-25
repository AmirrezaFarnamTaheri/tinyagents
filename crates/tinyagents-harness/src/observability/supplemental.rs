//! Event lifecycle for host model calls performed outside the agent loop.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tinyinference_llm::model::{ModelCallCorrelation, ModelRequest, ModelResponse};

use crate::{
    events::{AgentEvent, EventSink, GenerationEnrichment},
    ids::{CallId, RunId, now_ms},
};

/// Host enrichment for an out-of-loop model call.
pub trait SupplementalEnricher: Send + Sync {
    /// Return the actual model label and any validated provider pricing data.
    fn enrich(
        &self,
        model: &str,
        response: &ModelResponse,
    ) -> (Option<String>, GenerationEnrichment);
}

/// Correlated trace state shared by supplemental calls in one run.
pub struct SupplementalCallTrace {
    events: EventSink,
    run_id: RunId,
    next_call: AtomicUsize,
    actual_model: Mutex<Option<String>>,
    cache_hit: AtomicBool,
    enricher: Arc<dyn SupplementalEnricher>,
}

impl SupplementalCallTrace {
    /// Creates an event trace; call ids begin at one and remain unique per trace.
    pub fn new(
        events: EventSink,
        run_id: RunId,
        enricher: Arc<dyn SupplementalEnricher>,
    ) -> Arc<Self> {
        Arc::new(Self {
            events,
            run_id,
            next_call: AtomicUsize::new(1),
            actual_model: Mutex::new(None),
            cache_hit: AtomicBool::new(false),
            enricher,
        })
    }

    /// The owning run identifier.
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// The actual model from the latest completed supplemental call.
    pub fn actual_model(&self) -> Option<String> {
        self.actual_model
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether the latest completed supplemental call came from cache.
    pub fn is_cache_hit(&self) -> bool {
        self.cache_hit.load(Ordering::SeqCst)
    }
}

/// One in-flight supplemental call; dropping it records a single failure.
pub struct SupplementalCallGuard {
    trace: Arc<SupplementalCallTrace>,
    call_id: CallId,
    model: String,
    started_at_ms: u64,
    input: serde_json::Value,
    completed: bool,
}

impl SupplementalCallGuard {
    /// Starts a correlated call and emits its `ModelStarted` event.
    pub fn start(
        trace: &Arc<SupplementalCallTrace>,
        model: &str,
        request: &mut ModelRequest,
    ) -> Self {
        let n = trace.next_call.fetch_add(1, Ordering::Relaxed);
        let call_id = CallId::new(format!("{}-host-supplemental-{n}", trace.run_id.as_str()));
        let started_at_ms = now_ms();
        request.correlation = Some(ModelCallCorrelation::new(
            trace.run_id.as_str(),
            call_id.as_str(),
        ));
        trace.events.emit(AgentEvent::ModelStarted {
            call_id: call_id.clone(),
            model: model.to_owned(),
        });
        Self {
            trace: Arc::clone(trace),
            call_id,
            model: model.to_owned(),
            started_at_ms,
            input: serde_json::to_value(&request.messages).unwrap_or(serde_json::Value::Null),
            completed: false,
        }
    }

    /// Records completion, usage and host enrichment exactly once.
    pub fn complete(&mut self, response: &ModelResponse) {
        let mut usage = response.usage.or(response.message.usage);
        if response.served_from_cache {
            self.trace.events.emit(AgentEvent::ModelResponseCached {
                call_id: self.call_id.clone(),
            });
            if let Some(usage) = &mut usage {
                usage.charged_amount = None;
            }
        }
        self.trace
            .cache_hit
            .store(response.served_from_cache, Ordering::SeqCst);
        if let Some(usage) = usage {
            self.trace.events.emit(AgentEvent::UsageRecorded { usage });
        }
        self.trace.events.emit(AgentEvent::ModelCompleted {
            call_id: self.call_id.clone(),
            started_at_ms: Some(self.started_at_ms),
            usage,
            input: Some(self.input.clone()),
            output: Some(
                serde_json::to_value(&response.message).unwrap_or(serde_json::Value::Null),
            ),
        });
        let (actual_model, enrichment) = self.trace.enricher.enrich(&self.model, response);
        if let Some(actual_model) = actual_model {
            *self
                .trace
                .actual_model
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(actual_model);
        }
        if enrichment.provider_id.is_some()
            || enrichment.pricing.is_some()
            || enrichment.estimated_cost_usd.is_some()
        {
            self.trace.events.emit(AgentEvent::GenerationEnriched {
                call_id: self.call_id.clone(),
                enrichment,
            });
        }
        self.completed = true;
    }
}

impl Drop for SupplementalCallGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.trace.events.emit(AgentEvent::ModelFailed {
                call_id: self.call_id.clone(),
                model: self.model.clone(),
                started_at_ms: Some(self.started_at_ms),
                attempts: Some(1),
                error: "supplemental model call failed".to_owned(),
            });
        }
    }
}

#[cfg(test)]
#[path = "supplemental_tests.rs"]
mod tests;
