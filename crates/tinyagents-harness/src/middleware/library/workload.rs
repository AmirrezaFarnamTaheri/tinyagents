//! Generic middleware for declarative workload routes.

use std::sync::Arc;

use async_trait::async_trait;
use tinyinference_llm::model::{CapabilitySet, ModelRequest, ModelResponse, ResolvedModelRoute};
use tinyinference_llm::usage::{BilledUsage, usage_info_from_response};

use crate::context::RunContext;
use crate::events::{AgentEvent, GenerationEnrichment, GenerationPricing, is_valid_provider_id};
use crate::middleware::{MiddlewareModelOutcome, ModelHandler, ModelMiddleware};

/// Stamps a route's required capabilities unless a stricter requirement was
/// already attached to the request.
pub struct RequiredCapabilitiesMiddleware {
    required: CapabilitySet,
}

impl RequiredCapabilitiesMiddleware {
    /// Creates middleware for one already-selected route.
    pub fn new(required: CapabilitySet) -> Self {
        Self { required }
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ModelMiddleware<State, Ctx>
    for RequiredCapabilitiesMiddleware
{
    fn name(&self) -> &str {
        "workload.required_capabilities"
    }

    async fn wrap_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        state: &State,
        mut request: ModelRequest,
        next: ModelHandler<'_, State, Ctx>,
    ) -> crate::error::Result<MiddlewareModelOutcome> {
        if request.required_capabilities.is_none() {
            request = request.with_required_capabilities(self.required.clone());
        }
        next.run(ctx, state, request).await
    }
}

/// Emits a fallback event after a successful route-resolved call selects a
/// model other than the turn's primary model.
///
/// The middleware observes the resolved response and never dispatches another
/// call. Hosts should put it around the native retry/fallback path and provide
/// responses with typed `resolved_model` metadata.
pub struct FallbackObserverMiddleware {
    primary: String,
}

impl FallbackObserverMiddleware {
    /// Creates an observer for the turn's selected primary model name.
    pub fn new(primary: impl Into<String>) -> Self {
        Self {
            primary: primary.into(),
        }
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ModelMiddleware<State, Ctx>
    for FallbackObserverMiddleware
{
    fn name(&self) -> &str {
        "workload.fallback_observer"
    }

    async fn wrap_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        state: &State,
        request: ModelRequest,
        next: ModelHandler<'_, State, Ctx>,
    ) -> crate::error::Result<MiddlewareModelOutcome> {
        let outcome = next.run(ctx, state, request).await?;
        let MiddlewareModelOutcome::Response(response) = outcome else {
            return Ok(outcome);
        };
        if let Some(resolved) = response.resolved_model.as_ref()
            && resolved.name != self.primary
        {
            ctx.emit(AgentEvent::FallbackSelected {
                from: self.primary.clone(),
                to: resolved.name.clone(),
            });
        }
        Ok(MiddlewareModelOutcome::from(response))
    }
}

/// Carries the complete logical-call response to a host-owned usage sink.
///
/// It runs once after the wrapped call succeeds, including after a stream has
/// been folded into its terminal response. Hosts may inspect typed usage and
/// raw provider metadata without adding product billing types to the harness.
pub struct UsageCarryMiddleware {
    carry: Arc<dyn Fn(&ModelResponse) + Send + Sync>,
}

impl UsageCarryMiddleware {
    /// Creates a response observer. The callback should only copy the fields it
    /// needs into host-owned accounting state.
    pub fn new(carry: impl Fn(&ModelResponse) + Send + Sync + 'static) -> Self {
        Self {
            carry: Arc::new(carry),
        }
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ModelMiddleware<State, Ctx> for UsageCarryMiddleware {
    fn name(&self) -> &str {
        "workload.usage_carry"
    }

    async fn wrap_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        state: &State,
        request: ModelRequest,
        next: ModelHandler<'_, State, Ctx>,
    ) -> crate::error::Result<MiddlewareModelOutcome> {
        let outcome = next.run(ctx, state, request).await?;
        let MiddlewareModelOutcome::Response(response) = outcome else {
            return Ok(outcome);
        };
        (self.carry)(&response);
        Ok(MiddlewareModelOutcome::from(response))
    }
}

/// Estimates a model call's cost using host-owned pricing policy.
///
/// `BilledUsage` retains the complete provider usage record, including cache,
/// reasoning, reported-charge and context-window fields. The harness decides
/// when an estimate is appropriate; the host decides how to price it.
pub trait CostEstimator: Send + Sync {
    /// Returns an estimated USD cost for this model and usage, when known.
    fn estimate(&self, model: &str, usage: &BilledUsage) -> Option<f64>;
}

type PricingLookup = Arc<dyn Fn(&str) -> Option<GenerationPricing> + Send + Sync>;
type ResolvedRouteSink<Ctx> = Arc<dyn Fn(&Ctx, ResolvedModelRoute) + Send + Sync>;

/// Enriches generation events with the provider and model selected by the
/// successful response, while sending its typed route to a host-owned sink.
///
/// The route sink receives the current run's context data, so callers can
/// update per-run state without task-local or process-global route storage.
/// Pricing lookup is optional because catalog ownership belongs to the host.
pub struct ResolvedRouteMiddleware<Ctx, E: CostEstimator> {
    configured_provider: String,
    configured_model: String,
    estimator: E,
    pricing_lookup: Option<PricingLookup>,
    route_sink: ResolvedRouteSink<Ctx>,
}

impl<Ctx, E: CostEstimator> ResolvedRouteMiddleware<Ctx, E> {
    /// Creates route enrichment with a host cost estimator and per-run route
    /// sink. A pricing lookup may be supplied with [`Self::with_pricing_lookup`].
    pub fn new(
        configured_provider: impl Into<String>,
        configured_model: impl Into<String>,
        estimator: E,
        route_sink: impl Fn(&Ctx, ResolvedModelRoute) + Send + Sync + 'static,
    ) -> Self {
        Self {
            configured_provider: configured_provider.into(),
            configured_model: configured_model.into(),
            estimator,
            pricing_lookup: None,
            route_sink: Arc::new(route_sink),
        }
    }

    /// Adds a host-owned model price lookup for generation trace metadata.
    pub fn with_pricing_lookup(
        mut self,
        lookup: impl Fn(&str) -> Option<GenerationPricing> + Send + Sync + 'static,
    ) -> Self {
        self.pricing_lookup = Some(Arc::new(lookup));
        self
    }
}

#[async_trait]
impl<State, Ctx, E> ModelMiddleware<State, Ctx> for ResolvedRouteMiddleware<Ctx, E>
where
    State: Send + Sync,
    Ctx: Send + Sync,
    E: CostEstimator,
{
    fn name(&self) -> &str {
        "workload.resolved_route"
    }

    async fn wrap_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        state: &State,
        request: ModelRequest,
        next: ModelHandler<'_, State, Ctx>,
    ) -> crate::error::Result<MiddlewareModelOutcome> {
        let call_id = request
            .correlation
            .as_ref()
            .map(|correlation| correlation.call_id.clone())
            .or_else(|| {
                ctx.active_model_call
                    .as_ref()
                    .map(|call_id| call_id.as_str().to_owned())
            });

        if let Some(call_id) = &call_id
            && is_valid_provider_id(&self.configured_provider)
        {
            ctx.emit(AgentEvent::GenerationEnriched {
                call_id: call_id.clone().into(),
                enrichment: GenerationEnrichment {
                    provider_id: Some(self.configured_provider.clone()),
                    ..Default::default()
                },
            });
        }

        let outcome = next.run(ctx, state, request).await?;
        let MiddlewareModelOutcome::Response(response) = outcome else {
            return Ok(outcome);
        };

        let route = response.resolved_route.clone();
        if let Some(route) = route.clone() {
            (self.route_sink)(&ctx.data, route);
        }

        if let Some(call_id) = call_id {
            let model = route
                .as_ref()
                .map(|route| route.model.as_str())
                .or_else(|| {
                    response
                        .resolved_model
                        .as_ref()
                        .map(|model| model.name.as_str())
                })
                .unwrap_or(&self.configured_model);
            let provider = route
                .as_ref()
                .map(|route| route.provider.as_str())
                .unwrap_or(&self.configured_provider);
            let pricing = if model.trim().to_ascii_lowercase().ends_with(":free") {
                Some(GenerationPricing {
                    input_per_mtok_usd: Some(0.0),
                    cached_input_per_mtok_usd: Some(0.0),
                    output_per_mtok_usd: Some(0.0),
                })
            } else {
                self.pricing_lookup
                    .as_ref()
                    .and_then(|lookup| lookup(model))
            };
            let usage = usage_info_from_response(&response);
            let estimated_cost_usd = if response.served_from_cache
                || usage.is_some_and(|usage| usage.charge_reported && !usage.cost_is_estimate)
            {
                None
            } else {
                usage.and_then(|usage| self.estimator.estimate(model, &usage))
            };
            let enrichment = GenerationEnrichment {
                provider_id: is_valid_provider_id(provider).then(|| provider.to_owned()),
                pricing,
                estimated_cost_usd,
            };
            if enrichment.provider_id.is_some()
                || enrichment.pricing.is_some()
                || enrichment.estimated_cost_usd.is_some()
            {
                ctx.emit(AgentEvent::GenerationEnriched {
                    call_id: call_id.into(),
                    enrichment,
                });
            }
        }

        Ok(MiddlewareModelOutcome::from(response))
    }
}
