//! Generic middleware for declarative workload routes.

use std::sync::Arc;

use async_trait::async_trait;
use tinyinference_llm::model::{CapabilitySet, ModelRequest, ModelResponse};

use crate::context::RunContext;
use crate::events::AgentEvent;
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
        let response = outcome.into_response();
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
        let response = outcome.into_response();
        (self.carry)(&response);
        Ok(MiddlewareModelOutcome::from(response))
    }
}
