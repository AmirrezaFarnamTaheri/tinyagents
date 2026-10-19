//! Role-keyed model construction and turn model assembly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tinyinference_llm::model::{
    ChatModel, ErrorSlotModel, ModelErrorSlot, ResolvedModelRoute, RouteRecordingModel,
};
use tinyinference_llm::model::workload::WORKLOAD_ROUTE_TIERS;

use crate::error::Result;
use crate::host::model_resolver::PrebuiltModelResolver;

/// Construction options passed through without interpretation by the harness.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BuildOpts {
    /// Whether the provider should expose native tool support.
    pub native_tools: bool,
    /// Optional provider temperature override.
    pub temperature: Option<f64>,
    /// Host-supplied egress accounting label.
    pub egress_tag: Option<String>,
}

/// Why a model is being constructed. Hosts can apply distinct policies to
/// primary, workload-tier, and summarization models without sharing policy
/// details with the harness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelPurpose {
    /// Model selected for the primary turn.
    Primary,
    /// A workload route alias such as `hint:chat`.
    WorkloadTier(String),
    /// Model reserved for transcript summarization.
    Summarization,
}

/// Neutral facts a host may inspect before building one model.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelBuildRequest {
    /// Host-defined role key.
    pub role: String,
    /// Optional host route alias, separate from the role and model id.
    pub route: Option<String>,
    /// Construction purpose.
    pub purpose: ModelPurpose,
    /// Optional exact provider model id.
    pub model_pin: Option<String>,
    /// Provider construction options.
    pub opts: BuildOpts,
}

impl ModelBuildRequest {
    fn new(role: impl Into<String>, purpose: ModelPurpose) -> Self {
        Self {
            role: role.into(),
            route: None,
            purpose,
            model_pin: None,
            opts: BuildOpts::default(),
        }
    }
}

/// Host policy hook called before every model construction.
pub trait ModelBuildGate {
    /// Authorize constructing the model described by `request`.
    fn check(&self, request: &ModelBuildRequest) -> Result<()>;
}

/// Host implementation that selects and constructs a provider model.
pub trait ModelFactory<State: Send + Sync> {
    /// Construct a model and report the concrete route selected by the host.
    fn build(
        &self,
        request: &ModelBuildRequest,
    ) -> Result<(Arc<dyn ChatModel<State>>, ResolvedModelRoute)>;
}

/// Configures construction of a primary, workload models, and summarizer.
pub struct TurnModelSetBuilder<'a, State: Send + Sync, F, G> {
    factory: &'a F,
    gate: &'a G,
    primary: ModelBuildRequest,
    tiers: Vec<ModelBuildRequest>,
    summarization_role: Option<String>,
    summarization_pin: Option<String>,
    summarization_opts: Option<BuildOpts>,
    marker: std::marker::PhantomData<fn() -> State>,
}

impl<'a, State: Send + Sync + 'static, F: ModelFactory<State>, G: ModelBuildGate>
    TurnModelSetBuilder<'a, State, F, G>
{
    /// Configures the default workload routes with `primary_role`.
    pub fn new(factory: &'a F, gate: &'a G, primary_role: impl Into<String>) -> Self {
        let primary = ModelBuildRequest::new(primary_role, ModelPurpose::Primary);
        let tiers = WORKLOAD_ROUTE_TIERS
            .iter()
            .map(|route| {
                let mut request = ModelBuildRequest::new(
                    *route,
                    ModelPurpose::WorkloadTier((*route).to_owned()),
                );
                request.route = Some((*route).to_owned());
                request
            })
            .collect();
        Self {
            factory,
            gate,
            primary,
            tiers,
            summarization_role: None,
            summarization_pin: None,
            summarization_opts: None,
            marker: std::marker::PhantomData,
        }
    }

    /// Begins a turn model set using the canonical workload routes.
    pub fn builder(factory: &'a F, gate: &'a G, primary_role: impl Into<String>) -> Self {
        Self::new(factory, gate, primary_role)
    }

    /// Pins only the primary model; workload-tier requests remain independent.
    #[must_use]
    pub fn primary_model_pin(mut self, model_pin: impl Into<String>) -> Self {
        self.primary.model_pin = Some(model_pin.into());
        self
    }

    /// Sets construction options for the primary model.
    #[must_use]
    pub fn primary_opts(mut self, opts: BuildOpts) -> Self {
        self.primary.opts = opts;
        self
    }

    /// Replaces default workload routes with caller-defined route aliases.
    /// Each route name is also its default role key; use
    /// [`workload_requests`](Self::workload_requests) for distinct keys.
    #[must_use]
    pub fn workload_tiers<I, S>(mut self, tiers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tiers = tiers
            .into_iter()
            .map(|route| {
                let route = route.into();
                let mut request = ModelBuildRequest::new(
                    route.clone(),
                    ModelPurpose::WorkloadTier(route.clone()),
                );
                request.route = Some(route);
                request
            })
            .collect();
        self
    }

    /// Replaces workload routes with fully specified neutral requests.
    #[must_use]
    pub fn workload_requests(mut self, tiers: Vec<ModelBuildRequest>) -> Self {
        self.tiers = tiers;
        self
    }

    /// Omits workload routes, for example when the host supplies an injected
    /// primary model and should not construct unrelated external providers.
    #[must_use]
    pub fn without_workload_tiers(mut self) -> Self {
        self.tiers.clear();
        self
    }

    /// Selects an explicit summarization role and optional model pin.
    #[must_use]
    pub fn summarization(mut self, role: impl Into<String>, model_pin: Option<String>) -> Self {
        self.summarization_role = Some(role.into());
        self.summarization_pin = model_pin;
        self
    }

    /// Sets options for the summarization model. By default it inherits the
    /// primary model's options.
    #[must_use]
    pub fn summarization_opts(mut self, opts: BuildOpts) -> Self {
        self.summarization_opts = Some(opts);
        self
    }

    /// Builds all requested models. Primary and summarization failures are
    /// fatal; failed workload routes are logged and omitted.
    pub fn build(self) -> Result<TurnModelSet<State>> {
        self.gate.check(&self.primary)?;
        let (primary_model, primary_route) = self.factory.build(&self.primary)?;
        let primary = Arc::new(RouteRecordingModel::new(
            primary_model,
            primary_route.clone(),
        ));
        let primary_error_slot = Arc::new(Mutex::new(None));
        let lead = Arc::new(ErrorSlotModel::new(
            Arc::clone(&primary) as Arc<dyn ChatModel<State>>,
            Arc::clone(&primary_error_slot),
        ));

        let mut routes = Vec::new();
        let mut route_errors = HashMap::new();
        for request in self.tiers {
            if self
                .primary
                .model_pin
                .as_deref()
                .is_some_and(|pin| request.model_pin.as_deref() == Some(pin))
            {
                tracing::debug!(route = ?request.route, "skipping workload model matching the primary pin");
                continue;
            }
            if let Err(error) = self.gate.check(&request) {
                tracing::debug!(route = ?request.route, %error, "skipping gated workload model");
                continue;
            }
            match self.factory.build(&request) {
                Ok((model, route)) => {
                    let slot = Arc::new(Mutex::new(None));
                    let recorded = Arc::new(RouteRecordingModel::new(model, route));
                    let wrapped = Arc::new(ErrorSlotModel::new(
                        recorded as Arc<dyn ChatModel<State>>,
                        Arc::clone(&slot),
                    ));
                    let name = request.route.clone().unwrap_or_else(|| request.role.clone());
                    route_errors.insert(name.clone(), slot);
                    routes.push((name, wrapped as Arc<dyn ChatModel<State>>));
                }
                Err(error) => {
                    tracing::debug!(route = ?request.route, %error, "skipping unavailable workload model");
                }
            }
        }

        let summary_request = ModelBuildRequest {
            role: self
                .summarization_role
                .unwrap_or_else(|| self.primary.role.clone()),
            route: None,
            purpose: ModelPurpose::Summarization,
            model_pin: self.summarization_pin.or(self.primary.model_pin),
            opts: self.summarization_opts.unwrap_or(self.primary.opts),
        };
        self.gate.check(&summary_request)?;
        let (summary_model, summary_route) = self.factory.build(&summary_request)?;
        let summary_error_slot = Arc::new(Mutex::new(None));
        let summarizer = Arc::new(ErrorSlotModel::new(
            Arc::new(RouteRecordingModel::new(summary_model, summary_route.clone()))
                as Arc<dyn ChatModel<State>>,
            Arc::clone(&summary_error_slot),
        ));

        let resolver = PrebuiltModelResolver::new(
            primary as Arc<dyn ChatModel<State>>,
            routes,
        )
        .with_lead_model(lead as Arc<dyn ChatModel<State>>);

        Ok(TurnModelSet {
            resolver,
            primary_route,
            summary_route,
            summarizer,
            primary_error_slot,
            summary_error_slot,
            route_errors,
        })
    }
}

/// Built models for one turn configuration.
pub struct TurnModelSet<State: Send + Sync> {
    resolver: PrebuiltModelResolver<State>,
    primary_route: ResolvedModelRoute,
    summary_route: ResolvedModelRoute,
    summarizer: Arc<dyn ChatModel<State>>,
    primary_error_slot: ModelErrorSlot,
    summary_error_slot: ModelErrorSlot,
    route_errors: HashMap<String, ModelErrorSlot>,
}

impl<State: Send + Sync + 'static> TurnModelSet<State> {
    /// Starts building a model set using the standard workload aliases.
    pub fn builder<'a, F, G>(
        factory: &'a F,
        gate: &'a G,
        primary_role: impl Into<String>,
    ) -> TurnModelSetBuilder<'a, State, F, G>
    where
        F: ModelFactory<State>,
        G: ModelBuildGate,
    {
        TurnModelSetBuilder::new(factory, gate, primary_role)
    }

    /// Borrows the prebuilt route resolver.
    pub fn resolver(&self) -> &PrebuiltModelResolver<State> {
        &self.resolver
    }

    /// Primary model's concrete route.
    pub fn primary_route(&self) -> &ResolvedModelRoute {
        &self.primary_route
    }

    /// Summarization model's concrete route.
    pub fn summarizer_route(&self) -> &ResolvedModelRoute {
        &self.summary_route
    }

    /// Model reserved for summarization.
    pub fn summarizer(&self) -> Arc<dyn ChatModel<State>> {
        Arc::clone(&self.summarizer)
    }

    /// Error slot for team-lead calls routed through the primary model.
    pub fn primary_error_slot(&self) -> ModelErrorSlot {
        Arc::clone(&self.primary_error_slot)
    }

    /// Error slot for summarization calls.
    pub fn summarizer_error_slot(&self) -> ModelErrorSlot {
        Arc::clone(&self.summary_error_slot)
    }

    /// Error slot for a successfully built workload route.
    pub fn workload_error_slot(&self, route: &str) -> Option<ModelErrorSlot> {
        self.route_errors.get(route).cloned()
    }
}

/// Host-owned context passed to the Claude subprocess provider constructors.
#[cfg(feature = "claude-code")]
pub struct SubprocessProviderContext {
    /// Working directory used by Claude Code.
    pub workspace_dir: std::path::PathBuf,
    /// Action/project directory used by Claude Code.
    pub action_dir: std::path::PathBuf,
    /// Host-owned Claude Agent SDK settings.
    pub claude_agent_sdk: crate::providers::claude_agent_sdk::ClaudeAgentSdkConfig,
    /// Optional host-owned MCP endpoint provider.
    pub mcp_provider: Option<Arc<dyn crate::providers::claude_code::driver::McpEndpointProvider>>,
}

/// Builds one supported Claude subprocess route, returning `None` for other
/// provider kinds. Hosts must run their [`ModelBuildGate`] before calling this
/// function; it never reads host credentials or modifies process environment.
#[cfg(feature = "claude-code")]
pub fn build_claude_subprocess_model(
    route: &tinyinference_providers::route::ProviderRoute,
    context: &SubprocessProviderContext,
) -> anyhow::Result<Option<Arc<dyn ChatModel<()>>>> {
    use crate::providers::{claude_agent_sdk::ClaudeAgentSdkProvider, claude_code::ClaudeCodeProvider};
    use tinyinference_providers::route::RouteKind;

    let model: Arc<dyn ChatModel<()>> = match route.kind() {
        RouteKind::ClaudeCode => {
            let mut provider = ClaudeCodeProvider::from_env(
                route.model.clone(),
                context.workspace_dir.clone(),
                context.action_dir.clone(),
            )?;
            if let Some(mcp) = &context.mcp_provider {
                provider = provider.with_mcp_provider(Arc::clone(mcp));
            }
            Arc::new(provider)
        }
        RouteKind::ClaudeAgentSdk => Arc::new(ClaudeAgentSdkProvider::for_model(
            context.claude_agent_sdk.clone(),
            route.model.clone(),
        )),
        _ => return Ok(None),
    };
    Ok(Some(model))
}

#[cfg(test)]
#[path = "model_factory_tests.rs"]
mod tests;
