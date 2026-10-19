//! Declarative types for the high-level [`WorkloadRouter`](super::WorkloadRouter): a
//! [`WorkloadRoute`] (one named tier) and the router that owns an ordered set of
//! them.

use serde::{Deserialize, Serialize};

use tinyinference_llm::model::CapabilitySet;

/// Host-provided facts used by the generic workload router.
///
/// Implementations should expose configuration and catalog decisions without
/// moving credentials, billing, egress, or user-facing policy into TinyAgents.
pub trait RouteSource {
    /// The explicitly configured provider route for `role`, if any.
    fn configured_route_for_role(&self, role: &str) -> Option<&str>;
    /// The host-resolved primary cloud route, including any legacy or
    /// fail-closed configuration behavior.
    fn primary_cloud_route(&self) -> &str;
    /// The configured default model selection.
    fn default_model(&self) -> Option<&str>;
    /// Classify whether the default model is an explicit provider route,
    /// managed catalog model, or other model identifier.
    fn classify_default_model_route(&self, model: &str) -> DefaultModelRoute;
    /// Whether a resolved provider route is host-managed.
    fn is_managed_route(&self, route: &str) -> bool;
    /// The concrete managed default model, if the host has one available.
    fn managed_default_model(&self) -> Option<&str>;
    /// Resolve a host-retired tier spelling to its canonical workload role.
    fn legacy_tier_role(&self, tier: &str) -> Option<&str>;
}

/// Host classification of a configured default model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultModelRoute {
    /// A configured provider route eligible to serve chat-tier roles.
    ExplicitProvider,
    /// A model selected from a host-managed catalog.
    ManagedCatalog,
    /// A model identifier that must not alter provider routing.
    Other,
}

/// Metadata for a role that can implicitly use a primary cloud route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudFallbackRole {
    /// Canonical workload role being routed.
    pub role: &'static str,
    /// Role/configuration name the host can present as the override point.
    pub override_role: &'static str,
}

/// Why a route selected a fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FallbackReason {
    /// The role had no explicit route and used the host's primary cloud route.
    ImplicitCloudFallback,
}

/// Structured routing context for host-specific diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FallbackDiagnostics {
    /// Canonical workload role being routed.
    pub role: String,
    /// The requested role route, if explicitly set.
    pub requested_route: Option<String>,
    /// Host-resolved fallback route selected for the role.
    pub fallback_route: String,
    /// Generic reason for selecting this fallback.
    pub reason: FallbackReason,
    /// Role/configuration name that can override the fallback.
    pub override_role: String,
}

/// One declarative **workload route**: a stable alias (e.g. `reasoning-v1`) that
/// resolves to a concrete registered model, plus the capability gate a request
/// routed here must satisfy and an ordered same-family fallback chain of sibling
/// aliases.
///
/// A route is pure metadata — it names a model rather than owning one, so the
/// same route table can be declared once and projected onto any
/// [`CapabilityRegistry`](crate::CapabilityRegistry) /
/// [`ModelRegistry`](tinyagents_harness::ModelRegistry) that has registered
/// those model names. This is what lets a host describe its tiered routing
/// (workload aliases → concrete BYOK/managed/local models) declaratively and hand
/// it to the crate, instead of re-implementing alias resolution + fallback
/// ordering + capability gating by hand.
///
/// # Fields
/// - `alias` — the routable name callers dispatch to (the registry model name the
///   route is registered under).
/// - `model` — the concrete provider model id this alias forwards to. Often equal
///   to `alias` when the backend resolves the alias server-side (a managed tier),
///   or a real model id when the router itself is the resolver (BYOK).
/// - `requires` — the [`CapabilitySet`] stamped on every request routed to this
///   alias, so an unfit candidate is rejected/skipped pre-dispatch. [`Default`]
///   (require nothing) is the common case.
/// - `fallbacks` — ordered sibling aliases to try when this route errors. Each
///   entry should itself be a registered route so the crate can resolve it. Kept
///   short (0–2 same-family alternates) to bound cross-route fan-out.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadRoute {
    /// The routable alias (registry model name) callers dispatch to.
    pub alias: String,
    /// The concrete provider model id this alias forwards to.
    pub model: String,
    /// Capabilities a request routed to this alias must satisfy.
    #[serde(default, skip_serializing_if = "is_default_capabilities")]
    pub requires: CapabilitySet,
    /// Ordered sibling aliases to fall back to when this route errors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallbacks: Vec<String>,
}

impl WorkloadRoute {
    /// A route mapping `alias` → `model` with no capability gate and no fallbacks.
    pub fn new(alias: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            model: model.into(),
            requires: CapabilitySet::default(),
            fallbacks: Vec::new(),
        }
    }

    /// Sets the capability gate for requests routed to this alias.
    #[must_use]
    pub fn requiring(mut self, requires: CapabilitySet) -> Self {
        self.requires = requires;
        self
    }

    /// Sets the ordered same-family fallback aliases for this route.
    #[must_use]
    pub fn with_fallbacks(
        mut self,
        fallbacks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.fallbacks = fallbacks.into_iter().map(Into::into).collect();
        self
    }

    /// Whether this route imposes any capability requirement.
    pub(super) fn has_capability_gate(&self) -> bool {
        self.requires != CapabilitySet::default()
    }
}

/// Skip serializing a `requires` field that requires nothing (the common case),
/// keeping declared route tables compact.
fn is_default_capabilities(set: &CapabilitySet) -> bool {
    *set == CapabilitySet::default()
}
