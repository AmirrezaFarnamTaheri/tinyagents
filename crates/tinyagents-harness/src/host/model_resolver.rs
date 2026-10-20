//! Host capability: choosing *which* model answers a turn.
//!
//! [`tinyinference_llm::model::ChatModel`] already covers *how* a model is called.
//! What the runtime cannot supply is the decision of **which** model a given
//! turn should use. That decision is product policy — route the lead agent to a
//! frontier model and its subagents to a cheap one, send background/"thinking"
//! work to a lower tier, pin a particular role to a local model — and it is
//! exactly the kind of rule that differs per host and changes without notice.
//! Encoding any of it here would bake one product's economics into a
//! redistributed crate.
//!
//! So the seam is a question, not an answer: the runtime describes the turn it
//! is about to run ([`ModelResolveRequest`]) and the host hands back a model to
//! call. The runtime never inspects the reasoning, never learns which tier it
//! got, and never caches the decision — a host is free to answer differently
//! for two identical requests (A/B routing, failover after an outage, a quota
//! that just tripped).
//!
//! # When a host supplies this
//!
//! Always. Unlike the optional sinks, `ModelResolver` is a **required**
//! capability: a turn with no model cannot run at all, so there is no
//! meaningful "absent" state to distinguish from failure. A host with exactly
//! one model wires [`FixedModelResolver`] and is done; a host with routing
//! rules implements the trait over its own policy.
//!
//! # Naming hazard — read before "fixing" this
//!
//! The crate already has [`tinyinference_llm::model::ModelRequest`], and it is a
//! **different thing**: that type is the provider call payload (messages,
//! tools, sampling parameters) handed to a model that has already been chosen.
//! The type here, [`ModelResolveRequest`], is the *routing question* asked
//! before any payload exists. The near-miss in names is unfortunate but the
//! two are not interchangeable, and renaming this one to `ModelRequest` would
//! collide outright. Keep the `Resolve` in the name.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use tinyinference_llm::model::{CapabilitySet, ChatModel};

// ── ModelResolveRequest ───────────────────────────────────────────────────────

/// The routing question: everything the host needs to pick a model for one turn.
///
/// Deliberately tiny and inert (serde + std only). It carries *identity and
/// position*, never policy: there is no "tier", no "cheap/expensive" flag, no
/// budget hint. Those are conclusions the host draws from these facts, and
/// putting them here would mean the runtime had already made the decision it is
/// supposed to be delegating.
///
/// [`model_pin`](Self::model_pin) is consistent with that rule rather than an
/// exception to it. A pin is a *declared fact* — this agent's definition names
/// this model — not a conclusion the runtime reached. Passing it along still
/// leaves the host to decide whether to honour it; see the field docs.
///
/// Fields are public so a host can pattern-match without accessor ceremony; the
/// builder methods exist for call sites that construct one inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ModelResolveRequest {
    /// Identifier of the agent about to take the turn.
    ///
    /// A plain `String` because **this crate has no `AgentId` newtype** — the
    /// RFC's signature writes `&AgentId`, but introducing one here purely for
    /// this trait would create a second identity type competing with the host's
    /// own. Ids are opaque to the runtime: never parsed, never compared against
    /// a known list, only passed through.
    pub agent_id: String,

    /// Host-defined role of the agent, when the host models roles at all.
    ///
    /// `None` means "no role supplied", which is distinct from a role whose
    /// name happens to be empty. The runtime does not interpret this string —
    /// role vocabularies are product taxonomy and differ per host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Whether this agent is the lead of its team rather than a delegate.
    ///
    /// Kept as a separate flag instead of a magic role name because leadership
    /// is *structural* — the runtime genuinely knows it from the call graph,
    /// whereas a role is something only the host can assert. It is the one
    /// routing input the runtime can supply honestly, and it is the split most
    /// hosts key on first (lead model vs subagent model).
    #[serde(default)]
    pub is_team_lead: bool,

    /// Exact model id the agent's definition pinned, if any.
    ///
    /// Distinct from [`role`](Self::role) and the distinction is the whole
    /// point: a role is a **host taxonomy** ("chat", "background", "thinking"),
    /// whereas this is a **concrete model id** (`claude-3-5-sonnet`, a BYOK id,
    /// a local model name). Before this field existed, `role` was the only
    /// string on the request, so a wiring author needing a pin honoured would
    /// naturally put the model id there — and a host resolver reasonably
    /// treating `role` as a role vocabulary would fail to recognise it and fall
    /// back to a default. The pin would be dropped silently, which is precisely
    /// the ambiguity this field removes.
    ///
    /// **Advisory, not binding.** The host decides whether it can honour the
    /// pin — the model may be unconfigured, the credentials absent, the
    /// provider down, or the id simply unknown — and is free to resolve
    /// something else or return an error. Keeping that judgement host-side is
    /// deliberate: the runtime has no view of credentials or provider health,
    /// so a runtime that honoured pins itself would route to models the host
    /// cannot actually call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_pin: Option<String>,

    /// Advisory router hint from the definition, not a concrete model id.
    ///
    /// The host interprets this vocabulary. Middleware-selected exact model
    /// overrides suppress the hint before resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_hint: Option<String>,

    /// Capabilities required by middleware for this provider call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_capabilities: Option<CapabilitySet>,
}

impl ModelResolveRequest {
    /// A request for `agent_id` with no role, no model pin, and not a team lead.
    pub fn new(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            role: None,
            is_team_lead: false,
            model_pin: None,
            model_hint: None,
            required_capabilities: None,
        }
    }

    /// Sets the host-defined role.
    ///
    /// Pass a *role*, never a model id — see
    /// [`with_model_pin`](Self::with_model_pin) for the latter.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Sets the exact model id the agent's definition pinned.
    pub fn with_model_pin(mut self, model: impl Into<String>) -> Self {
        self.model_pin = Some(model.into());
        self
    }

    /// Supply an advisory routing hint without synthesizing a model id.
    pub fn with_model_hint(mut self, hint: impl Into<String>) -> Self {
        self.model_hint = Some(hint.into());
        self
    }

    /// Carries requirements added by request middleware to the host router.
    pub fn with_required_capabilities(mut self, capabilities: CapabilitySet) -> Self {
        self.required_capabilities = Some(capabilities);
        self
    }

    /// Marks this agent as the lead of its team.
    pub fn as_team_lead(mut self) -> Self {
        self.is_team_lead = true;
        self
    }

    /// The role as a borrowed string, or `None` when unset.
    ///
    /// A blank or whitespace-only role is reported as `None`: a host mapping a
    /// missing field to `Some("")` almost certainly means "no role", and
    /// letting that reach a routing `match` would silently select a
    /// role-specific branch keyed on the empty string.
    pub fn role(&self) -> Option<&str> {
        self.role
            .as_deref()
            .map(str::trim)
            .filter(|r| !r.is_empty())
    }

    /// The pinned model id as a borrowed string, or `None` when unset.
    ///
    /// Blank-trims for the same reason [`role`](Self::role) does, and the
    /// consequence here is worse: a definition with `model = ""` reaching a
    /// resolver as `Some("")` would be looked up as a model literally named
    /// empty-string, missing, and reported as an unroutable pin — turning a
    /// cosmetic config blank into a failed turn instead of the intended
    /// "no pin, route normally".
    pub fn model_pin(&self) -> Option<&str> {
        self.model_pin
            .as_deref()
            .map(str::trim)
            .filter(|m| !m.is_empty())
    }
}

// ── ModelResolver ─────────────────────────────────────────────────────────────

/// Resolves the model that should answer a turn.
///
/// Generic over the application `State` for the same reason
/// [`ChatModel`] is: the returned model is invoked with the host's state, so
/// the resolver has to produce a model of the *same* state type. This is why
/// the trait cannot be state-erased into a single object shared across
/// differently-typed harnesses.
///
/// Returning `Arc` (not `Box`) is deliberate: a host almost always hands back
/// one of a small fixed set of long-lived clients, and a per-turn clone of a
/// provider client would throw away its connection pool.
///
/// # Errors
///
/// Return an error only when no model can be produced — an unroutable agent,
/// an unconfigured provider, a tripped quota. There is no `Ok(None)` here on
/// purpose: unlike the optional capabilities, "no model" is never a normal
/// state the runtime can proceed past, so it must not be expressible as a
/// success.
#[async_trait]
pub trait ModelResolver<State: Send + Sync>: Send + Sync {
    /// Chooses the model for the turn described by `req`.
    ///
    /// Called once per model call, not once per session — a host may return
    /// different models across the turns of a single conversation (escalating
    /// after a failure, downgrading once a budget tightens). Implementations
    /// should therefore be cheap and must not assume the answer is memoized by
    /// the caller.
    async fn resolve(&self, req: &ModelResolveRequest) -> Result<Arc<dyn ChatModel<State>>>;

    /// Resolves an exact configured name without falling back to a default.
    /// Used by the generic runtime to walk an explicit fallback chain. Hosts
    /// with dynamic routing may leave this unsupported; named fallbacks then
    /// cannot be selected through that resolver.
    async fn resolve_named(&self, _name: &str) -> Result<Option<Arc<dyn ChatModel<State>>>> {
        Ok(None)
    }

    /// Returns a cheap synchronous preview for middleware that shapes a
    /// request from the target model profile. Network-backed resolvers should
    /// leave this unavailable and use their normal resolution path.
    fn preview(&self, _req: &ModelResolveRequest) -> Option<Arc<dyn ChatModel<State>>> {
        None
    }

    /// The selected configured name, when the resolver can identify it
    /// without changing its answer. This carries observability and fallback
    /// identity alongside the executable model.
    fn selected_name(&self, _req: &ModelResolveRequest) -> Option<String> {
        None
    }
}

// ── FixedModelResolver ────────────────────────────────────────────────────────

/// The default resolver: always returns the same model, whatever is asked.
///
/// This is the honest single-model case, not a stub — a host with one
/// configured model has genuinely finished routing. It ignores every field of
/// [`ModelResolveRequest`], which is the correct behaviour rather than a
/// limitation to work around: a host that wants role-based or lead/subagent
/// routing implements [`ModelResolver`] itself instead of teaching this type
/// about product tiers.
pub struct FixedModelResolver<State: Send + Sync> {
    model: Arc<dyn ChatModel<State>>,
    name: Option<String>,
}

impl<State: Send + Sync> FixedModelResolver<State> {
    /// Wraps `model` as the answer to every resolution request.
    pub fn new(model: Arc<dyn ChatModel<State>>) -> Self {
        Self { model, name: None }
    }

    /// Wraps one model while preserving its configured runtime name.
    pub fn named(name: impl Into<String>, model: Arc<dyn ChatModel<State>>) -> Self {
        Self {
            model,
            name: Some(name.into()),
        }
    }

    /// The wrapped model.
    pub fn model(&self) -> &Arc<dyn ChatModel<State>> {
        &self.model
    }
}

// Hand-written: `#[derive(Clone)]` would demand `State: Clone`, which is wrong —
// only the `Arc` is cloned and `State` never appears by value.
impl<State: Send + Sync> Clone for FixedModelResolver<State> {
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            name: self.name.clone(),
        }
    }
}

// Hand-written for the same reason, and because `dyn ChatModel` is not `Debug`.
impl<State: Send + Sync> std::fmt::Debug for FixedModelResolver<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedModelResolver").finish_non_exhaustive()
    }
}

#[async_trait]
impl<State: Send + Sync> ModelResolver<State> for FixedModelResolver<State> {
    async fn resolve(&self, _req: &ModelResolveRequest) -> Result<Arc<dyn ChatModel<State>>> {
        Ok(Arc::clone(&self.model))
    }

    async fn resolve_named(&self, name: &str) -> Result<Option<Arc<dyn ChatModel<State>>>> {
        let matches = self.name.as_deref() == Some(name)
            || self
                .model
                .profile()
                .and_then(|profile| profile.model.as_deref())
                == Some(name);
        Ok(matches.then(|| Arc::clone(&self.model)))
    }

    fn preview(&self, _req: &ModelResolveRequest) -> Option<Arc<dyn ChatModel<State>>> {
        Some(Arc::clone(&self.model))
    }

    fn selected_name(&self, _req: &ModelResolveRequest) -> Option<String> {
        self.name.clone().or_else(|| {
            self.model
                .profile()
                .and_then(|profile| profile.model.clone())
        })
    }
}

/// Resolves from models that a host has already constructed for one turn.
///
/// Team leads always use `lead` (the turn's selected model); delegated agents
/// first resolve an exact definition pin, then a `hint:<name>` route, and use
/// `primary` when neither route is available. Model construction and route
/// names remain host-owned. In particular, this resolver does not inspect
/// credentials, capabilities, or provider state.
pub struct PrebuiltModelResolver<State: Send + Sync> {
    primary: Arc<dyn ChatModel<State>>,
    primary_name: Option<String>,
    lead: Arc<dyn ChatModel<State>>,
    routes: std::collections::HashMap<String, Arc<dyn ChatModel<State>>>,
}

impl<State: Send + Sync> PrebuiltModelResolver<State> {
    /// Creates a resolver using `primary` as both the lead model and fallback.
    pub fn new<I>(primary: Arc<dyn ChatModel<State>>, routes: I) -> Self
    where
        I: IntoIterator<Item = (String, Arc<dyn ChatModel<State>>)>,
    {
        Self {
            lead: Arc::clone(&primary),
            primary,
            primary_name: None,
            routes: routes.into_iter().collect(),
        }
    }

    /// Sets a separately wrapped model for team leads.
    pub fn with_lead_model(mut self, lead: Arc<dyn ChatModel<State>>) -> Self {
        self.lead = lead;
        self
    }

    /// Gives the primary model its exact configured name for fallback lookup.
    pub fn with_primary_name(mut self, name: impl Into<String>) -> Self {
        self.primary_name = Some(name.into());
        self
    }
}

impl<State: Send + Sync> std::fmt::Debug for PrebuiltModelResolver<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrebuiltModelResolver")
            .field("route_count", &self.routes.len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<State: Send + Sync> ModelResolver<State> for PrebuiltModelResolver<State> {
    async fn resolve(&self, req: &ModelResolveRequest) -> Result<Arc<dyn ChatModel<State>>> {
        if req.is_team_lead {
            return Ok(Arc::clone(&self.lead));
        }

        let hinted_route = req
            .model_hint
            .as_deref()
            .map(str::trim)
            .filter(|hint| !hint.is_empty())
            .map(|hint| format!("hint:{hint}"));
        let route = req.model_pin().or(hinted_route.as_deref());
        Ok(route
            .and_then(|name| self.routes.get(name))
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.primary)))
    }

    async fn resolve_named(&self, name: &str) -> Result<Option<Arc<dyn ChatModel<State>>>> {
        if self.primary_name.as_deref() == Some(name) {
            return Ok(Some(Arc::clone(&self.primary)));
        }
        Ok(self.routes.get(name).cloned())
    }

    fn preview(&self, req: &ModelResolveRequest) -> Option<Arc<dyn ChatModel<State>>> {
        if req.is_team_lead {
            return Some(Arc::clone(&self.lead));
        }
        let hinted_route = req
            .model_hint
            .as_deref()
            .map(str::trim)
            .filter(|hint| !hint.is_empty())
            .map(|hint| format!("hint:{hint}"));
        let route = req.model_pin().or(hinted_route.as_deref());
        Some(
            route
                .and_then(|name| self.routes.get(name))
                .cloned()
                .unwrap_or_else(|| Arc::clone(&self.primary)),
        )
    }

    fn selected_name(&self, req: &ModelResolveRequest) -> Option<String> {
        let hinted_route = req
            .model_hint
            .as_deref()
            .map(str::trim)
            .filter(|hint| !hint.is_empty())
            .map(|hint| format!("hint:{hint}"));
        let route = req.model_pin().or(hinted_route.as_deref());
        if let Some(name) = route.filter(|name| self.routes.contains_key(*name)) {
            return Some(name.to_owned());
        }
        self.primary_name.clone().or_else(|| {
            self.primary
                .profile()
                .and_then(|profile| profile.model.clone())
        })
    }
}

#[cfg(test)]
#[path = "model_resolver_tests.rs"]
mod tests;
