//! Harness runtime facade.
//!
//! [`AgentHarness`] is the durable runtime facade. Hosted roots may provide an
//! invocation-local [`InvocationRuntime`] for models, tools, and middleware;
//! authorized children inherit that exact overlay and never substitute their
//! own durable registries.
//! Hosted roots may attach an [`InvocationRuntime`] for their model, tool, and
//! middleware surface. It is invocation-local and every hosted child must
//! inherit it; a missing overlay is rejected rather than falling back to a
//! child's durable harness.
//!
//! Per-tool deadlines are enabled separately from [`RunPolicy`] through
//! [`AgentHarness::with_tool_timeout_settings`]. Expiry becomes a recoverable
//! tool-error result so the model can repair its plan, while the run wall-clock
//! limit remains the outer hard abort.
//!
//! Owns the high-level [`AgentHarness`] builder and the [`RunPolicy`] bundle
//! that wires registries, middleware, and run policy into a single ergonomic
//! entry point. The agent loop driven by this facade lives in the sibling
//! [`crate::agent_loop`] module.
//!
//! # Layout
//!
//! - `types` holds the public type definitions ([`RunPolicy`] and
//!   [`AgentHarness`]).
//! - This file holds the builder, registration, and accessor methods.
//! - `test.rs` holds focused tests for construction and registration.

mod agent;
mod types;

#[cfg(test)]
pub(crate) use agent::HostInvocationAuthority;
pub use agent::{
    AgentInvocation, AgentStream, AgentTurnRequest, HostedError, HostedErrorKind, TimeoutBound,
};
pub(crate) use agent::{ErasedHostAuthority, emit_host_progress, host_invocation_binding};
pub use types::*;

use std::sync::Arc;

use crate::cache::ResponseCache;
use crate::middleware::{
    AgentMiddleware, Middleware, MiddlewareStack, ModelMiddleware, ToolMiddleware,
};
use crate::host::ModelResolver;
use crate::tool::{ToolDispatch, ToolRegistry, ToolTimeoutSettings};
use tinytools::Tool;

/// Installs a host-selected recursion ceiling without widening the active
/// [`RunPolicy`] limit.
pub fn install_depth<State: Send + Sync, Ctx: Send + Sync>(
    harness: &mut AgentHarness<State, Ctx>,
    max_depth: usize,
) {
    let mut policy = harness.policy().clone();
    policy.limits.max_depth = policy.limits.max_depth.min(max_depth);
    harness.with_policy(policy);
}

impl<'model, State: Send + Sync, Ctx: Send + Sync> AgentHarness<'model, State, Ctx> {
    /// Creates an empty harness with default policy and no models, tools, or
    /// middleware registered.
    pub fn new() -> Self {
        Self {
            model_resolver: None,
            tools: ToolRegistry::new(),
            middleware: MiddlewareStack::new(),
            policy: RunPolicy::default(),
            tool_timeouts: None,
            response_cache: None,
            output_validator: None,
            deferred_tool_handler: None,
            toolset: None,
            loop_driver: None,
        }
    }

    /// Supplies the resolver used for direct SDK turns. Hosted invocations
    /// continue to use their invocation-local host resolver.
    pub fn with_model_resolver(
        &mut self,
        resolver: Arc<dyn ModelResolver<State> + 'model>,
    ) -> &mut Self {
        self.model_resolver = Some(resolver);
        self
    }

    /// Registers a tool, keyed by its [`Tool::name`]. Returns `&mut Self` for
    /// chaining.
    pub fn register_tool(&mut self, tool: Arc<dyn Tool>) -> &mut Self {
        self.tools.register(tool);
        self
    }

    /// Registers a schema-only external tool the host executes out of band
    /// (A2). See [`ToolRegistry::register_external`].
    pub fn register_external_tool(
        &mut self,
        schema: tinyinference_llm::tool::ToolSchema,
    ) -> &mut Self {
        self.tools.register_external(schema);
        self
    }

    /// Registers a tool whose execution needs the typed parent run.
    pub fn register_tool_dispatch(
        &mut self,
        dispatch: Arc<dyn ToolDispatch<State, Ctx>>,
    ) -> &mut Self {
        self.tools.register_dispatch(dispatch);
        self
    }

    /// Appends a lifecycle middleware to the stack. Registration order is the
    /// onion order: the first pushed middleware is the outermost layer.
    pub fn push_middleware(&mut self, middleware: Arc<dyn Middleware<State, Ctx>>) -> &mut Self {
        self.middleware.push(middleware);
        self
    }

    /// Appends middleware around the complete agent run.
    pub fn push_agent_middleware(
        &mut self,
        middleware: Arc<dyn AgentMiddleware<State, Ctx>>,
    ) -> &mut Self {
        self.middleware.push_agent_middleware(middleware);
        self
    }

    /// Appends an around-model wrap middleware ([`ModelMiddleware`]). The
    /// first-registered wrap middleware is the outermost layer; the real model
    /// call (cache + retry + fallback core) is the innermost. Returns
    /// `&mut Self` for chaining.
    pub fn push_model_middleware(
        &mut self,
        middleware: Arc<dyn ModelMiddleware<State, Ctx>>,
    ) -> &mut Self {
        self.middleware.push_model_middleware(middleware);
        self
    }

    /// Appends an around-tool wrap middleware ([`ToolMiddleware`]). The
    /// first-registered wrap middleware is the outermost layer; the real tool
    /// call is the innermost. Returns `&mut Self` for chaining.
    pub fn push_tool_middleware(
        &mut self,
        middleware: Arc<dyn ToolMiddleware<State, Ctx>>,
    ) -> &mut Self {
        self.middleware.push_tool_middleware(middleware);
        self
    }

    /// Replaces the run policy. Returns `&mut Self` for chaining.
    pub fn with_policy(&mut self, policy: RunPolicy) -> &mut Self {
        self.policy = policy;
        self
    }

    /// Installs the shared resolver used for per-tool timeout policies.
    ///
    /// Keeping this runtime concern on the harness leaves [`RunPolicy`]
    /// source-compatible for callers that construct it with struct literals.
    pub fn with_tool_timeout_settings(&mut self, settings: ToolTimeoutSettings) -> &mut Self {
        self.tool_timeouts = Some(settings);
        self
    }

    /// Returns the installed per-tool timeout settings, if any.
    pub fn tool_timeout_settings(&self) -> Option<&ToolTimeoutSettings> {
        self.tool_timeouts.as_ref()
    }

    /// Attaches a [`ResponseCache`] shared across every run this harness drives.
    ///
    /// Once attached, the agent loop computes a stable
    /// [`cache_key`][crate::cache::cache_key] for each model request
    /// and consults the cache before calling the provider. On a hit the
    /// provider is **not** invoked and the cached
    /// [`tinyinference_llm::model::ModelResponse`] is reused; on a miss the
    /// provider is called and the successful response is stored back. Whether
    /// caching is active for a given call is governed by the effective
    /// [`CachePolicy`](tinyinference_llm::cache::CachePolicy) (the per-request
    /// [`tinyinference_llm::model::ModelRequest::cache_policy`] overriding
    /// [`RunPolicy::cache`]).
    ///
    /// Because the cache lives on the harness rather than a single run, two
    /// identical requests issued across separate runs share a key, so the
    /// second run can be served entirely from cache. Returns `&mut Self` for
    /// chaining.
    pub fn with_response_cache(&mut self, cache: Arc<dyn ResponseCache>) -> &mut Self {
        self.response_cache = Some(cache);
        self
    }

    /// Returns a reference to the attached response cache, if any.
    pub fn response_cache(&self) -> Option<&Arc<dyn ResponseCache>> {
        self.response_cache.as_ref()
    }

    /// Registers an [`crate::structured::OutputValidator`] consulted after
    /// the final turn's structured extraction succeeds (A3's
    /// output-validation retry loop).
    ///
    /// The validator sees the *already schema-valid* extracted value; a
    /// `TinyAgentsError::ModelRetry` it returns is treated exactly like a
    /// schema-validation failure — re-asked, bounded by
    /// [`RunPolicy::output_retry`]. Only one validator may be installed;
    /// calling this again replaces it. Returns `&mut Self` for chaining.
    pub fn with_output_validator(
        &mut self,
        validator: Arc<dyn crate::structured::OutputValidator<State, Ctx>>,
    ) -> &mut Self {
        self.output_validator = Some(validator);
        self
    }

    /// Installs an inline [`crate::tool::DeferredToolHandler`] (A2).
    ///
    /// With a handler present, a tool batch that defers one or more calls
    /// (approval-required policy, `ApprovalRequired`/`CallDeferred`, or an
    /// external tool) is resolved by calling the handler right there and the
    /// loop continues; the caller never sees `AgentRun::deferred`. Without
    /// one, the loop exits with the pending requests for the host to resolve
    /// and resume later. Only one handler may be installed; calling this
    /// again replaces it. Returns `&mut Self` for chaining.
    pub fn with_deferred_tool_handler(
        &mut self,
        handler: Arc<dyn crate::tool::DeferredToolHandler>,
    ) -> &mut Self {
        self.deferred_tool_handler = Some(handler);
        self
    }

    /// Installs a composable [`crate::tool::toolset::ToolSet`] chain (gap
    /// B3) as an additional source of tools, consulted alongside
    /// [`Self::tools`].
    ///
    /// # What this changes
    ///
    /// - **Advertisement**: the agent loop's per-turn model-visible tool
    ///   catalogue is built by projecting this toolset's
    ///   [`crate::tool::toolset::ToolSet::tools`] (re-consulted every turn,
    ///   so a [`crate::tool::toolset::PreparedToolSet`] or
    ///   [`crate::tool::toolset::ApprovalRequiredToolSet`] in the chain can
    ///   vary what is advertised turn to turn) **in addition to** the
    ///   registry's own `Direct` schemas — a name the toolset does not
    ///   mention falls back to the registry unchanged.
    /// - **Dispatch is not automatically wired to this toolset.** The agent
    ///   loop's admission path (`agent_loop/tools.rs`) resolves calls through
    ///   [`Self::tools`] only, exactly as before this field existed. A tool
    ///   that only the toolset chain exposes must also be reachable through
    ///   the registry to be *callable* (not just advertised) — bridge it
    ///   explicitly with
    ///   [`crate::tool::toolset::ToolSetDispatchBridge`] and
    ///   [`Self::register_tool_dispatch`]. See that bridge's doc comment for
    ///   why: it requires `State: 'static, Ctx: 'static`, a bound the loop's
    ///   generic admission path deliberately does not carry (recursive
    ///   sub-agent dispatch stays callable with a borrowed, non-`'static`
    ///   `State`/`Ctx`).
    ///
    /// A caller building a fresh [`crate::tool::ToolRegistry`] separately
    /// (rather than through [`Self::register_tool`]) can pass it here
    /// directly — [`crate::tool::ToolRegistry`] implements
    /// [`crate::tool::toolset::ToolSet`] — or compose it with other
    /// toolsets via [`crate::tool::toolset::CombinedToolSet`].
    ///
    /// `None` (never calling this) leaves every existing harness's turn
    /// behavior exactly as before this field existed. Returns `&mut Self`
    /// for chaining.
    pub fn with_toolset(
        &mut self,
        toolset: Arc<dyn crate::tool::toolset::ToolSet<State, Ctx>>,
    ) -> &mut Self {
        self.toolset = Some(toolset);
        self
    }

    /// Returns the installed toolset chain, if any. See
    /// [`Self::with_toolset`].
    pub fn toolset(&self) -> Option<&Arc<dyn crate::tool::toolset::ToolSet<State, Ctx>>> {
        self.toolset.as_ref()
    }

    /// Returns a reference to the tool registry.
    pub fn tools(&self) -> &ToolRegistry<State, Ctx> {
        &self.tools
    }

    /// Returns a reference to the middleware stack.
    pub fn middleware(&self) -> &MiddlewareStack<State, Ctx> {
        &self.middleware
    }

    /// Returns a reference to the active run policy.
    pub fn policy(&self) -> &RunPolicy {
        &self.policy
    }

    /// Installs an alternate loop engine (A5), consulted by `invoke*` when
    /// [`RunPolicy::execution`] is
    /// [`LoopExecution`][crate::runtime::LoopExecution]`::Graph`. See
    /// [`crate::agent_loop::phases::LoopDriver`]. Returns `&mut Self` for
    /// chaining.
    pub fn with_loop_driver(
        &mut self,
        driver: Arc<dyn crate::agent_loop::phases::LoopDriver<State, Ctx>>,
    ) -> &mut Self {
        self.loop_driver = Some(driver);
        self
    }

    /// Returns the installed alternate loop engine, if any. See
    /// [`Self::with_loop_driver`].
    pub fn loop_driver(
        &self,
    ) -> Option<&Arc<dyn crate::agent_loop::phases::LoopDriver<State, Ctx>>> {
        self.loop_driver.as_ref()
    }
}

impl<State: Send + Sync, Ctx: Send + Sync> Default for AgentHarness<State, Ctx> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;

#[cfg(test)]
#[path = "mod_authorization_context_tests.rs"]
mod authorization_context_tests;
