//! Lifecycle hooks a host fills in with policy: the memory pack delivered to
//! every model request, the router for bare names of withheld packed tools, and
//! the token-accounting parity observer.
//!
//! Each is a generic middleware over a small host trait, so the host keeps only
//! the trait implementation (where the pack lives, which pack owns a tool,
//! whether the tool may run).

use std::collections::HashSet;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::context::RunContext;
use crate::error::Result as TaResult;
use crate::middleware::{AgentRun, Middleware};
use tinyinference_llm::model::ModelRequest;
use tinyinference_llm::tool::ToolCall;

use super::{BudgetTracker, push_ephemeral_instruction};

/// Where a turn's memory pack lives on the host's run context.
pub trait MemoryPackSource<Ctx>: Send + Sync {
    /// The text to add to each model request, or `None` when the turn has no
    /// pack.
    fn injection(&self, data: &Ctx) -> Option<String>;
}

/// Adds the turn's memory pack to every model request of the turn.
///
/// Uses [`push_ephemeral_instruction`]: the request is built from a copy of the
/// working transcript, so the pack is never committed and the persisted history
/// and the provider's cached prefix stay as they were.
pub struct MemoryPackMiddleware<Ctx, Src> {
    source: Src,
    _ctx: PhantomData<fn() -> Ctx>,
}

impl<Ctx, Src> MemoryPackMiddleware<Ctx, Src> {
    /// Build the middleware over the host's `source`.
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _ctx: PhantomData,
        }
    }
}

#[async_trait]
impl<Ctx, Src> Middleware<(), Ctx> for MemoryPackMiddleware<Ctx, Src>
where
    Ctx: Send + Sync + 'static,
    Src: MemoryPackSource<Ctx>,
{
    fn name(&self) -> &str {
        "memory_pack"
    }

    fn is_observer(&self) -> bool {
        true
    }

    async fn before_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        request: &mut ModelRequest,
    ) -> TaResult<()> {
        let Some(injection) = self.source.injection(&ctx.data) else {
            return Ok(());
        };
        tracing::trace!("[lifecycle_hooks] memory pack added to the model request");
        push_ephemeral_instruction(request, injection, ctx.model_profile.as_ref());
        Ok(())
    }
}

/// Host decision for [`PackedToolRouteMiddleware`]: which pack owns a tool name
/// and whether this turn may run it.
pub trait PackedToolRouter<Ctx>: Send + Sync {
    /// The pack id to route `tool` through, or `None` to leave the call alone
    /// (no pack owns it, the pack is not withheld, or the session blocks it).
    fn pack_for(&self, data: &Ctx, tool: &str) -> Option<String>;
}

/// Rewrite a call to a withheld packed tool's bare name into
/// `<router_tool> { "skill": <pack>, "tool": name, "args": args }`.
///
/// A pack withholds a tool's schema, not its name, so a model calls the bare
/// name as a top-level tool and the unknown-tool answer never says it exists
/// behind a pack. Left alone: a registered name, a turn without the router
/// tool, a call whose arguments failed to parse or are not an object, and any
/// name the [`PackedToolRouter`] declines. The rewrite happens before
/// admission, so the call then passes everything an explicit call passes.
pub struct PackedToolRouteMiddleware<Ctx, R> {
    registered: HashSet<String>,
    router_tool: String,
    router: R,
    _ctx: PhantomData<fn() -> Ctx>,
}

impl<Ctx, R> PackedToolRouteMiddleware<Ctx, R> {
    /// Build over the names this turn registered, the tool that runs a packed
    /// tool (`use_skill`), and the host `router`.
    pub fn new(
        registered: impl IntoIterator<Item = String>,
        router_tool: impl Into<String>,
        router: R,
    ) -> Self {
        Self {
            registered: registered.into_iter().collect(),
            router_tool: router_tool.into(),
            router,
            _ctx: PhantomData,
        }
    }

    fn route(&self, data: &Ctx, call: &mut ToolCall) -> bool
    where
        R: PackedToolRouter<Ctx>,
    {
        if call.invalid.is_some()
            || self.registered.contains(&call.name)
            || !self.registered.contains(&self.router_tool)
        {
            return false;
        }
        let Some(pack) = self.router.pack_for(data, &call.name) else {
            return false;
        };
        let args = match call.arguments {
            Value::Object(_) => call.arguments.take(),
            Value::Null => json!({}),
            _ => return false,
        };
        let tool = std::mem::replace(&mut call.name, self.router_tool.clone());
        tracing::info!(
            tool = %tool,
            skill = %pack,
            call_id = %call.id,
            "[lifecycle_hooks] routed bare packed tool call through the router tool"
        );
        call.arguments = json!({ "skill": pack, "tool": tool, "args": args });
        true
    }
}

#[async_trait]
impl<Ctx, R> Middleware<(), Ctx> for PackedToolRouteMiddleware<Ctx, R>
where
    Ctx: Send + Sync + 'static,
    R: PackedToolRouter<Ctx>,
{
    fn name(&self) -> &str {
        "packed_tool_route"
    }

    async fn before_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        call: &mut ToolCall,
    ) -> TaResult<()> {
        self.route(&ctx.data, call);
        Ok(())
    }
}

/// Compares a [`BudgetTracker`]'s token accounting against the authoritative
/// [`AgentRun::usage`] at the end of a run and logs parity or divergence.
/// Observe-only: never fails the run, never enforces a cap.
pub struct BudgetParityObserver {
    tracker: BudgetTracker,
}

impl BudgetParityObserver {
    /// Compare against `tracker`, the observe-only budget middleware's handle.
    pub fn new(tracker: BudgetTracker) -> Self {
        Self { tracker }
    }

    /// Whether the tracker's token totals match `run`'s.
    pub fn matches(&self, run: &AgentRun) -> bool {
        let crate_usage = self.tracker.snapshot().usage;
        let (l, c) = (&run.usage.usage, &crate_usage.usage);
        l.input_tokens == c.input_tokens
            && l.output_tokens == c.output_tokens
            && l.cache_read_tokens == c.cache_read_tokens
            && l.cache_creation_tokens == c.cache_creation_tokens
            && l.reasoning_tokens == c.reasoning_tokens
            && l.total_tokens == c.total_tokens
            && run.usage.calls == crate_usage.calls
    }
}

#[async_trait]
impl<Ctx: Send + Sync + 'static> Middleware<(), Ctx> for BudgetParityObserver {
    fn name(&self) -> &str {
        "budget_parity"
    }

    fn is_observer(&self) -> bool {
        true
    }

    async fn after_agent(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &(),
        run: &mut AgentRun,
    ) -> TaResult<()> {
        if self.matches(run) {
            tracing::debug!(
                calls = run.usage.calls,
                "[budget_parity] token accounting matches"
            );
        } else {
            tracing::warn!(
                local_calls = run.usage.calls,
                tracked_calls = self.tracker.snapshot().usage.calls,
                "[budget_parity] divergence: tracker token accounting differs from AgentRun.usage"
            );
        }
        Ok(())
    }
}
