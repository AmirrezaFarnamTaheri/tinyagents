//! Rewrite of bare packed-tool names into the router tool.

use std::collections::HashSet;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde_json::{Value, json};

use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::Result as TaResult;
use tinyagents_harness::middleware::Middleware;
use tinyinference_llm::tool::ToolCall;

use super::PackedToolRouter;

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
