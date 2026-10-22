//! [`CliRpcOnlyMiddleware`]: refuse CLI/RPC-only tools inside the autonomous
//! agent loop.

use std::sync::Arc;

use async_trait::async_trait;
use tinyinference_llm::tool::ToolCall;
use tinytools::{Tool, ToolResult, ToolScope};

use crate::context::RunContext;
use crate::error::Result;
use crate::middleware::{MiddlewareToolOutcome, ToolHandler, ToolMiddleware};

/// `wrap_tool`: refuse a tool whose scope is [`ToolScope::CliRpcOnly`] inside
/// the autonomous agent loop. A CLI/RPC-only tool (a phone call, say) is
/// reachable by an explicit invocation, never from the model loop. The
/// restriction is intrinsic to the tool, not the session, so a host installs
/// this on every path (channel, session, sub-agent).
///
/// The refusal is a model-consumable error result; the tool never runs.
pub struct CliRpcOnlyMiddleware {
    tool_sets: Vec<Arc<Vec<Box<dyn Tool>>>>,
}

impl CliRpcOnlyMiddleware {
    /// Build the guard over the registered tool sets it resolves names in.
    #[must_use]
    pub fn new(tool_sets: Vec<Arc<Vec<Box<dyn Tool>>>>) -> Self {
        Self { tool_sets }
    }

    /// Whether the registered tool called `name` is scoped to CLI/RPC only. An
    /// unregistered name is not.
    #[must_use]
    pub fn is_cli_rpc_only(&self, name: &str) -> bool {
        self.tool_sets
            .iter()
            .flat_map(|set| set.iter())
            .find(|t| t.name() == name)
            .is_some_and(|t| t.scope() == ToolScope::CliRpcOnly)
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ToolMiddleware<State, Ctx> for CliRpcOnlyMiddleware {
    fn name(&self) -> &str {
        "cli_rpc_only"
    }

    async fn wrap_tool(
        &self,
        ctx: &RunContext<Ctx>,
        state: &State,
        call: ToolCall,
        next: ToolHandler<'_, State, Ctx>,
    ) -> Result<MiddlewareToolOutcome> {
        if self.is_cli_rpc_only(&call.name) {
            tracing::warn!(
                tool = call.name.as_str(),
                "[tinyagents::mw] tool scope is CliRpcOnly — denied in agent loop"
            );
            let content = format!(
                "Tool '{}' is only available via explicit CLI/RPC invocation, not in the autonomous agent loop.",
                call.name
            );
            return Ok(MiddlewareToolOutcome::Result(ToolResult::error(content)));
        }
        next.run(ctx, state, call).await
    }
}

#[cfg(test)]
#[path = "cli_rpc_only_tests.rs"]
mod tests;
