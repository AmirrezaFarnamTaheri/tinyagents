//! Early-exit tools: a successful call to one of them pauses the loop and
//! surfaces the tool's output instead of letting the model keep going.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::Result;
use tinyagents_harness::middleware::{Middleware, ToolInvocationIdentity};
use tinyagents_harness::steering::{SteeringCommand, SteeringHandle};
use tinytools::ToolResult;

use super::types::EarlyExit;

/// One hook per run: records the first early exit and requests a pause.
#[derive(Clone)]
pub struct EarlyExitHook {
    handle: SteeringHandle,
    slot: Arc<Mutex<Option<EarlyExit>>>,
}

impl EarlyExitHook {
    /// A hook that pauses `handle` and records into a fresh slot.
    pub fn new(handle: SteeringHandle) -> Self {
        Self {
            handle,
            slot: Arc::default(),
        }
    }

    /// The captured early exit, if one fired.
    pub fn take(&self) -> Option<EarlyExit> {
        self.slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn trigger(&self, tool: &str, question: String) {
        {
            let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.is_none() {
                *slot = Some(EarlyExit {
                    tool: tool.to_string(),
                    question,
                });
            }
        }
        tracing::info!(tool, "[turn-driver] early-exit tool: requesting pause");
        self.handle.send(SteeringCommand::Pause);
    }
}

/// Fires an [`EarlyExitHook`] when a named tool returns without error.
///
/// Installed after the host's context middleware, so its `after_tool` runs
/// first (after-hooks run in reverse registration order) and records the
/// tool's output before any cap or summariser rewrites it.
pub struct EarlyExitMiddleware {
    tools: BTreeSet<String>,
    hook: EarlyExitHook,
}

impl EarlyExitMiddleware {
    /// Watches `tools`, reporting through `hook`.
    pub fn new(tools: BTreeSet<String>, hook: EarlyExitHook) -> Self {
        Self { tools, hook }
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> Middleware<State, Ctx> for EarlyExitMiddleware {
    fn name(&self) -> &str {
        "early_exit"
    }

    async fn after_tool(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &State,
        invocation: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> Result<()> {
        if !result.is_error && self.tools.contains(invocation.tool_name()) {
            self.hook
                .trigger(invocation.tool_name(), result.output_for_llm(true));
        }
        Ok(())
    }
}
