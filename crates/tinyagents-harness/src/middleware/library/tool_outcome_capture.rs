//! Captures tool outcomes before the agent loop folds them into transcript
//! messages that no longer carry success, arguments, or elapsed time.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use tinyinference_llm::tool::ToolCall;
use tinytools::ToolResult;

use crate::context::RunContext;
use crate::error::Result;
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::turn_outcome::{ToolCallOutcome, ToolOutcomeSink};

#[derive(Default)]
pub(super) struct ToolCallCapture {
    started: Mutex<HashMap<(u64, String), (Instant, serde_json::Value)>>,
}

impl ToolCallCapture {
    pub(super) fn start<Ctx>(&self, ctx: &RunContext<Ctx>, call: &ToolCall) {
        if let Ok(mut started) = self.started.lock() {
            started.insert(
                (ctx.instance_id(), call.id.to_string()),
                (Instant::now(), call.arguments.clone()),
            );
        }
    }

    pub(super) fn finish<Ctx>(
        &self,
        ctx: &RunContext<Ctx>,
        invocation: &ToolInvocationIdentity,
        result: &ToolResult,
    ) -> ToolCallOutcome {
        let (duration_ms, arguments) = self
            .started
            .lock()
            .ok()
            .and_then(|mut started| {
                started.remove(&(ctx.instance_id(), invocation.call_id().to_string()))
            })
            .map(|(start, arguments)| {
                (
                    start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    arguments,
                )
            })
            .unwrap_or_default();
        ToolCallOutcome {
            call_id: invocation.call_id().to_string(),
            name: invocation.tool_name().to_owned(),
            arguments,
            success: !result.is_error,
            content: result.output(),
            duration_ms,
        }
    }

    pub(super) fn forget_run(&self, instance_id: u64) {
        if let Ok(mut started) = self.started.lock() {
            started.retain(|(run, _), _| *run != instance_id);
        }
    }
}

/// Captures tool outcomes in the shared [`ToolOutcomeSink`]. Register it before
/// middleware that changes model-visible result content so reverse-order
/// `after_tool` execution records the final text.
pub struct ToolOutcomeCaptureMiddleware {
    sink: ToolOutcomeSink,
    capture: ToolCallCapture,
}

impl ToolOutcomeCaptureMiddleware {
    /// Creates a capture middleware for the supplied shared sink.
    pub fn new(sink: ToolOutcomeSink) -> Self {
        Self {
            sink,
            capture: ToolCallCapture::default(),
        }
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> Middleware<State, Ctx> for ToolOutcomeCaptureMiddleware {
    fn name(&self) -> &str {
        "tool_outcome_capture"
    }

    async fn before_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        call: &mut ToolCall,
    ) -> Result<()> {
        self.capture.start(ctx, call);
        Ok(())
    }

    async fn after_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        invocation: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> Result<()> {
        let outcome = self.capture.finish(ctx, invocation, result);
        if let Ok(mut sink) = self.sink.lock() {
            sink.push(outcome);
        }
        Ok(())
    }

    async fn after_agent(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        _run: &mut crate::middleware::AgentRun,
    ) -> Result<()> {
        self.capture.forget_run(ctx.instance_id());
        Ok(())
    }

    async fn on_error(
        &self,
        ctx: &mut RunContext<Ctx>,
        _error: &crate::error::TinyAgentsError,
    ) -> Result<()> {
        self.capture.forget_run(ctx.instance_id());
        Ok(())
    }
}

#[cfg(test)]
#[path = "tool_outcome_capture_tests.rs"]
mod tests;
