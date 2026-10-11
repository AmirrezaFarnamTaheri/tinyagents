//! One-shot instruction delivery to model-request copies.
mod types;
pub use types::{EphemeralInstructionMiddleware, EphemeralInstructionQueue};

use crate::context::RunContext;
use crate::error::{Result, TinyAgentsError};
use crate::middleware::{AgentRun, Middleware, push_ephemeral_instruction};
use async_trait::async_trait;
use std::sync::Arc;
use tinyinference_llm::model::ModelRequest;
use types::PendingInstructions;

impl EphemeralInstructionQueue {
    /// Create an empty producer/consumer channel.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue an instruction for this context's next model request.
    pub fn push<Ctx>(&self, context: &RunContext<Ctx>, instruction: impl Into<String>) {
        let instruction = instruction.into();
        let mut runs = self.runs.lock().unwrap_or_else(|p| p.into_inner());
        runs.entry(context.instance_id())
            .or_insert_with(|| PendingInstructions {
                lifecycle: Arc::downgrade(&context.lifecycle),
                instructions: Vec::new(),
            })
            .instructions
            .push(instruction);
    }

    /// Peek at this context's pending instructions in insertion order.
    pub(crate) fn peek_for<Ctx>(&self, context: &RunContext<Ctx>) -> Vec<String> {
        self.runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&context.instance_id())
            .map(|pending| pending.instructions.clone())
            .unwrap_or_default()
    }

    /// Consume the staged prefix that was inserted into a request.
    pub(crate) fn commit_for<Ctx>(&self, context: &RunContext<Ctx>, count: usize) {
        let mut runs = self.runs.lock().unwrap_or_else(|p| p.into_inner());
        let Some(pending) = runs.get_mut(&context.instance_id()) else {
            return;
        };
        let remove = count.min(pending.instructions.len());
        pending.instructions.drain(..remove);
        if pending.instructions.is_empty() {
            runs.remove(&context.instance_id());
        }
    }

    /// Consume this context's pending instructions in insertion order.
    pub fn drain_for<Ctx>(&self, context: &RunContext<Ctx>) -> Vec<String> {
        let instructions = self.peek_for(context);
        self.commit_for(context, instructions.len());
        instructions
    }

    /// Drain all contexts for explicit shutdown or administrative inspection.
    ///
    /// Order is preserved within each context; order between contexts is
    /// unspecified. Model delivery always uses `drain_for` instead.
    pub fn drain_all(&self) -> Vec<String> {
        self.runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain()
            .flat_map(|(_, p)| p.instructions)
            .collect()
    }

    /// Build the request-scoped delivery middleware over this queue.
    pub fn middleware(&self) -> EphemeralInstructionMiddleware {
        EphemeralInstructionMiddleware {
            queue: self.clone(),
        }
    }

    fn prune_abandoned(&self) {
        self.runs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|_, pending| pending.lifecycle.strong_count() > 0);
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> Middleware<State, Ctx>
    for EphemeralInstructionMiddleware
{
    fn name(&self) -> &str {
        "ephemeral_instructions"
    }
    fn is_observer(&self) -> bool {
        true
    }

    async fn before_agent(&self, _context: &mut RunContext<Ctx>, _state: &State) -> Result<()> {
        self.queue.prune_abandoned();
        Ok(())
    }

    async fn before_model_dispatch(
        &self,
        context: &mut RunContext<Ctx>,
        _state: &State,
        request: &mut ModelRequest,
    ) -> Result<()> {
        for instruction in self.queue.peek_for(context) {
            push_ephemeral_instruction(request, instruction, context.model_profile.as_ref());
        }
        Ok(())
    }

    async fn after_model_dispatch(
        &self,
        context: &mut RunContext<Ctx>,
        _state: &State,
        base_called: bool,
    ) -> Result<()> {
        if base_called {
            let count = self.queue.peek_for(context).len();
            self.queue.commit_for(context, count);
        }
        Ok(())
    }

    async fn after_agent(
        &self,
        context: &mut RunContext<Ctx>,
        _state: &State,
        _run: &mut AgentRun,
    ) -> Result<()> {
        self.queue.drain_for(context);
        Ok(())
    }

    async fn on_error(
        &self,
        context: &mut RunContext<Ctx>,
        _error: &TinyAgentsError,
    ) -> Result<()> {
        self.queue.drain_for(context);
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
