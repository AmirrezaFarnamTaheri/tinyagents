//! Explicit callbacks carried by a single agent or turn.
use std::sync::Arc;

use tinyagents_harness::cost::TurnCost;
use tinyagents_harness::host::LearningSink;

use crate::stop::StopHook;
use crate::tool::ToolHook;

/// Tool, learning, and stop callbacks attached to one scope.
#[derive(Clone, Default)]
pub struct HookScope<Ctx = (), Metrics = TurnCost> {
    tools: Vec<Arc<dyn ToolHook<Ctx>>>,
    learning: Vec<Arc<dyn LearningSink>>,
    stop: Vec<Arc<dyn StopHook<Metrics>>>,
}

impl<Ctx, Metrics> HookScope<Ctx, Metrics> {
    /// Append a tool hook, preserving registration order.
    pub fn push_tool(&mut self, hook: Arc<dyn ToolHook<Ctx>>) { self.tools.push(hook); }
    /// Append a learning sink, preserving registration order.
    pub fn push_learning(&mut self, hook: Arc<dyn LearningSink>) { self.learning.push(hook); }
    /// Append a stop policy, preserving registration order.
    pub fn push_stop(&mut self, hook: Arc<dyn StopHook<Metrics>>) { self.stop.push(hook); }
    /// Tool hooks in registration order.
    pub fn tool_hooks(&self) -> &[Arc<dyn ToolHook<Ctx>>] { &self.tools }
    /// Learning sinks in registration order.
    pub fn learning_hooks(&self) -> &[Arc<dyn LearningSink>] { &self.learning }
    /// Stop policies in registration order.
    pub fn stop_hooks(&self) -> &[Arc<dyn StopHook<Metrics>>] { &self.stop }
    /// Whether this scope has no callbacks.
    pub fn is_empty(&self) -> bool { self.tools.is_empty() && self.learning.is_empty() && self.stop.is_empty() }
}

#[cfg(test)]
#[path = "scope_tests.rs"]
mod tests;
