//! Registry-owned executable capability data.

use std::sync::Arc;

use tinyagents_harness::middleware::Middleware;
use tinyagents_harness::tool::toolset::ToolSet;

/// A named bundle of instructions, tools, and middleware stored by the
/// [`CapabilityRegistry`](crate::CapabilityRegistry).
///
/// The registry type-erases this value because `Ctx` belongs to each bundle,
/// not to the registry as a whole. This type describes stored capability data;
/// installing it into a running harness is a host responsibility.
pub struct Capability<State: Send + Sync, Ctx: Send + Sync> {
    /// Stable name used to register and look up this capability.
    pub name: String,
    /// Instructions associated with the capability.
    pub instructions: Option<String>,
    /// Tools associated with the capability.
    pub toolset: Option<Arc<dyn ToolSet<State, Ctx>>>,
    /// Middleware associated with the capability.
    pub middleware: Vec<Arc<dyn Middleware<State, Ctx>>>,
}

impl<State: Send + Sync, Ctx: Send + Sync> Capability<State, Ctx> {
    /// Creates a capability with the given stable name and no attachments.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            instructions: None,
            toolset: None,
            middleware: Vec::new(),
        }
    }

    /// Sets the instructions associated with this capability.
    #[must_use]
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    /// Sets the toolset associated with this capability.
    #[must_use]
    pub fn with_toolset(mut self, toolset: Arc<dyn ToolSet<State, Ctx>>) -> Self {
        self.toolset = Some(toolset);
        self
    }

    /// Appends middleware to this capability.
    #[must_use]
    pub fn with_middleware(mut self, middleware: Arc<dyn Middleware<State, Ctx>>) -> Self {
        self.middleware.push(middleware);
        self
    }
}

impl<State: Send + Sync, Ctx: Send + Sync> Clone for Capability<State, Ctx> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            instructions: self.instructions.clone(),
            toolset: self.toolset.clone(),
            middleware: self.middleware.clone(),
        }
    }
}

impl<State: Send + Sync, Ctx: Send + Sync> std::fmt::Debug for Capability<State, Ctx> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Capability")
            .field("name", &self.name)
            .field("instructions", &self.instructions)
            .field("has_toolset", &self.toolset.is_some())
            .field("middleware_count", &self.middleware.len())
            .finish()
    }
}
