//! [`CanonicalSharedToolAdapter`]: a `tinytools::Tool` over a tool resolved
//! from shared, non-cloneable registries.
//!
//! The harness executes the canonical `tinytools::Tool` contract directly. A
//! host that keeps its tools in `Arc<Vec<Box<dyn Tool>>>` sets shared across
//! sessions cannot hand the harness an owned tool, so this adapter resolves the
//! tool by name at call time, forwards spec, policy, exposure, family and
//! context, and preserves an optional [`EarlyExitHook`].

mod early_exit;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tinytools::{Tool, ToolCallOptions, ToolResult, ToolRunContext, ToolTimeout};

pub use early_exit::{EarlyExit, EarlyExitHook};

/// A boxed tool execution, as handed to a [`ToolExecScope`].
pub type ToolExecution<'a> = Pin<Box<dyn Future<Output = anyhow::Result<ToolResult>> + Send + 'a>>;

/// Wraps every execution of an adapted tool in a host's ambient scope (a
/// task-local, a tracing span, an origin stamp) without the harness knowing
/// what that scope is.
pub trait ToolExecScope: Send + Sync {
    /// Runs `execution` inside the scope. `context` is the call's run context.
    fn scope<'a>(
        &'a self,
        context: Option<&'a dyn ToolRunContext>,
        execution: ToolExecution<'a>,
    ) -> ToolExecution<'a>;
}

/// Canonical TinyTools adapter over shared tool sets.
///
/// Resolves a non-cloneable tool from the shared registry on each call and
/// preserves the early-exit control hook.
pub struct CanonicalSharedToolAdapter {
    sets: Vec<Arc<Vec<Box<dyn Tool>>>>,
    name: String,
    description: String,
    parameters_schema: serde_json::Value,
    early_exit: Option<EarlyExitHook>,
    force_deferred: bool,
    exec_scope: Option<Arc<dyn ToolExecScope>>,
}

impl std::fmt::Debug for CanonicalSharedToolAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalSharedToolAdapter")
            .field("name", &self.name)
            .field("early_exit", &self.early_exit.is_some())
            .finish_non_exhaustive()
    }
}

impl CanonicalSharedToolAdapter {
    /// Builds an adapter for the tool called `name`, snapshotting its spec.
    ///
    /// Returns `None` when no set contains a tool with that name.
    #[must_use]
    pub fn for_name(sets: Vec<Arc<Vec<Box<dyn Tool>>>>, name: &str) -> Option<Self> {
        let spec = sets
            .iter()
            .flat_map(|set| set.iter())
            .find(|tool| tool.name() == name)
            .map(|tool| tool.spec())?;
        Some(Self {
            sets,
            name: spec.name,
            description: spec.description,
            parameters_schema: spec.parameters,
            early_exit: None,
            force_deferred: false,
            exec_scope: None,
        })
    }

    /// Reports a directly exposed tool as `Deferred` (served through the
    /// search bridge). Deferral only subtracts: a `Hidden` tool stays hidden
    /// and an already `Deferred` one is unchanged.
    #[must_use]
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.force_deferred = deferred;
        self
    }

    /// Runs every execution inside `scope`.
    #[must_use]
    pub fn with_exec_scope(mut self, scope: Arc<dyn ToolExecScope>) -> Self {
        self.exec_scope = Some(scope);
        self
    }

    /// Attaches an [`EarlyExitHook`] fired after a successful call.
    #[must_use]
    pub fn with_early_exit(mut self, hook: EarlyExitHook) -> Self {
        self.early_exit = Some(hook);
        self
    }

    fn resolved_tool(&self) -> Option<&dyn Tool> {
        self.sets
            .iter()
            .flat_map(|set| set.iter())
            .find(|tool| tool.name() == self.name)
            .map(|tool| tool.as_ref())
    }
}

#[async_trait]
impl Tool for CanonicalSharedToolAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> serde_json::Value {
        self.parameters_schema.clone()
    }

    fn policy(&self) -> tinytools::ToolPolicy {
        self.resolved_tool().map(Tool::policy).unwrap_or_default()
    }

    /// Forwarded so the harness indexes `Deferred` registrations for its
    /// `tool_search` bridge instead of advertising them. Without this every
    /// registered tool reported `Direct` and the bridge stayed inert.
    ///
    /// `Hidden` is **not** forwarded. The host is the exposure policy owner:
    /// it already drops every `Hidden` registration from a wildcard belt, so a
    /// `Hidden` tool that reaches harness registration was named by hand in a
    /// belt or is a synthesised route the belt admitted. Forwarding `Hidden`
    /// made the harness advertise only a subset of the visible tools while the
    /// prompt described all of them, leaving calls the model was told about
    /// unreachable.
    fn exposure(&self) -> tinytools::ToolExposure {
        match self.resolved_tool().map(Tool::exposure) {
            Some(tinytools::ToolExposure::Deferred) => tinytools::ToolExposure::Deferred,
            Some(tinytools::ToolExposure::Direct) if self.force_deferred => {
                tinytools::ToolExposure::Deferred
            }
            _ => tinytools::ToolExposure::Direct,
        }
    }

    fn family(&self) -> Option<&str> {
        self.resolved_tool().and_then(Tool::family)
    }

    // Tool rules read these: a dropped tag lets a tag rule miss, and a
    // dropped indirect target lets a dispatcher's real target escape.
    fn tags(&self) -> Vec<String> {
        self.resolved_tool().map(Tool::tags).unwrap_or_default()
    }

    fn indirect_target(&self, args: &serde_json::Value) -> Option<tinytools::IndirectCall> {
        self.resolved_tool()
            .and_then(|tool| tool.indirect_target(args))
    }

    async fn indirect_target_async(
        &self,
        args: &serde_json::Value,
    ) -> anyhow::Result<Option<tinytools::IndirectCall>> {
        match self.resolved_tool() {
            Some(tool) => tool.indirect_target_async(args).await,
            None => Ok(None),
        }
    }

    fn injected_arguments(&self) -> Vec<tinytools::ToolInjectedArgument> {
        self.resolved_tool()
            .map(Tool::injected_arguments)
            .unwrap_or_default()
    }

    fn supports_markdown(&self) -> bool {
        self.resolved_tool().is_some_and(Tool::supports_markdown)
    }

    fn is_concurrency_safe(&self, args: &serde_json::Value) -> bool {
        self.resolved_tool()
            .is_some_and(|tool| tool.is_concurrency_safe(args))
    }

    fn timeout_policy(&self, args: &serde_json::Value) -> ToolTimeout {
        self.resolved_tool()
            .map(|tool| tool.timeout_policy(args))
            .unwrap_or(ToolTimeout::Inherit)
    }

    fn return_direct(&self) -> bool {
        self.resolved_tool().is_some_and(Tool::return_direct)
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(
            args,
            ToolCallOptions {
                prefer_markdown: true,
            },
            None,
        )
        .await
    }

    async fn prepare_authorization(
        &self,
        args: &serde_json::Value,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        match self.resolved_tool() {
            Some(tool) => tool.prepare_authorization(args).await,
            None => Ok(None),
        }
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.resolved_tool() else {
            tracing::warn!(tool = %self.name, "shared tool not found");
            return Ok(ToolResult::error(format!("unknown tool '{}'", self.name)));
        };
        // A callable tool's operational failure is input to the agent loop, not
        // a failure of the harness itself.  Preserve it as an error result so
        // the model can recover (or explain the failure) on its next round.
        let execution: ToolExecution<'_> =
            Box::pin(tool.execute_with_context(args, options, context));
        let execution = match &self.exec_scope {
            Some(scope) => scope.scope(context, execution),
            None => execution,
        };
        let result = match execution.await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(tool = %self.name, %error, "shared tool execution failed");
                ToolResult::error(format!("{} failed: {error}", self.name))
            }
        };
        if !result.is_error
            && let Some(hook) = &self.early_exit
        {
            hook.trigger(&self.name, result.output_for_llm(true));
        }
        Ok(result)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
