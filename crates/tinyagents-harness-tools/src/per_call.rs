//! Rebuild a tool around the run context for each invocation.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinytools::{
    PermissionLevel, Tool, ToolCallOptions, ToolCategory, ToolExposure, ToolInjectedArgument,
    ToolPolicy, ToolResult, ToolRunContext, ToolScope, ToolTimeout,
};

type ContextFactory<C> = dyn for<'a> Fn(Option<&'a dyn ToolRunContext>) -> C + Send + Sync;
type Build<T, C> = dyn Fn(C) -> T + Send + Sync;

/// A tool whose executing instance is rebuilt from each invocation's context.
pub struct PerCallTool<T: Tool, C: 'static> {
    prototype: T,
    context: Arc<ContextFactory<C>>,
    build: Arc<Build<T, C>>,
}

impl<T: Tool, C: 'static> PerCallTool<T, C> {
    /// `context` extracts a host-defined value from the harness run context;
    /// `build` makes the tool that will execute using that value.
    pub fn new(
        context: impl for<'a> Fn(Option<&'a dyn ToolRunContext>) -> C + Send + Sync + 'static,
        build: impl Fn(C) -> T + Send + Sync + 'static,
    ) -> Self {
        let context: Arc<ContextFactory<C>> = Arc::new(context);
        let build: Arc<Build<T, C>> = Arc::new(build);
        let prototype = build(context(None));
        Self {
            prototype,
            context,
            build,
        }
    }

    fn for_call(&self, context: Option<&dyn ToolRunContext>) -> T {
        (self.build)((self.context)(context))
    }
}

impl<T: Tool, C: 'static> std::fmt::Debug for PerCallTool<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PerCallTool")
            .field(&self.prototype.name())
            .finish()
    }
}

#[async_trait]
impl<T: Tool, C: 'static> Tool for PerCallTool<T, C> {
    fn name(&self) -> &str {
        self.prototype.name()
    }
    fn description(&self) -> &str {
        self.prototype.description()
    }
    fn parameters_schema(&self) -> Value {
        self.prototype.parameters_schema()
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.for_call(None).execute(args).await
    }

    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        self.prototype.prepare_authorization(args).await
    }

    async fn execute_with_options(
        &self,
        args: Value,
        options: ToolCallOptions,
    ) -> anyhow::Result<ToolResult> {
        self.for_call(None)
            .execute_with_options(args, options)
            .await
    }

    async fn execute_with_context(
        &self,
        args: Value,
        options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        self.for_call(context)
            .execute_with_context(args, options, context)
            .await
    }

    fn policy(&self) -> ToolPolicy {
        self.prototype.policy()
    }
    fn injected_arguments(&self) -> Vec<ToolInjectedArgument> {
        self.prototype.injected_arguments()
    }
    fn supports_markdown(&self) -> bool {
        self.prototype.supports_markdown()
    }
    fn permission_level(&self) -> PermissionLevel {
        self.prototype.permission_level()
    }
    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        self.prototype.permission_level_with_args(args)
    }
    fn scope(&self) -> ToolScope {
        self.prototype.scope()
    }
    fn category(&self) -> ToolCategory {
        self.prototype.category()
    }
    fn exposure(&self) -> ToolExposure {
        self.prototype.exposure()
    }
    fn family(&self) -> Option<&str> {
        self.prototype.family()
    }
    fn tags(&self) -> Vec<String> {
        self.prototype.tags()
    }
    fn indirect_target(&self, args: &Value) -> Option<tinytools::rules::IndirectCall> {
        self.prototype.indirect_target(args)
    }
    async fn indirect_target_async(
        &self,
        args: &Value,
    ) -> anyhow::Result<Option<tinytools::rules::IndirectCall>> {
        self.prototype.indirect_target_async(args).await
    }
    fn is_concurrency_safe(&self, args: &Value) -> bool {
        self.prototype.is_concurrency_safe(args)
    }
    fn external_effect(&self) -> bool {
        self.prototype.external_effect()
    }
    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.prototype.external_effect_with_args(args)
    }
    fn max_result_size_chars(&self) -> Option<usize> {
        self.prototype.max_result_size_chars()
    }
    fn timeout_policy(&self, args: &Value) -> ToolTimeout {
        self.prototype.timeout_policy(args)
    }
    fn host_extension(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.prototype.host_extension()
    }
}

#[cfg(test)]
#[path = "per_call_tests.rs"]
mod tests;
