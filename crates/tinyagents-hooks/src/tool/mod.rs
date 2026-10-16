//! Typed in-process hooks around tool execution.
//!
//! Hooks run in registration order. A denial or unresolved approval request
//! refuses a call, while argument rewrites flow to subsequent hooks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Identity facts a host can expose to a tool hook.
pub trait HookIdentity {
    /// Session or thread identifier, when available.
    fn session_id(&self) -> Option<String>;
    /// Stable agent identifier, when available.
    fn agent_id(&self) -> Option<String>;
    /// Working directory, when available.
    fn cwd(&self) -> Option<PathBuf>;
}

/// Lifecycle point delivered to a tool hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolHookEvent { /// Before the tool executes.
    PreToolUse,
    /// After the tool completes.
    PostToolUse,
}

/// Tool invocation data passed to hook implementations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolHookContext {
    /// Lifecycle point.
    pub event: ToolHookEvent,
    /// Provider call id.
    pub call_id: String,
    /// Registered tool name.
    pub tool_name: String,
    /// Arguments at this lifecycle point.
    pub arguments: serde_json::Value,
    /// Whether the completed call succeeded.
    pub success: Option<bool>,
    /// Duration in milliseconds, when known.
    pub duration_ms: Option<u64>,
    /// Raw successful result, when supplied by the host.
    pub output: Option<String>,
    /// Failure result, when supplied by the host.
    pub error: Option<String>,
    /// Session identifier, when available.
    pub session_id: Option<String>,
    /// Agent identifier, when available.
    pub agent_id: Option<String>,
    /// Working directory, when available.
    pub cwd: Option<PathBuf>,
}

/// Unified tool decision vocabulary, also used by command hooks.
#[derive(Debug, Clone, Default)]
pub enum ToolHookDecision {
    /// Allow the original input.
    #[default]
    Allow,
    /// Allow after replacing the input.
    UpdatedInput(serde_json::Value),
    /// Refuse the operation.
    Deny(String),
    /// Require an external approval decision.
    Ask(String),
    /// Allow and append context for the next model call.
    AdditionalContext(String),
}

/// Typed callback around tool execution.
#[async_trait]
pub trait ToolHook: Send + Sync {
    /// Stable diagnostic name.
    fn name(&self) -> &str;
    /// Observe before execution. Errors are treated as a denial.
    async fn before_tool(&self, _context: &ToolHookContext) -> anyhow::Result<()> { Ok(()) }
    /// Decide before execution. Override to rewrite or deny the call.
    async fn before_tool_decision(&self, context: &ToolHookContext) -> ToolHookDecision {
        match self.before_tool(context).await {
            Ok(()) => ToolHookDecision::Allow,
            Err(error) => ToolHookDecision::Deny(format!("{error:#}")),
        }
    }
    /// Observe a completed invocation. Errors are advisory.
    async fn after_tool(&self, _context: &ToolHookContext) -> anyhow::Result<()> { Ok(()) }
    /// Observe a completed invocation and optionally return model context.
    async fn after_tool_context(&self, context: &ToolHookContext) -> Option<String> {
        if let Err(error) = self.after_tool(context).await {
            tracing::warn!(hook = self.name(), %error, "tool hook failed after execution");
        }
        None
    }
}

/// Deliver typed callbacks through the harness middleware lifecycle.
pub struct ToolHooksMiddleware<Ctx> {
    hooks: Vec<std::sync::Arc<dyn ToolHook>>,
    arguments: std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>,
    marker: std::marker::PhantomData<fn() -> Ctx>,
}

impl<Ctx> ToolHooksMiddleware<Ctx> {
    /// Creates a middleware with callbacks in execution order.
    pub fn new(hooks: Vec<std::sync::Arc<dyn ToolHook>>) -> Self {
        Self { hooks, arguments: std::sync::Mutex::new(std::collections::HashMap::new()), marker: std::marker::PhantomData }
    }
}

#[async_trait]
impl<Ctx> tinyagents_harness::middleware::Middleware<(), Ctx> for ToolHooksMiddleware<Ctx>
where Ctx: HookIdentity + Send + Sync {
    fn name(&self) -> &str { "tool_hooks" }

    async fn before_tool(&self, ctx: &mut tinyagents_harness::context::RunContext<Ctx>, _state: &(), call: &mut tinyinference_llm::tool::ToolCall) -> tinyagents_harness::error::Result<()> {
        use tinyagents_harness::error::TinyAgentsError;
        let mut event = ToolHookContext {
            event: ToolHookEvent::PreToolUse,
            call_id: call.id.clone(), tool_name: call.name.clone(), arguments: call.arguments.clone(),
            success: None, duration_ms: None, output: None, error: None,
            session_id: ctx.data.session_id(), agent_id: ctx.data.agent_id(), cwd: ctx.data.cwd(),
        };
        for hook in &self.hooks {
            match hook.before_tool_decision(&event).await {
                ToolHookDecision::Allow | ToolHookDecision::AdditionalContext(_) => {}
                ToolHookDecision::UpdatedInput(input) => { call.arguments = input.clone(); event.arguments = input; }
                ToolHookDecision::Deny(reason) => return Err(TinyAgentsError::Tool(format!("tool hook '{}' denied {}: {reason}", hook.name(), event.tool_name))),
                ToolHookDecision::Ask(reason) => return Err(TinyAgentsError::Tool(format!("tool hook '{}' requires approval for {}: {reason}", hook.name(), event.tool_name))),
            }
        }
        self.arguments.lock().unwrap_or_else(|e| e.into_inner()).insert(call.id.clone(), call.arguments.clone());
        Ok(())
    }

    async fn after_tool(&self, ctx: &mut tinyagents_harness::context::RunContext<Ctx>, _state: &(), invocation: &tinyagents_harness::middleware::ToolInvocationIdentity, result: &mut tinytools::ToolResult) -> tinyagents_harness::error::Result<()> {
        let args = self.arguments.lock().unwrap_or_else(|e| e.into_inner()).remove(&invocation.call_id().to_string()).unwrap_or(serde_json::Value::Null);
        let text = result.text();
        let mut event = ToolHookContext {
            event: ToolHookEvent::PostToolUse, call_id: invocation.call_id().to_string(), tool_name: invocation.tool_name().to_owned(), arguments: args,
            success: Some(!result.is_error), duration_ms: None,
            output: (!result.is_error).then(|| text.clone()), error: result.is_error.then_some(text),
            session_id: ctx.data.session_id(), agent_id: ctx.data.agent_id(), cwd: ctx.data.cwd(),
        };
        for hook in self.hooks.iter().rev() {
            if let Some(additional) = hook.after_tool_context(&event).await {
                result.content.push(tinytools::ToolContent::Text { text: additional });
            }
            event.arguments = event.arguments.clone();
        }
        Ok(())
    }
}
