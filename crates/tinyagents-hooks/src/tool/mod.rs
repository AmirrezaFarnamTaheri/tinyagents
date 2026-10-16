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
pub enum ToolHookEvent {
    /// Before the tool executes.
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

/// Unified decision type for typed and command hooks.
pub type ToolHookDecision = super::command::HookOutput;

/// Typed callback around tool execution.
#[async_trait]
pub trait ToolHook<Ctx: Send + Sync = ()>: Send + Sync {
    /// Stable diagnostic name.
    fn name(&self) -> &str;
    /// Observe before execution. Errors are treated as a denial.
    async fn before_tool(
        &self,
        _host_context: &Ctx,
        _context: &ToolHookContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    /// Decide before execution. Override to rewrite or deny the call.
    async fn before_tool_decision(
        &self,
        host_context: &Ctx,
        context: &ToolHookContext,
    ) -> ToolHookDecision {
        match self.before_tool(host_context, context).await {
            Ok(()) => ToolHookDecision::default(),
            Err(error) => ToolHookDecision::deny(format!("{error:#}")),
        }
    }
    /// Observe a completed invocation. Errors are advisory.
    async fn after_tool(
        &self,
        _host_context: &Ctx,
        _context: &ToolHookContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    /// Observe a completed invocation and optionally return model context.
    async fn after_tool_context(
        &self,
        host_context: &Ctx,
        context: &ToolHookContext,
    ) -> Option<String> {
        if let Err(error) = self.after_tool(host_context, context).await {
            tracing::warn!(hook = self.name(), %error, "tool hook failed after execution");
        }
        None
    }
}

/// Summarize tool output without retaining its contents in learning records.
pub fn sanitize_tool_output(output: &str, tool_name: &str, success: bool) -> String {
    if success {
        return format!("{tool_name}: ok ({} chars)", output.chars().count());
    }
    let lower = output.to_lowercase();
    let class = if lower.contains("timeout") {
        "timeout"
    } else if lower.contains("not found") || lower.contains("no such file") {
        "not_found"
    } else if lower.contains("permission") || lower.contains("denied") {
        "permission_denied"
    } else if lower.contains("connection") || lower.contains("network") {
        "connection_error"
    } else if lower.contains("parse") || lower.contains("invalid") || lower.contains("syntax") {
        "parse_error"
    } else if lower.contains("unknown tool") {
        "unknown_tool"
    } else {
        "error"
    };
    format!("{tool_name}: failed ({class})")
}

/// Deliver typed callbacks through the harness middleware lifecycle.
pub struct ToolHooksMiddleware<Ctx> {
    hooks: Vec<std::sync::Arc<dyn ToolHook<Ctx>>>,
    arguments: std::sync::Mutex<
        std::collections::HashMap<
            (u64, String),
            (serde_json::Value, std::time::Instant, Vec<String>),
        >,
    >,
    marker: std::marker::PhantomData<fn() -> Ctx>,
}

impl<Ctx> ToolHooksMiddleware<Ctx> {
    /// Creates a middleware with callbacks in execution order.
    pub fn new(hooks: Vec<std::sync::Arc<dyn ToolHook<Ctx>>>) -> Self {
        Self {
            hooks,
            arguments: std::sync::Mutex::new(std::collections::HashMap::new()),
            marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<Ctx> tinyagents_harness::middleware::Middleware<(), Ctx> for ToolHooksMiddleware<Ctx>
where
    Ctx: HookIdentity + Send + Sync,
{
    fn name(&self) -> &str {
        "tool_hooks"
    }

    async fn before_tool(
        &self,
        ctx: &mut tinyagents_harness::context::RunContext<Ctx>,
        _state: &(),
        call: &mut tinyinference_llm::tool::ToolCall,
    ) -> tinyagents_harness::error::Result<()> {
        use tinyagents_harness::error::TinyAgentsError;
        let mut event = ToolHookContext {
            event: ToolHookEvent::PreToolUse,
            call_id: call.id.clone(),
            tool_name: call.name.clone(),
            arguments: call.arguments.clone(),
            success: None,
            duration_ms: None,
            output: None,
            error: None,
            session_id: ctx.data.session_id(),
            agent_id: ctx.data.agent_id(),
            cwd: ctx.data.cwd(),
        };
        let mut additional_context = Vec::new();
        for hook in &self.hooks {
            let decision = hook.before_tool_decision(&ctx.data, &event).await;
            if decision.is_deny() || decision.is_ask() {
                let ask = decision.is_ask();
                let reason = decision
                    .agent_message
                    .or(decision.user_message)
                    .unwrap_or_else(|| "hook refused the tool call".into());
                let kind = if ask {
                    "requires approval for"
                } else {
                    "denied"
                };
                return Err(TinyAgentsError::Tool(format!(
                    "tool hook '{}' {kind} {}: {reason}",
                    hook.name(),
                    event.tool_name
                )));
            }
            if let Some(input) = decision.updated_input {
                call.arguments = input.clone();
                event.arguments = input;
            }
            if let Some(context) = decision.additional_context {
                additional_context.push(context);
            }
        }
        self.arguments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                (ctx.instance_id(), call.id.clone()),
                (
                    call.arguments.clone(),
                    std::time::Instant::now(),
                    additional_context,
                ),
            );
        Ok(())
    }

    async fn after_tool(
        &self,
        ctx: &mut tinyagents_harness::context::RunContext<Ctx>,
        _state: &(),
        invocation: &tinyagents_harness::middleware::ToolInvocationIdentity,
        result: &mut tinytools::ToolResult,
    ) -> tinyagents_harness::error::Result<()> {
        let (args, started, mut additional_context) = self
            .arguments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(ctx.instance_id(), invocation.call_id().to_string()))
            .unwrap_or((
                serde_json::Value::Null,
                std::time::Instant::now(),
                Vec::new(),
            ));
        let text = result.text();
        let mut event = ToolHookContext {
            event: ToolHookEvent::PostToolUse,
            call_id: invocation.call_id().to_string(),
            tool_name: invocation.tool_name().to_owned(),
            arguments: args,
            success: Some(!result.is_error),
            duration_ms: Some(started.elapsed().as_millis().min(u64::MAX as u128) as u64),
            output: (!result.is_error).then(|| text.clone()),
            error: result.is_error.then_some(text),
            session_id: ctx.data.session_id(),
            agent_id: ctx.data.agent_id(),
            cwd: ctx.data.cwd(),
        };
        for hook in self.hooks.iter().rev() {
            if let Some(additional) = hook.after_tool_context(&ctx.data, &event).await {
                additional_context.push(additional);
            }
        }
        result.content.extend(
            additional_context
                .into_iter()
                .map(|text| tinytools::ToolContent::Text { text }),
        );
        Ok(())
    }
}

#[async_trait]
impl<Ctx: HookIdentity + Send + Sync> ToolHook<Ctx> for crate::command::HookEngine {
    fn name(&self) -> &str {
        "command_hooks"
    }

    async fn before_tool_decision(
        &self,
        host: &Ctx,
        context: &ToolHookContext,
    ) -> ToolHookDecision {
        let input = crate::command::context::build_input(
            crate::command::HookEvent::PreToolUse,
            crate::command::context::TurnIdentity {
                conversation_id: host.session_id(),
                session_id: host.session_id(),
                agent_id: host.agent_id(),
                cwd: host.cwd().as_ref().map(|path| path.display().to_string()),
                ..Default::default()
            },
            crate::command::HookPayload::Tool(crate::command::ToolPayload {
                tool_name: context.tool_name.clone(),
                tool_input: context.arguments.clone(),
                tool_use_id: context.call_id.clone(),
                ..Default::default()
            }),
        );
        self.dispatch(crate::command::HookEvent::PreToolUse, input)
            .await
            .output
    }

    async fn after_tool_context(&self, host: &Ctx, context: &ToolHookContext) -> Option<String> {
        let event = if context.success == Some(false) {
            crate::command::HookEvent::PostToolUseFailure
        } else {
            crate::command::HookEvent::PostToolUse
        };
        let input = crate::command::context::build_input(
            event,
            crate::command::context::TurnIdentity {
                conversation_id: host.session_id(),
                session_id: host.session_id(),
                agent_id: host.agent_id(),
                cwd: host.cwd().as_ref().map(|path| path.display().to_string()),
                ..Default::default()
            },
            crate::command::HookPayload::Tool(crate::command::ToolPayload {
                tool_name: context.tool_name.clone(),
                tool_input: context.arguments.clone(),
                tool_use_id: context.call_id.clone(),
                tool_output: context.output.clone(),
                duration_ms: context.duration_ms,
                error_message: context.error.clone(),
                ..Default::default()
            }),
        );
        self.dispatch(event, input).await.output.additional_context
    }
}
