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
        let event = ToolHookContext {
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

#[cfg(test)]
#[path = "tool_tests.rs"]
mod tests;

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
        let identity = crate::command::context::TurnIdentity {
                conversation_id: host.session_id(),
                session_id: host.session_id(),
                agent_id: host.agent_id(),
                cwd: host.cwd().as_ref().map(|path| path.display().to_string()),
                ..Default::default()
            };
        let output = self.dispatch_tool_hooks(context, identity, true).await;
        output
    }

    async fn after_tool_context(&self, host: &Ctx, context: &ToolHookContext) -> Option<String> {
        let identity = crate::command::context::TurnIdentity {
            conversation_id: host.session_id(), session_id: host.session_id(),
            agent_id: host.agent_id(), cwd: host.cwd().as_ref().map(|path| path.display().to_string()),
            ..Default::default()
        };
        self.dispatch_tool_hooks(context, identity, false).await.additional_context
    }
}

impl crate::command::HookEngine {
    async fn dispatch_tool_hooks(&self, context: &ToolHookContext, identity: crate::command::context::TurnIdentity, pre: bool) -> ToolHookDecision {
        use crate::command::{HookEvent, HookPayload, ToolPayload};
        let generic = if pre { HookEvent::PreToolUse } else if context.success == Some(false) { HookEvent::PostToolUseFailure } else { HookEvent::PostToolUse };
        let mut output = ToolHookDecision::default();
        if self.has_hooks(generic).await {
            let input = crate::command::context::build_input(generic, identity.clone(), HookPayload::Tool(ToolPayload {
                tool_name: context.tool_name.clone(),
                tool_input: context.arguments.clone(),
                tool_use_id: context.call_id.clone(),
                tool_output: context.output.clone(), duration_ms: context.duration_ms,
                error_message: context.error.clone(),
                ..Default::default()
            }));
            output.merge(self.dispatch(generic, input).await.output);
        }
        if output.is_deny() { return output; }
        let name = context.tool_name.to_ascii_lowercase();
        let derived = if ["shell", "bash", "run_command", "command"].contains(&name.as_str()) {
            Some(if pre { HookEvent::BeforeShellExecution } else { HookEvent::AfterShellExecution })
        } else if ["file_read", "read_file", "read", "readfile"].contains(&name.as_str()) && pre {
            Some(HookEvent::BeforeReadFile)
        } else if ["file_write", "write_file", "edit_file", "write", "edit"].contains(&name.as_str()) && !pre {
            Some(HookEvent::AfterFileEdit)
        } else if name.starts_with("mcp_") || name.starts_with("mcp:") || name.starts_with("mcp__") {
            Some(if pre { HookEvent::BeforeMcpExecution } else { HookEvent::AfterMcpExecution })
        } else { None };
        if let Some(event) = derived && self.has_hooks(event).await {
            let payload = match event {
                HookEvent::BeforeShellExecution | HookEvent::AfterShellExecution => HookPayload::Shell(crate::command::types::ShellPayload {
                    command: context.arguments.get("command").and_then(serde_json::Value::as_str).unwrap_or_default().to_owned(),
                    sandbox: context.arguments.get("sandbox").and_then(serde_json::Value::as_bool).unwrap_or(false),
                    output: context.output.clone(), duration_ms: context.duration_ms,
                }),
                HookEvent::BeforeReadFile | HookEvent::AfterFileEdit => HookPayload::File(crate::command::types::FilePayload {
                    file_path: ["path", "file_path", "filename", "file"].iter().find_map(|k| context.arguments.get(*k).and_then(serde_json::Value::as_str)).unwrap_or_default().to_owned(),
                    edits: Vec::new(),
                }),
                _ => HookPayload::Tool(ToolPayload { tool_name: context.tool_name.clone(), tool_input: context.arguments.clone(), tool_use_id: context.call_id.clone(), tool_output: context.output.clone(), duration_ms: context.duration_ms, error_message: context.error.clone(), ..Default::default() }),
            };
            let result = self.dispatch(event, crate::command::context::build_input(event, identity, payload)).await;
            output.merge(result.output);
        }
        output
    }
}
