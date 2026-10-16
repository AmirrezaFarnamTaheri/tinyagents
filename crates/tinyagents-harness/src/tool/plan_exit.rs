//! Host-configured plan-to-build handoff tool.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext};

use crate::middleware::library::{RunMode, RunModeHandle};

/// Stable marker emitted for a plan-to-build handoff.
pub const PLAN_EXIT_MARKER: &str = "[plan_exit]";

/// Host callback that resolves the live mode handle only when this call may
/// leave plan mode. Hosts can keep tenant lookup and approval checks here.
pub type PlanExitModeLookup = dyn Fn(&dyn ToolRunContext) -> Option<RunModeHandle> + Send + Sync;

/// Tool that returns a plan and switches its host-selected run mode to Build.
pub struct PlanExitTool {
    lookup: Arc<PlanExitModeLookup>,
}

impl PlanExitTool {
    /// Creates a tool using the host's run-mode lookup and policy checks.
    pub fn new(lookup: Arc<PlanExitModeLookup>) -> Self {
        Self { lookup }
    }
}

#[async_trait]
impl Tool for PlanExitTool {
    fn name(&self) -> &str {
        "plan_exit"
    }

    fn description(&self) -> &str {
        "Exit plan mode and hand off the plan to execution. Call this once the plan is complete."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "plan": { "type": "string", "description": "Markdown-formatted plan text to hand off." }
            },
            "required": ["plan"]
        })
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::None
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(args, ToolCallOptions::default(), None)
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        _options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let plan = args
            .get("plan")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Missing 'plan' parameter"))?;
        let plan = plan.trim();
        if plan.is_empty() {
            return Ok(ToolResult::error("`plan` must not be empty"));
        }

        if let Some(context) = context
            && let Some(mode) = (self.lookup)(context)
        {
            mode.set(RunMode::Build);
        }

        Ok(ToolResult::success(format!("{PLAN_EXIT_MARKER}\n{plan}")))
    }
}

#[cfg(test)]
#[path = "plan_exit_tests.rs"]
mod tests;
