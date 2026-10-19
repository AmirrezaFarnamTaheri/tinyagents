//! Host-configured plan-to-build handoff tool.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext};


/// Stable marker emitted for a plan-to-build handoff.
pub const PLAN_EXIT_MARKER: &str = "[plan_exit]";

/// Host-owned state operations required for a generic plan exit.
pub trait PlanExitHost: Send + Sync {
    fn review_is_parked(&self, context: &dyn ToolRunContext) -> bool;
    fn switch_to_build(&self, context: &dyn ToolRunContext);
}

/// Tool that returns a plan and switches its host-selected run mode to Build.
pub struct PlanExitTool {
    host: Arc<dyn PlanExitHost>,
}

impl PlanExitTool {
    /// Stable tool name advertised to the model and host registries.
    pub const NAME: &'static str = "plan_exit";

    /// Stable tool description advertised to the model and host registries.
    pub const DESCRIPTION: &'static str = "Exit plan mode and hand off the plan to execution. Call this once the plan is complete — the wrapped harness will switch to build mode (when wired). The `plan` argument is the user-facing plan text that downstream agents will execute against.";

    /// Creates a tool whose parked-review check and mode transition are
    /// delegated to the host's generic stores.
    pub fn new(host: Arc<dyn PlanExitHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl Tool for PlanExitTool {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn description(&self) -> &str {
        Self::DESCRIPTION
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

        if let Some(context) = context {
            if self.host.review_is_parked(context) {
                return Ok(ToolResult::error("A plan review is still pending for this thread"));
            }
            self.host.switch_to_build(context);
        }

        Ok(ToolResult::success(format!("{PLAN_EXIT_MARKER}\n{plan}")))
    }
}

#[cfg(test)]
#[path = "plan_exit_tests.rs"]
mod tests;
