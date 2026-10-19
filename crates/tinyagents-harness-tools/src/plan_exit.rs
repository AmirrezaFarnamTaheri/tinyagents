//! Host-configured plan-to-build handoff tool.

use std::{hash::Hash, sync::Arc};

use async_trait::async_trait;
use serde_json::json;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext};
use crate::{plan_review::PlanReviewStore, run_mode::RunModeStore};
use tinyagents_harness::middleware::library::RunMode;


/// Stable marker emitted for a plan-to-build handoff.
pub const PLAN_EXIT_MARKER: &str = "[plan_exit]";

/// Host-owned state operations required for a generic plan exit.
pub trait PlanExitHost: Send + Sync {
    type Scope: Clone + Eq + Hash + Send + Sync + 'static;
    fn scope_and_thread(&self, context: &dyn ToolRunContext) -> (Self::Scope, String);
}

/// Tool that returns a plan and switches its host-selected run mode to Build.
pub struct PlanExitTool<S> {
    host: Arc<dyn PlanExitHost<Scope = S>>,
    run_modes: Arc<dyn RunModeStore<S>>,
    reviews: Option<Arc<PlanReviewStore<S>>>,
}

impl<S: Clone + Eq + Hash + Send + Sync + 'static> PlanExitTool<S> {
    /// Stable tool name advertised to the model and host registries.
    pub const NAME: &'static str = "plan_exit";

    /// Stable tool description advertised to the model and host registries.
    pub const DESCRIPTION: &'static str = "Exit plan mode and hand off the plan to execution. Call this once the plan is complete — the wrapped harness will switch to build mode (when wired). The `plan` argument is the user-facing plan text that downstream agents will execute against.";

    /// Creates a tool whose parked-review check and mode transition are
    /// delegated to the host's generic stores.
    pub fn new(host: Arc<dyn PlanExitHost<Scope = S>>, run_modes: Arc<dyn RunModeStore<S>>, reviews: Option<Arc<PlanReviewStore<S>>>) -> Self {
        Self { host, run_modes, reviews }
    }
}

#[async_trait]
impl<S: Clone + Eq + Hash + Send + Sync + 'static> Tool for PlanExitTool<S> {
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
            let (scope, thread_id) = self.host.scope_and_thread(context);
            if self.reviews.as_ref().is_some_and(|reviews| reviews.has_parked_review(&scope, &thread_id)) {
                return Ok(ToolResult::error("A plan review is still pending for this thread"));
            }
            self.run_modes.set_mode(&scope, &thread_id, RunMode::Build);
        }

        Ok(ToolResult::success(format!("{PLAN_EXIT_MARKER}\n{plan}")))
    }
}

#[cfg(test)]
#[path = "plan_exit_tests.rs"]
mod tests;
