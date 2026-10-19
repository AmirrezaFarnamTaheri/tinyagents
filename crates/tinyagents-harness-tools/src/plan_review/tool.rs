use super::{PlanReviewHost, PlanReviewRequest, PlanReviewResolution};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use tinyagents_harness::middleware::library::RunMode;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext, ToolTimeout};

pub struct RequestPlanReviewTool {
    port: Option<Arc<dyn PlanReviewHost>>,
}
impl RequestPlanReviewTool {
    #[must_use]
    pub fn new(port: Option<Arc<dyn PlanReviewHost>>) -> Self {
        Self { port }
    }
}

#[async_trait]
impl Tool for RequestPlanReviewTool {
    fn name(&self) -> &str {
        "request_plan_review"
    }
    fn description(&self) -> &str {
        "Pause the turn so the user can approve a thread-scoped plan before you execute it. Blocks until they decide, then returns approved, rejected, or revise with their feedback. Non-interactive policy is supplied by the host."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"summary":{"type":"string","description":"One-line description of the plan being reviewed."},"steps":{"type":"array","description":"Ordered plan steps shown to the user for review.","items":{"type":"string"}}},"required":["summary"]})
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::None
    }
    fn external_effect(&self) -> bool {
        false
    }
    fn timeout_policy(&self, _: &Value) -> ToolTimeout {
        ToolTimeout::Unbounded
    }
    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(args, ToolCallOptions::default(), None)
            .await
    }
    async fn execute_with_context(
        &self,
        args: Value,
        _: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let Some(port) = &self.port else {
            return Ok(ToolResult::error(
                "Plan review is unavailable: no review port is installed.",
            ));
        };
        let summary = args
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        let steps = args
            .get("steps")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let Some(()) = port.interactive_surface(context) else {
            return Ok(ToolResult::success(
                "approved: non-interactive turn (no review surface) — proceed with the plan.",
            ));
        };
        let thread_id = port.thread_id(context);
        if port.run_mode(context, thread_id.as_deref()) != RunMode::Plan {
            return Ok(ToolResult::success(
                "not applicable: this thread is not in plan mode, so there is no plan to review. Do not call `request_plan_review` again this turn.",
            ));
        }
        let tool_call_id = port.tool_call_id(context);
        let resolution = match port
            .request_review(
                PlanReviewRequest {
                    summary,
                    steps,
                    tool_call_id,
                },
                context,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => return Ok(ToolResult::error(format!("Plan review failed: {error}"))),
        };
        let result = match resolution {
            PlanReviewResolution::Approve => "approved: the user approved the plan — proceed and execute it now.".to_owned(),
            PlanReviewResolution::NonInteractiveApprove => "approved: non-interactive turn (no review surface) — proceed with the plan.".to_owned(),
            PlanReviewResolution::NotApplicable => "not applicable: this thread is not in plan mode, so there is no plan to review. Do not call `request_plan_review` again this turn.".to_owned(),
            PlanReviewResolution::Reject => "rejected: the user rejected the plan — do NOT execute it. Briefly ask what they would like to do instead.".to_owned(),
            PlanReviewResolution::Revise { feedback } => format!("revise: the user requested changes before executing. Their feedback:\n{feedback}\nRevise the plan accordingly, then call `request_plan_review` again before executing."),
        };
        Ok(ToolResult::success(result))
    }
}
