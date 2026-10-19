use async_trait::async_trait;
use tinytools::ToolRunContext;
use tinyagents_harness::middleware::library::RunMode;

/// Generic host seam used by the review tool. A missing interactive surface
/// means the caller has an explicit non-interactive auto-approval policy.
pub trait PlanReviewHost: PlanReviewPort {
    fn interactive_surface(&self, context: Option<&dyn ToolRunContext>) -> bool;
    fn run_mode(&self, context: Option<&dyn ToolRunContext>, thread_id: Option<&str>) -> RunMode;
    fn tool_call_id(&self, context: Option<&dyn ToolRunContext>) -> Option<String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanReviewRequest {
    pub summary: String,
    pub steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanReviewResolution {
    Approve,
    NonInteractiveApprove,
    NotApplicable,
    Reject,
    Revise { feedback: String },
}

#[async_trait]
pub trait PlanReviewPort: Send + Sync {
    async fn request_review(
        &self,
        request: PlanReviewRequest,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String>;
}
