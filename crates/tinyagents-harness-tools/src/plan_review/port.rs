use async_trait::async_trait;
use tinytools::ToolRunContext;

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
