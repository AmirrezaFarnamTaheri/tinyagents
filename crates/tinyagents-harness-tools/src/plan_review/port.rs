use async_trait::async_trait;
use tinyagents_harness::middleware::library::RunMode;
use tinytools::ToolRunContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanReviewRequest {
    pub summary: String,
    pub steps: Vec<String>,
    pub tool_call_id: Option<String>,
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
pub trait PlanReviewHost: Send + Sync {
    /// `None` selects the established non-interactive auto-approval path.
    fn interactive_surface(&self, _context: Option<&dyn ToolRunContext>) -> Option<()> {
        Some(())
    }
    fn thread_id(&self, _context: Option<&dyn ToolRunContext>) -> Option<String> {
        None
    }
    fn run_mode(&self, _context: Option<&dyn ToolRunContext>, _thread_id: Option<&str>) -> RunMode {
        RunMode::Plan
    }
    fn tool_call_id(&self, _context: Option<&dyn ToolRunContext>) -> Option<String> {
        None
    }
    async fn request_review(
        &self,
        request: PlanReviewRequest,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String>;
}

/// Compatibility name retained from the initial generic port introduced in
/// the harness-tools crate.
pub use PlanReviewHost as PlanReviewPort;
