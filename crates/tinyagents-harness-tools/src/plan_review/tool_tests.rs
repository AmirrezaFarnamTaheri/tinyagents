use super::*;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use std::sync::Mutex;
use tinyagents_harness::middleware::library::RunMode;
use tinytools::{Tool, ToolRunContext};

struct Port {
    resolution: PlanReviewResolution,
    request: Mutex<Option<PlanReviewRequest>>,
}
struct FailingPort;
struct NonInteractive;
struct BuildMode;
#[async_trait]
impl PlanReviewHost for NonInteractive {
    fn interactive_surface(
        &self,
        _: Option<&dyn ToolRunContext>,
    ) -> Option<InteractiveReviewSurface> {
        None
    }
    async fn request_review(
        &self,
        _: PlanReviewRequest,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String> {
        panic!("non-interactive requests are auto-approved before parking")
    }
}
#[async_trait]
impl PlanReviewHost for BuildMode {
    fn run_mode(&self, _: Option<&dyn ToolRunContext>, _: Option<&str>) -> RunMode {
        RunMode::Build
    }
    async fn request_review(
        &self,
        _: PlanReviewRequest,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String> {
        panic!("non-plan requests are not parked")
    }
}
#[async_trait]
impl PlanReviewHost for FailingPort {
    async fn request_review(
        &self,
        _: PlanReviewRequest,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String> {
        Err("review surface unavailable".into())
    }
}
#[async_trait]
impl PlanReviewHost for Port {
    fn tool_call_id(&self, _: Option<&dyn ToolRunContext>) -> Option<String> {
        Some("call-1".into())
    }
    async fn request_review(
        &self,
        request: PlanReviewRequest,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<PlanReviewResolution, String> {
        *self.request.lock().unwrap() = Some(request);
        Ok(self.resolution.clone())
    }
}

#[tokio::test]
async fn missing_port_fails_closed() {
    let result = RequestPlanReviewTool::new(None)
        .execute(json!({"summary":"plan"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.text().contains("no review port"));
}

#[tokio::test]
async fn missing_interactive_surface_auto_approves() {
    let result = RequestPlanReviewTool::new(Some(Arc::new(NonInteractive)))
        .execute(json!({"summary":"plan"}))
        .await
        .unwrap();
    assert_eq!(
        result.text(),
        "approved: non-interactive turn (no review surface) — proceed with the plan."
    );
}

#[tokio::test]
async fn build_mode_returns_not_applicable_without_parking() {
    let result = RequestPlanReviewTool::new(Some(Arc::new(BuildMode)))
        .execute(json!({"summary":"plan"}))
        .await
        .unwrap();
    assert!(result.text().starts_with("not applicable:"));
}

#[tokio::test]
async fn failing_port_returns_error_without_approval() {
    let result = RequestPlanReviewTool::new(Some(Arc::new(FailingPort)))
        .execute(json!({"summary":"plan"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(
        result.text(),
        "Plan review failed: review surface unavailable"
    );
}

#[tokio::test]
async fn request_is_trimmed_and_resolution_text_is_preserved() {
    let port = Arc::new(Port {
        resolution: PlanReviewResolution::Revise {
            feedback: "add tests".into(),
        },
        request: Mutex::new(None),
    });
    let tool = RequestPlanReviewTool::new(Some(port.clone()));
    let result = tool
        .execute(json!({"summary":" plan ","steps":[" first "," ",2,"second"]}))
        .await
        .unwrap();
    assert_eq!(
        result.text(),
        "revise: the user requested changes before executing. Their feedback:\nadd tests\nRevise the plan accordingly, then call `request_plan_review` again before executing."
    );
    assert_eq!(
        *port.request.lock().unwrap(),
        Some(PlanReviewRequest {
            summary: "plan".into(),
            steps: vec!["first".into(), "second".into()],
            tool_call_id: Some("call-1".into()),
        })
    );
}

#[tokio::test]
async fn each_resolution_has_its_distinct_result() {
    let cases = [
        (
            PlanReviewResolution::Approve,
            "approved: the user approved the plan — proceed and execute it now.",
        ),
        (
            PlanReviewResolution::NonInteractiveApprove,
            "approved: non-interactive turn (no review surface) — proceed with the plan.",
        ),
        (
            PlanReviewResolution::NotApplicable,
            "not applicable: this thread is not in plan mode, so there is no plan to review. Do not call `request_plan_review` again this turn.",
        ),
        (
            PlanReviewResolution::Reject,
            "rejected: the user rejected the plan — do NOT execute it. Briefly ask what they would like to do instead.",
        ),
    ];
    for (resolution, expected) in cases {
        let tool = RequestPlanReviewTool::new(Some(Arc::new(Port {
            resolution,
            request: Mutex::new(None),
        })));
        assert_eq!(
            tool.execute(json!({"summary":"x"})).await.unwrap().text(),
            expected
        );
    }
}
