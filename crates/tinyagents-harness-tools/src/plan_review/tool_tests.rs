use super::*;
use std::sync::Mutex;

struct Port {
    resolution: PlanReviewResolution,
    request: Mutex<Option<PlanReviewRequest>>,
}
#[async_trait]
impl PlanReviewPort for Port {
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
            steps: vec!["first".into(), "second".into()]
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
