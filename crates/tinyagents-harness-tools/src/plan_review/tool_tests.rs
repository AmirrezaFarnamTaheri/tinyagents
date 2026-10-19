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

#[tokio::test]
async fn interactive_tool_parks_in_the_scoped_store_until_decided() {
    use crate::plan_review::{ParkedReview, PlanReviewNotifier, PlanReviewReason, PlanReviewStore};
    use std::time::Duration;

    #[derive(Default)]
    struct StoreEvents {
        requested: tokio::sync::Notify,
        decided: Mutex<
            Vec<(
                String,
                PlanReviewResolution,
                Option<String>,
                PlanReviewReason,
            )>,
        >,
    }
    impl PlanReviewNotifier<u8> for StoreEvents {
        fn requested(&self, _: &ParkedReview<u8>) {
            self.requested.notify_one();
        }
        fn decided(
            &self,
            review: &ParkedReview<u8>,
            resolution: &PlanReviewResolution,
            reason: PlanReviewReason,
        ) {
            self.decided.lock().unwrap().push((
                review.thread_id.clone(),
                resolution.clone(),
                review.tool_call_id.clone(),
                reason,
            ));
        }
    }
    struct StorePort(Arc<PlanReviewStore<u8>>);
    #[async_trait]
    impl PlanReviewHost for StorePort {
        fn thread_id(&self, _: Option<&dyn ToolRunContext>) -> Option<String> {
            Some("thread-7".into())
        }
        fn tool_call_id(&self, _: Option<&dyn ToolRunContext>) -> Option<String> {
            Some("call-9".into())
        }
        async fn request_review(
            &self,
            request: PlanReviewRequest,
            _: Option<&dyn ToolRunContext>,
        ) -> Result<PlanReviewResolution, String> {
            Ok(self
                .0
                .request_review(42, "thread-7".into(), Some("client-3".into()), request)
                .await)
        }
    }

    let events = Arc::new(StoreEvents::default());
    let store = Arc::new(PlanReviewStore::with_ttl(
        Duration::from_secs(60),
        events.clone(),
    ));
    let tool = RequestPlanReviewTool::new(Some(Arc::new(StorePort(store.clone()))));
    let pending = tool.execute(json!({"summary":"ship it","steps":["build"]}));
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::ZERO, &mut pending)
            .await
            .is_err()
    );
    events.requested.notified().await;
    let parked = store.parked_review_for_thread(&42, "thread-7").unwrap();
    assert_eq!(parked.client_id.as_deref(), Some("client-3"));
    assert_eq!(parked.tool_call_id.as_deref(), Some("call-9"));
    assert_eq!(parked.summary, "ship it");
    assert_eq!(parked.steps, vec!["build"]);
    assert!(store.decide(&42, &parked.request_id, PlanReviewResolution::Approve));
    let result = pending.await.unwrap();
    assert_eq!(
        result.text(),
        "approved: the user approved the plan — proceed and execute it now."
    );
    assert!(!store.has_parked_review(&42, "thread-7"));
    assert_eq!(
        *events.decided.lock().unwrap(),
        vec![(
            "thread-7".into(),
            PlanReviewResolution::Approve,
            Some("call-9".into()),
            PlanReviewReason::Approved,
        )]
    );
}
