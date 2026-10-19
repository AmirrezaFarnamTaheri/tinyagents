use super::*;
use std::sync::{Arc, Mutex};
use tinyagents_harness::{context::{RunConfig, RunContext}, ids::CallId, middleware::library::{RunMode, RunModeHandle}, tool::ToolExecutionContext};

struct Host;
impl PlanExitHost for Host { type Scope = u8; fn scope_and_thread(&self, _: &dyn tinytools::ToolRunContext) -> (u8, String) { (1, "thread".into()) } }
#[derive(Default)] struct Changes(Mutex<Vec<RunMode>>);
impl crate::run_mode::ModeChangeNotifier<u8> for Changes { fn changed(&self, _: &u8, _: &str, mode: RunMode) { self.0.lock().unwrap().push(mode); } }
fn tool(mode: RunMode, reviews: Option<Arc<PlanReviewStore<u8>>>, changes: Arc<Changes>) -> (PlanExitTool<u8>, RunModeHandle) {
    let modes = Arc::new(crate::run_mode::InMemoryRunModeStore::new(changes));
    modes.set_mode(&1, "thread", mode);
    let handle = modes.handle_for_thread(&1, "thread");
    (PlanExitTool::new(Arc::new(Host), modes, reviews), handle)
}
struct NoopReviews;
impl crate::plan_review::PlanReviewNotifier<u8> for NoopReviews {
    fn requested(&self, _: &crate::plan_review::ParkedReview<u8>) {}
    fn decided(&self, _: &crate::plan_review::ParkedReview<u8>, _: &crate::plan_review::PlanReviewResolution, _: crate::plan_review::PlanReviewReason) {}
}

#[tokio::test]
async fn plan_exit_switches_mode_through_notifying_store_and_keeps_marker() {
    let changes = Arc::new(Changes::default());
    let (tool, mode) = tool(RunMode::Plan, None, changes.clone());
    let parent = RunContext::new(RunConfig::new("plan-run"), ());
    let context = ToolExecutionContext::from_run_context(&parent, CallId::new("call-1"));
    let result = tool.execute_with_context(json!({"plan":" step one "}), ToolCallOptions::default(), Some(&context)).await.unwrap();
    assert_eq!(mode.get(), RunMode::Build);
    assert_eq!(*changes.0.lock().unwrap(), vec![RunMode::Plan, RunMode::Build]);
    assert_eq!(result.text(), format!("{PLAN_EXIT_MARKER}\nstep one"));
}

#[tokio::test]
async fn plan_exit_refuses_while_review_is_parked_and_leaves_mode_unchanged() {
    let reviews = Arc::new(PlanReviewStore::new(Arc::new(NoopReviews)));
    let waiter_store = reviews.clone();
    let parked = tokio::spawn(async move { waiter_store.request_review(1, "thread".into(), None, crate::plan_review::PlanReviewRequest { summary: "plan".into(), steps: vec![], tool_call_id: None }).await });
    tokio::task::yield_now().await;
    let changes = Arc::new(Changes::default());
    let (tool, mode) = tool(RunMode::Plan, Some(reviews), changes.clone());
    let parent = RunContext::new(RunConfig::new("plan-run"), ());
    let context = ToolExecutionContext::from_run_context(&parent, CallId::new("call-2"));
    let result = tool.execute_with_context(json!({"plan":"step"}), ToolCallOptions::default(), Some(&context)).await.unwrap();
    assert!(result.is_error);
    assert_eq!(mode.get(), RunMode::Plan);
    assert_eq!(changes.0.lock().unwrap().last(), Some(&RunMode::Plan));
    let review = reviews.parked_review_for_thread(&1, "thread").unwrap();
    reviews.decide(&1, &review.request_id, crate::plan_review::PlanReviewResolution::Approve);
    let _ = parked.await;
}
