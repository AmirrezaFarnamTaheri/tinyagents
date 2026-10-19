use super::*;
use tinyagents_harness::middleware::library::{RunMode, RunModeHandle};
use std::sync::atomic::{AtomicBool, Ordering};

struct Host { parked: bool, changed: AtomicBool, mode: RunModeHandle }
impl PlanExitHost for Host {
    fn review_is_parked(&self, _: &dyn tinytools::ToolRunContext) -> bool { self.parked }
    fn switch_to_build(&self, _: &dyn tinytools::ToolRunContext) { self.mode.set(RunMode::Build); self.changed.store(true, Ordering::SeqCst); }
}

use tinyagents_harness::{
    context::{RunConfig, RunContext},
    ids::CallId,
    tool::ToolExecutionContext,
};

#[tokio::test]
async fn plan_exit_switches_the_host_resolved_mode_and_keeps_the_marker() {
    let mode = RunModeHandle::new(RunMode::Plan);
    let tool = PlanExitTool::new(Arc::new(Host { parked: false, changed: AtomicBool::new(false), mode: mode.clone() }));
    let parent = RunContext::new(RunConfig::new("plan-run"), ());
    let context = ToolExecutionContext::from_run_context(&parent, CallId::new("call-1"));

    let result = tool
        .execute_with_context(
            json!({"plan": "  step one  "}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .expect("execute");

    assert_eq!(mode.get(), RunMode::Build);
    assert_eq!(result.text(), format!("{PLAN_EXIT_MARKER}\nstep one"));
}

#[tokio::test]
async fn plan_exit_leaves_mode_unchanged_when_host_withholds_the_handle() {
    let mode = RunModeHandle::new(RunMode::Plan);
    let tool = PlanExitTool::new(Arc::new(Host { parked: false, changed: AtomicBool::new(false), mode: mode.clone() }));
    let parent = RunContext::new(RunConfig::new("plan-run"), ());
    let context = ToolExecutionContext::from_run_context(&parent, CallId::new("call-2"));

    let result = tool
        .execute_with_context(
            json!({"plan": "approved later"}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .expect("execute");

    assert_eq!(mode.get(), RunMode::Plan);
    assert_eq!(result.text(), format!("{PLAN_EXIT_MARKER}\napproved later"));
}

#[tokio::test]
async fn plan_exit_refuses_while_review_is_parked() {
    let mode = RunModeHandle::new(RunMode::Plan);
    let host = Arc::new(Host { parked: true, changed: AtomicBool::new(false), mode: mode.clone() });
    let tool = PlanExitTool::new(host.clone());
    let parent = RunContext::new(RunConfig::new("plan-run"), ());
    let context = ToolExecutionContext::from_run_context(&parent, CallId::new("call-3"));
    let result = tool.execute_with_context(json!({"plan":"step"}), ToolCallOptions::default(), Some(&context)).await.unwrap();
    assert!(result.is_error);
    assert_eq!(mode.get(), RunMode::Plan);
    assert!(!host.changed.load(Ordering::SeqCst));
}
