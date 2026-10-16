//! Behavioral tests for the host-classified failure-scope driver.
use std::sync::Arc;

use serde_json::json;
use tinyinference_llm::tool::ToolCall;
use tinytools::ToolResult;

use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::no_progress::ClassifiedStopCause;
use crate::steering::{SteeringCommand, SteeringHandle};

struct Policy;

impl crate::no_progress::FailurePolicy for Policy {
    fn identical_halt_summary(&self, tool: &str, count: u32, _failure_text: &str) -> String {
        format!("{tool} failed {count} times")
    }

    fn no_progress_halt_summary(&self, count: u32, tool: &str, _failure_text: &str) -> String {
        format!("{tool} made no progress in {count} calls")
    }
}

impl FailureScopePolicy<()> for Policy {
    fn assess(
        &self,
        _ctx: &RunContext<()>,
        _invocation: &ToolInvocationIdentity,
        _scope: &FailureScope,
        result: &ToolResult,
    ) -> FailureAssessment {
        if result.is_error {
            FailureAssessment::Failure {
                error: result.output(),
                hard_reject: false,
                recoverable_miss: false,
            }
        } else {
            FailureAssessment::Progress
        }
    }
}

fn context() -> RunContext<()> {
    let mut ctx = RunContext::new(RunConfig::new("failure-scope"), ());
    ctx.instance_id = 41;
    ctx
}

async fn fail_once(middleware: &FailureScopeMiddleware<()>, id: &str) {
    let mut ctx = context();
    let mut call = ToolCall::new(id, "read_file", json!({"path": "a.txt"}));
    middleware
        .before_tool(&mut ctx, &(), &mut call)
        .await
        .unwrap();
    let invocation = ToolInvocationIdentity::new(id, "read_file");
    let mut result = ToolResult::error("permission denied");
    middleware
        .after_tool(&mut ctx, &(), &invocation, &mut result)
        .await
        .unwrap();
}

#[tokio::test]
async fn repeated_failures_nudge_before_halt_and_cleanup_is_run_scoped() {
    let handle = SteeringHandle::allow_all();
    let halt_summary = Arc::new(std::sync::Mutex::new(None));
    let middleware =
        FailureScopeMiddleware::new(handle.clone(), halt_summary.clone(), Arc::new(Policy));

    fail_once(&middleware, "one").await;
    assert!(halt_summary.lock().unwrap().is_none());
    fail_once(&middleware, "two").await;
    assert!(middleware.nudges.drain_for(&context()).len() > 0);
    assert!(halt_summary.lock().unwrap().is_none());
    fail_once(&middleware, "three").await;

    assert!(halt_summary.lock().unwrap().is_some());
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );

    let mut next_run = context();
    next_run.instance_id = 42;
    let mut call = ToolCall::new("fresh", "read_file", json!({"path": "a.txt"}));
    middleware
        .before_tool(&mut next_run, &(), &mut call)
        .await
        .unwrap();
    let invocation = ToolInvocationIdentity::new("fresh", "read_file");
    let mut result = ToolResult::success("read");
    middleware
        .after_tool(&mut next_run, &(), &invocation, &mut result)
        .await
        .unwrap();
    assert_eq!(middleware.nudges.drain_for(&next_run), Vec::<String>::new());
}

#[tokio::test]
async fn host_can_classify_terminal_failures_with_a_typed_cause() {
    struct TerminalPolicy;
    impl crate::no_progress::FailurePolicy for TerminalPolicy {
        fn identical_halt_summary(&self, _: &str, _: u32, _: &str) -> String {
            String::new()
        }
        fn no_progress_halt_summary(&self, _: u32, _: &str, _: &str) -> String {
            String::new()
        }
    }
    impl FailureScopePolicy<()> for TerminalPolicy {
        fn assess(
            &self,
            _: &RunContext<()>,
            _: &ToolInvocationIdentity,
            _: &FailureScope,
            _: &ToolResult,
        ) -> FailureAssessment {
            FailureAssessment::Halt {
                summary: "terminal inference failure".into(),
                cause: Some(ClassifiedStopCause {
                    class: "inference".into(),
                    operation: "transport".into(),
                    summary: "terminal inference failure".into(),
                }),
            }
        }
    }

    let handle = SteeringHandle::allow_all();
    let summary = Arc::new(std::sync::Mutex::new(None));
    let middleware =
        FailureScopeMiddleware::new(handle.clone(), summary.clone(), Arc::new(TerminalPolicy));
    fail_once(&middleware, "terminal").await;
    assert_eq!(
        summary.lock().unwrap().as_deref(),
        Some("terminal inference failure")
    );
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );
}
