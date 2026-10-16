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
async fn repeated_failures_nudge_before_halt() {
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

    assert!(
        halt_summary
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|summary| summary.starts_with("Stopping:"))
    );
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );
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

#[tokio::test]
async fn classified_budget_uses_shared_tracker_and_host_halt_copy() {
    struct ClassifiedPolicy;
    impl crate::no_progress::FailurePolicy for ClassifiedPolicy {
        fn identical_halt_summary(&self, _: &str, _: u32, _: &str) -> String {
            String::new()
        }
        fn no_progress_halt_summary(&self, _: u32, _: &str, _: &str) -> String {
            String::new()
        }
    }
    impl FailureScopePolicy<()> for ClassifiedPolicy {
        fn assess(
            &self,
            _: &RunContext<()>,
            _: &ToolInvocationIdentity,
            _: &FailureScope,
            _: &ToolResult,
        ) -> FailureAssessment {
            FailureAssessment::ClassifiedFailure {
                class: "permission".into(),
                budget: 0,
                error: "denied".into(),
                nudge: None,
            }
        }

        fn halt_copy(&self, kind: FailureStopKind, summary: String, _: &str) -> String {
            assert_eq!(kind, FailureStopKind::Classified);
            format!("host copy: {summary}")
        }
    }

    let handle = SteeringHandle::allow_all();
    let summary = Arc::new(std::sync::Mutex::new(None));
    let middleware =
        FailureScopeMiddleware::new(handle.clone(), summary.clone(), Arc::new(ClassifiedPolicy));
    fail_once(&middleware, "classified").await;

    let summary = summary.lock().unwrap();
    let summary = summary
        .as_deref()
        .expect("classified budget halts on its first failure");
    assert!(summary.starts_with("host copy:"));
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );
}

#[tokio::test]
async fn terminal_model_callback_halts_before_tool_accounting() {
    struct ModelPolicy;
    impl crate::no_progress::FailurePolicy for ModelPolicy {
        fn identical_halt_summary(&self, _: &str, _: u32, _: &str) -> String {
            String::new()
        }
        fn no_progress_halt_summary(&self, _: u32, _: &str, _: &str) -> String {
            String::new()
        }
    }
    impl FailureScopePolicy<()> for ModelPolicy {
        fn assess(
            &self,
            _: &RunContext<()>,
            _: &ToolInvocationIdentity,
            _: &FailureScope,
            _: &ToolResult,
        ) -> FailureAssessment {
            FailureAssessment::Ignore
        }

        fn terminal_model_halt(
            &self,
            _: &RunContext<()>,
            _: &tinyinference_llm::model::ModelResponse,
        ) -> Option<(String, Option<ClassifiedStopCause>)> {
            Some(("provider budget exhausted".into(), None))
        }
    }

    let handle = SteeringHandle::allow_all();
    let summary = Arc::new(std::sync::Mutex::new(None));
    let middleware =
        FailureScopeMiddleware::new(handle.clone(), summary.clone(), Arc::new(ModelPolicy));
    let mut ctx = context();
    let mut response = tinyinference_llm::model::ModelResponse::assistant("unavailable");
    middleware
        .after_model(&mut ctx, &(), &mut response)
        .await
        .unwrap();

    assert_eq!(
        summary.lock().unwrap().as_deref(),
        Some("provider budget exhausted")
    );
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );
}
