//! Behavioral tests for the host-classified failure-scope driver.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use tinyinference_llm::tool::ToolCall;
use tinytools::ToolResult;

use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::no_progress::ClassifiedStopCause;
use crate::steering::{SteeringCommand, SteeringHandle};

#[derive(Default)]
struct Policy {
    guard_refusals: AtomicUsize,
}

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
        call: &ToolCall,
        _scope: &FailureScope,
        result: &ToolResult,
    ) -> FailureAssessment {
        if call.name == "memory_search" || call.name == "repeat_guard" {
            return FailureAssessment::Ignore;
        }
        assert_eq!(call.name, "read_file");
        assert_eq!(call.arguments, json!({"path": "a.txt"}));
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

    fn is_repeat_guard_refusal(
        &self,
        _ctx: &RunContext<()>,
        call: &ToolCall,
        result: &ToolResult,
    ) -> bool {
        call.name == "repeat_guard" && result.is_error
    }

    fn record_guard_refusal(&self, _ctx: &RunContext<()>) {
        self.guard_refusals.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ResetAndCallCopyPolicy;

impl crate::no_progress::FailurePolicy for ResetAndCallCopyPolicy {
    fn identical_halt_summary(&self, tool: &str, count: u32, _failure_text: &str) -> String {
        format!("{tool} failed {count} times")
    }

    fn no_progress_halt_summary(&self, count: u32, tool: &str, _failure_text: &str) -> String {
        format!("{tool} made no progress in {count} calls")
    }
}

impl FailureScopePolicy<()> for ResetAndCallCopyPolicy {
    fn assess(
        &self,
        _ctx: &RunContext<()>,
        _invocation: &ToolInvocationIdentity,
        call: &ToolCall,
        _scope: &FailureScope,
        _result: &ToolResult,
    ) -> FailureAssessment {
        let assessment = if call.arguments.get("reset") == Some(&json!(true)) {
            FailureAssessment::ResetFailure {
                error: "new command exit".into(),
                hard_reject: false,
                recoverable_miss: false,
            }
        } else {
            FailureAssessment::Failure {
                error: "failed".into(),
                hard_reject: false,
                recoverable_miss: false,
            }
        };
        assessment
    }

    fn halt_copy_for_call(
        &self,
        _ctx: &RunContext<()>,
        _kind: FailureStopKind,
        summary: String,
        _error: &str,
        call: &ToolCall,
    ) -> String {
        format!("{}: {summary}", call.name)
    }
}

fn context(instance_id: u64) -> RunContext<()> {
    let mut ctx = RunContext::new(RunConfig::new("failure-scope"), ());
    ctx.instance_id = instance_id;
    ctx
}

async fn observe_once(
    middleware: &FailureScopeMiddleware<()>,
    run_id: u64,
    id: &str,
    tool: &str,
    arguments: serde_json::Value,
    result: ToolResult,
) {
    let mut ctx = context(run_id);
    let mut call = ToolCall::new(id, tool, arguments);
    middleware
        .before_tool(&mut ctx, &(), &mut call)
        .await
        .unwrap();
    let invocation = ToolInvocationIdentity::new(id, tool);
    let mut result = result;
    middleware
        .after_tool(&mut ctx, &(), &invocation, &mut result)
        .await
        .unwrap();
}

async fn fail_once(middleware: &FailureScopeMiddleware<()>, run_id: u64, id: &str) {
    observe_once(
        middleware,
        run_id,
        id,
        "read_file",
        json!({"path": "a.txt"}),
        ToolResult::error("permission denied"),
    )
    .await;
}

#[tokio::test]
async fn repeated_failures_nudge_before_halt() {
    let handle = SteeringHandle::allow_all();
    let halt_summary = Arc::new(std::sync::Mutex::new(None));
    let middleware = FailureScopeMiddleware::new(
        handle.clone(),
        halt_summary.clone(),
        Arc::new(Policy::default()),
    );

    fail_once(&middleware, 41, "one").await;
    assert!(halt_summary.lock().unwrap().is_none());
    fail_once(&middleware, 41, "two").await;
    assert!(!middleware.nudges.drain_for(&context(41)).is_empty());
    assert!(halt_summary.lock().unwrap().is_none());
    fail_once(&middleware, 41, "three").await;

    assert!(
        halt_summary
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|summary| {
                summary.starts_with("Stopping: the `read_file` call was retried 3 times")
            })
    );
    assert!(
        handle
            .drain()
            .iter()
            .any(|command| matches!(command, SteeringCommand::Pause))
    );
}

#[tokio::test]
async fn reset_failure_starts_new_ladder_without_host_progress_callback() {
    let handle = SteeringHandle::allow_all();
    let halt_summary = Arc::new(std::sync::Mutex::new(None));
    let middleware = FailureScopeMiddleware::with_identical_threshold(
        handle.clone(),
        halt_summary.clone(),
        Arc::new(ResetAndCallCopyPolicy),
        2,
    );
    let call = |id: &str, reset: bool| {
        observe_once(
            &middleware,
            89,
            id,
            "shell",
            json!({"reset": reset}),
            ToolResult::error("exit status 1"),
        )
    };

    call("first", false).await;
    call("second", false).await;
    assert!(halt_summary.lock().unwrap().is_none());
    call("new-command", true).await;
    assert!(halt_summary.lock().unwrap().is_none());
    call("new-command-repeat", false).await;
    assert!(halt_summary.lock().unwrap().is_none());
    call("new-command-repeat-again", false).await;
    assert_eq!(
        halt_summary.lock().unwrap().as_deref(),
        Some(
            "shell: Stopping: the `shell` call was retried 2 times with the same error. The environment is not making progress. Report the blocker to the user."
        )
    );
}

#[tokio::test]
async fn separate_run_instances_keep_halts_and_pause_handles_isolated() {
    let first_handle = SteeringHandle::allow_all();
    let first_summary = Arc::new(std::sync::Mutex::new(None));
    let first = FailureScopeMiddleware::new(
        first_handle.clone(),
        first_summary.clone(),
        Arc::new(Policy::default()),
    );
    let second_handle = SteeringHandle::allow_all();
    let second_summary = Arc::new(std::sync::Mutex::new(None));
    let second = FailureScopeMiddleware::new(
        second_handle.clone(),
        second_summary.clone(),
        Arc::new(Policy::default()),
    );

    for n in 0..3 {
        fail_once(&first, 51, &format!("first-{n}")).await;
    }
    assert!(first_summary.lock().unwrap().is_some());
    assert!(second_summary.lock().unwrap().is_none());
    assert!(
        first_handle
            .drain()
            .iter()
            .any(|c| matches!(c, SteeringCommand::Pause))
    );
    assert!(second_handle.drain().is_empty());

    for n in 0..3 {
        fail_once(&second, 52, &format!("second-{n}")).await;
    }
    assert!(second_summary.lock().unwrap().is_some());
    assert!(
        second_handle
            .drain()
            .iter()
            .any(|c| matches!(c, SteeringCommand::Pause))
    );
}

#[tokio::test]
async fn memory_and_repeat_guard_exemptions_do_not_change_failure_budget_or_step() {
    let handle = SteeringHandle::allow_all();
    let summary = Arc::new(std::sync::Mutex::new(None));
    let policy = Arc::new(Policy::default());
    let middleware = FailureScopeMiddleware::new(handle.clone(), summary.clone(), policy.clone());

    fail_once(&middleware, 61, "first").await;
    for n in 0..8 {
        observe_once(
            &middleware,
            61,
            &format!("memory-{n}"),
            "memory_search",
            json!({"query": "x"}),
            ToolResult::error("memory budget reached"),
        )
        .await;
        observe_once(
            &middleware,
            61,
            &format!("guard-{n}"),
            "repeat_guard",
            json!({"path": "a.txt"}),
            ToolResult::error("repeat guard refused"),
        )
        .await;
    }
    assert_eq!(policy.guard_refusals.load(Ordering::SeqCst), 8);
    assert!(summary.lock().unwrap().is_none());
    assert!(handle.drain().is_empty());

    fail_once(&middleware, 61, "second").await;
    let nudge = middleware.nudges.drain_for(&context(61));
    assert_eq!(nudge.len(), 1);
    assert!(nudge[0].starts_with("[no progress since step 2]"));
    assert!(summary.lock().unwrap().is_none());

    fail_once(&middleware, 61, "third").await;
    assert!(summary.lock().unwrap().is_some());
    assert!(
        handle
            .drain()
            .iter()
            .any(|c| matches!(c, SteeringCommand::Pause))
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
            _: &ToolCall,
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
    fail_once(&middleware, 41, "terminal").await;
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
            _: &ToolCall,
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
    fail_once(&middleware, 41, "classified").await;

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
            _: &ToolCall,
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
    let mut ctx = context(41);
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
