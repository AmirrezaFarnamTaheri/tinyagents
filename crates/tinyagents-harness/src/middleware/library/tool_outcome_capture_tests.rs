use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::Middleware;
use crate::turn_outcome::new_tool_outcome_sink;
use tinyinference_llm::tool::ToolCall;

#[tokio::test]
async fn outcome_capture_records_arguments_and_final_result_in_shared_sink() {
    let sink = new_tool_outcome_sink();
    let middleware = ToolOutcomeCaptureMiddleware::new(Arc::clone(&sink));
    let mut ctx = RunContext::new(RunConfig::new("outcome-test"), ());
    let mut call = ToolCall::new("call-1", "search", serde_json::json!({"q": "cats"}));
    middleware
        .before_tool(&mut ctx, &(), &mut call)
        .await
        .unwrap();
    let invocation = ToolInvocationIdentity::new("call-1", "search");
    let mut result = ToolResult::error("redacted by host policy");
    middleware
        .after_tool(&mut ctx, &(), &invocation, &mut result)
        .await
        .unwrap();

    let outcomes = sink.lock().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].arguments, serde_json::json!({"q": "cats"}));
    assert!(!outcomes[0].success);
    assert_eq!(outcomes[0].content, "redacted by host policy");
}

#[tokio::test]
async fn reused_call_ids_are_isolated_between_run_contexts() {
    let capture = ToolCallCapture::default();
    let first = RunContext::new(RunConfig::new("first"), ());
    let second = RunContext::new(RunConfig::new("second"), ());
    let first_call = ToolCall::new("shared-id", "search", serde_json::json!({"run": 1}));
    let second_call = ToolCall::new("shared-id", "search", serde_json::json!({"run": 2}));
    capture.start(&first, &first_call);
    capture.start(&second, &second_call);

    let invocation = ToolInvocationIdentity::new("shared-id", "search");
    let result = ToolResult::success("ok");
    assert_eq!(
        capture.finish(&first, &invocation, &result).arguments,
        serde_json::json!({"run": 1})
    );
    assert_eq!(
        capture.finish(&second, &invocation, &result).arguments,
        serde_json::json!({"run": 2})
    );
}
