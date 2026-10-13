use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::Middleware;
use tinyinference_llm::message::Message;
use tinyinference_llm::tool::{ToolCall, ToolSchema};
use tinyinference_llm::usage::Usage;
use tinytools::ToolResult;

fn response(finish: &str, reasoning: u64, cached: bool) -> ModelResponse {
    let mut response = ModelResponse::assistant("{}")
        .with_usage(Usage {
            reasoning_tokens: reasoning,
            ..Default::default()
        })
        .with_finish_reason(finish);
    response.served_from_cache = cached;
    response
}

fn ctx() -> RunContext {
    RunContext::new(RunConfig::new("shape-test"), ())
}

fn middleware(shape: ResponseShape) -> (Arc<ResponseShapeScope>, ResponseShapeMiddleware) {
    let scope = ResponseShapeScope::new(shape);
    (Arc::clone(&scope), ResponseShapeMiddleware(scope))
}

fn request_with_tool() -> ModelRequest {
    ModelRequest::new(vec![Message::user("hi")]).with_tools(vec![ToolSchema::new(
        "t",
        "d",
        serde_json::json!({"type": "object"}),
    )])
}

#[test]
fn the_last_call_reports_and_reasoning_adds_up() {
    let scope = ResponseShapeScope::new(ResponseShape::default());
    record(&scope, &response("tool_calls", 3, false));
    record(&scope, &response("stop", 4, false));
    record(&scope, &response("stop", 100, true));

    let report = scope.report();
    assert_eq!(report.finish_reason.as_deref(), Some("stop"));
    assert_eq!(report.reasoning_tokens, 7, "a cache replay spent nothing");
}

#[test]
fn usage_is_summed_and_unknown_cost_poisons_the_total() {
    let scope = ResponseShapeScope::new(ResponseShape::default());
    let mut priced = response("stop", 0, false);
    priced.raw = Some(serde_json::json!({"model": "m-1", "usage": {"cost": 0.25}}));
    record(&scope, &priced);
    let report = scope.report();
    assert_eq!(report.answered_model.as_deref(), Some("m-1"));
    let usage = report.usage.expect("usage recorded");
    assert_eq!(usage.cost_usd, Some(0.25));
    assert!(usage.has_cost_receipt);
    assert!(!report.unknown_cost);

    record(&scope, &response("stop", 0, false));
    let report = scope.report();
    assert!(report.unknown_cost, "a call without a receipt is unknown");
    assert_eq!(report.usage.expect("usage").cost_usd, None);
}

#[test]
fn malformed_billing_stays_unknown_instead_of_falling_back() {
    let scope = ResponseShapeScope::new(ResponseShape::default());
    let mut bad = response("stop", 0, false);
    bad.raw = Some(serde_json::json!({"usage": {"cost": "free"}}));
    record(&scope, &bad);
    let report = scope.report();
    assert!(report.unknown_cost);
    assert!(report.usage.expect("usage").has_cost_receipt);
}

#[tokio::test]
async fn before_model_applies_format_cap_and_sampling() {
    let (_, mw) = middleware(ResponseShape {
        max_output_tokens: Some(64),
        top_p: Some(0.5),
        response_format: Some(ResponseFormat::JsonObject),
        provider_options: serde_json::json!({"k": 1}),
        ..Default::default()
    });
    let mut request = request_with_tool();
    mw.before_model(&mut ctx(), &(), &mut request)
        .await
        .expect("before_model");
    assert_eq!(request.max_tokens, Some(64));
    assert_eq!(request.top_p, Some(0.5));
    assert!(request.response_format.is_some());
    assert_eq!(request.provider_options, serde_json::json!({"k": 1}));
}

#[tokio::test]
async fn required_tool_call_forces_tools_until_one_succeeds() {
    let (_, mw) = middleware(ResponseShape {
        require_tool_call: true,
        response_format: Some(ResponseFormat::JsonObject),
        ..Default::default()
    });
    let mut request = request_with_tool();
    mw.before_model(&mut ctx(), &(), &mut request)
        .await
        .expect("before_model");
    assert!(request.response_format.is_none());
    assert!(matches!(request.tool_choice, ToolChoice::Required));

    let mut ok = ToolResult::success("done");
    mw.after_tool(
        &mut ctx(),
        &(),
        &ToolInvocationIdentity::new("c1", "t"),
        &mut ok,
    )
    .await
    .expect("after_tool");
    let mut request = request_with_tool();
    mw.before_model(&mut ctx(), &(), &mut request)
        .await
        .expect("before_model");
    assert!(
        request.response_format.is_some(),
        "format returns after a tool ran"
    );
}

#[tokio::test]
async fn required_tool_call_without_tools_is_a_structured_error() {
    let (_, mw) = middleware(ResponseShape {
        require_tool_call: true,
        ..Default::default()
    });
    let mut request = ModelRequest::new(vec![Message::user("hi")]);
    let err = mw
        .before_model(&mut ctx(), &(), &mut request)
        .await
        .expect_err("no tools");
    assert!(matches!(err, TinyAgentsError::StructuredOutput(_)));
}

#[tokio::test]
async fn a_toolless_answer_before_any_tool_success_is_rejected() {
    let (scope, mw) = middleware(ResponseShape {
        require_tool_call: true,
        ..Default::default()
    });
    let mut answer = response("stop", 0, false);
    let err = mw
        .after_model(&mut ctx(), &(), &mut answer)
        .await
        .expect_err("rejected");
    assert!(matches!(err, TinyAgentsError::StructuredOutput(_)));
    assert_eq!(
        scope.report().validation_error.as_deref(),
        Some("RequiredToolCallMissing")
    );
    mw.on_error(&mut ctx(), &err).await.expect("on_error");
    assert!(scope.report().structured_failed);
}

struct RejectEmptyObject;
impl ResponseValidator for RejectEmptyObject {
    fn validate(&self, text: &str, _finish: Option<&str>) -> Result<(), String> {
        if text == "{}" {
            Err("EmptyObject".into())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn validator_failures_are_recorded_and_feed_the_repair_loop() {
    let (scope, mw) = middleware(ResponseShape {
        validator: Some(Arc::new(RejectEmptyObject)),
        ..Default::default()
    });
    let mut answer = response("stop", 0, false);
    mw.after_model(&mut ctx(), &(), &mut answer)
        .await
        .expect("after_model");
    let report = scope.report();
    assert_eq!(report.structured_attempts, 1);
    assert_eq!(report.validation_error.as_deref(), Some("EmptyObject"));

    let strict = StrictValidator(Arc::clone(&scope));
    let err = OutputValidator::<(), ()>::validate(&strict, &mut ctx(), &(), &serde_json::json!({}))
        .await
        .expect_err("retry");
    assert!(matches!(err, TinyAgentsError::ModelRetry(reason) if reason == "EmptyObject"));
}

#[tokio::test]
async fn a_tool_call_response_is_not_counted_as_an_answer_attempt() {
    let (scope, mw) = middleware(ResponseShape {
        validator: Some(Arc::new(RejectEmptyObject)),
        ..Default::default()
    });
    let mut call = ModelResponse::assistant("");
    call.message.tool_calls = vec![ToolCall::new("c", "t", serde_json::json!({}))];
    mw.after_model(&mut ctx(), &(), &mut call)
        .await
        .expect("after_model");
    assert_eq!(scope.report().structured_attempts, 0);
}

#[test]
fn install_arms_output_repair_only_when_a_validator_is_present() {
    let mut plain: AgentHarness<(), ()> = AgentHarness::new();
    install_response_shape(
        &mut plain,
        ResponseShapeScope::new(ResponseShape {
            max_output_tokens: Some(64),
            ..Default::default()
        }),
    );
    assert!(plain.policy().default_response_format.is_none());

    let mut strict: AgentHarness<(), ()> = AgentHarness::new();
    install_response_shape(
        &mut strict,
        ResponseShapeScope::new(ResponseShape {
            validator: Some(Arc::new(RejectEmptyObject)),
            structured_retries: 2,
            ..Default::default()
        }),
    );
    let policy = strict.policy();
    assert!(policy.default_response_format.is_some());
    assert_eq!(policy.output_retry.max_attempts, 2);
    assert_eq!(policy.truncated_empty_retries, 0);
    assert_eq!(policy.truncated_empty_nudges, 0);
}

#[tokio::test]
async fn a_validator_keeps_the_schema_when_the_shape_asks_for_json_object() {
    let (scope, mw) = middleware(ResponseShape {
        validator: Some(Arc::new(RejectEmptyObject)),
        response_format: Some(ResponseFormat::JsonObject),
        ..Default::default()
    });
    let permissive = ResponseFormat::JsonSchema {
        name: "host_answer".into(),
        schema: serde_json::json!({}),
    };
    let mut request = request_with_tool();
    request.response_format = Some(permissive.clone());
    mw.before_model(&mut ctx(), &(), &mut request)
        .await
        .expect("before_model");
    assert_eq!(request.response_format, Some(permissive));
    assert!(scope.shape().validator.is_some());
}
