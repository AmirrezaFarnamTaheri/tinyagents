use super::*;
use crate::cost::ModelBudget;
use crate::tinyinference_llm::message::ImageRef;
use crate::tinyinference_llm::model::budget::{Budget, CallBudget, SpendLimits};
use crate::tinyinference_llm::model::{
    ChatModel, ModelRequest, ModelResolutionSource, ModelResponse, ResolvedModel,
    ResolvedModelRoute,
};
use crate::tinyinference_llm::{AssistantMessage, ContentBlock, Message, ToolSchema, Usage};
use async_trait::async_trait;
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Capture {
    requests: Mutex<Vec<ModelRequest>>,
    calls: AtomicUsize,
    response: Mutex<Option<ModelResponse>>,
    failure: Mutex<Option<String>>,
}

#[async_trait]
impl ChatModel<()> for Capture {
    async fn invoke(
        &self,
        _state: &(),
        request: ModelRequest,
    ) -> crate::tinyinference_llm::Result<ModelResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        if let Some(error) = self.failure.lock().unwrap().take() {
            return Err(crate::tinyinference_llm::Error::Model(error));
        }
        Ok(self
            .response
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| ModelResponse {
                message: assistant("reply"),
                usage: Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    total_tokens: 2,
                    ..Default::default()
                }),
                finish_reason: None,
                raw: None,
                resolved_model: None,
                continue_turn: None,
                served_from_cache: false,
                correlation: None,
                resolved_route: None,
            }))
    }
}

fn assistant(text: &str) -> AssistantMessage {
    match Message::assistant(text) {
        Message::Assistant(message) => message,
        _ => unreachable!(),
    }
}

fn options() -> OneshotOptions {
    OneshotOptions::default()
}

#[tokio::test]
async fn complete_once_requires_nonblank_model_and_preserves_the_raw_id() {
    let model = Arc::new(Capture::default());
    let missing = complete_once(model.clone(), ModelRequest::default(), options()).await;
    assert_eq!(
        missing.unwrap_err(),
        "complete_once: request.model is required"
    );
    let blank = complete_once(
        model.clone(),
        ModelRequest::new(vec![]).with_model(" \t "),
        options(),
    )
    .await;
    assert_eq!(
        blank.unwrap_err(),
        "complete_once: request.model is required"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);

    let request = ModelRequest::new(vec![Message::user("hi")]).with_model(" model with spaces ");
    complete_once(model.clone(), request, options())
        .await
        .unwrap();
    assert_eq!(
        model.requests.lock().unwrap()[0].model.as_deref(),
        Some(" model with spaces ")
    );
}

#[tokio::test]
async fn complete_once_rejects_tools_and_reserved_options_before_calling() {
    let model = Arc::new(Capture::default());
    let request = ModelRequest::new(vec![])
        .with_model("model")
        .with_tools(vec![ToolSchema::new(
            "tool",
            "description",
            json!({"type":"object"}),
        )]);
    assert_eq!(
        complete_once(model.clone(), request, options())
            .await
            .unwrap_err(),
        "complete_once: tools are not supported on a stateless completion; use an agent turn"
    );

    for key in RESERVED_PROVIDER_OPTIONS {
        let mut request = ModelRequest::new(vec![]).with_model("model");
        request.provider_options = json!({ (*key): "caller value" });
        assert!(
            complete_once(model.clone(), request, options())
                .await
                .unwrap_err()
                .contains(key)
        );
    }
    let mut duplicate = ModelRequest::new(vec![]).with_model("model");
    duplicate.provider_options = json!({"stream":true,"model":"override","messages":[]});
    assert!(
        complete_once(model.clone(), duplicate, options())
            .await
            .unwrap_err()
            .contains("`model`")
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn complete_once_forwards_request_and_full_response() {
    let usage = Usage {
        input_tokens: 3,
        output_tokens: 4,
        total_tokens: 7,
        reasoning_tokens: 2,
        ..Default::default()
    };
    let mut message = assistant("answer");
    message.usage = Some(usage);
    message.content.push(ContentBlock::Thinking {
        text: "private reasoning preserved".into(),
        signature: Some("provider-signature".into()),
    });
    let expected = ModelResponse {
        message,
        usage: Some(usage),
        finish_reason: Some("stop".into()),
        raw: Some(json!({"provider":"kept"})),
        resolved_model: Some(ResolvedModel {
            name: "selected-model".into(),
            requested: Some("requested-model".into()),
            source: ModelResolutionSource::RequestOverride,
        }),
        continue_turn: Some("continue".into()),
        served_from_cache: true,
        correlation: Some(crate::tinyinference_llm::model::ModelCallCorrelation::new(
            "run", "call",
        )),
        resolved_route: Some(ResolvedModelRoute::new(
            "openai",
            "provider-model",
            "route-a",
        )),
    };
    let model = Arc::new(Capture::default());
    *model.response.lock().unwrap() = Some(expected.clone());
    let mut user = Message::user("u");
    if let Message::User(user) = &mut user {
        user.content.push(ContentBlock::Image(ImageRef {
            url: "data:image/png;base64,AA==".into(),
            mime_type: Some("image/png".into()),
        }));
    }
    let mut request = ModelRequest::new(vec![Message::system("s"), user])
        .with_model("model")
        .with_response_format(crate::tinyinference_llm::model::ResponseFormat::JsonObject);
    request.max_tokens = Some(42);
    request.temperature = Some(0.25);
    request.provider_options = json!({"temperature": 0.2});
    request.metadata = json!({"caller": true});
    let actual = complete_once(model.clone(), request.clone(), options())
        .await
        .unwrap();
    assert_eq!(actual.message, expected.message);
    assert_eq!(actual.usage, expected.usage);
    assert_eq!(actual.finish_reason, expected.finish_reason);
    assert_eq!(actual.raw, expected.raw);
    assert_eq!(actual.resolved_model, expected.resolved_model);
    assert_eq!(actual.continue_turn, expected.continue_turn);
    assert_eq!(actual.served_from_cache, expected.served_from_cache);
    assert_eq!(actual.correlation, expected.correlation);
    assert_eq!(actual.resolved_route, expected.resolved_route);

    let captured = model.requests.lock().unwrap();
    assert_eq!(captured[0].messages, request.messages);
    assert_eq!(captured[0].response_format, request.response_format);
    assert_eq!(captured[0].max_tokens, Some(42));
    assert_eq!(captured[0].temperature, Some(0.25));
    assert_eq!(captured[0].provider_options, json!({"temperature": 0.2}));
    assert_eq!(captured[0].metadata, json!({"caller": true}));
}

#[tokio::test]
async fn simple_chat_appends_prompt_after_ordered_history() {
    let model = Arc::new(Capture::default());
    simple_chat(
        model.clone(),
        vec![Message::assistant("prior"), Message::system("context")],
        "next",
    )
    .await
    .unwrap();
    let captured = model.requests.lock().unwrap();
    assert_eq!(
        captured[0].messages,
        vec![
            Message::assistant("prior"),
            Message::system("context"),
            Message::user("next"),
        ]
    );
}

#[tokio::test]
async fn complete_once_keeps_provider_error_display_and_prefix() {
    let model = Arc::new(Capture::default());
    *model.failure.lock().unwrap() = Some("provider failed".into());
    let error = complete_once(
        model,
        ModelRequest::new(vec![]).with_model("model"),
        options(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, "complete_once: model error: provider failed");
}

#[tokio::test]
async fn invalid_requests_do_not_reserve_or_spend_budget() {
    let model = Arc::new(Capture::default());
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(8193),
            cost_micros: None,
        }),
        CallBudget {
            input_tokens: 8192,
            output_tokens: 1,
            cost_micros: 1,
        },
    );
    let invalid = ModelRequest::new(vec![])
        .with_model("model")
        .with_tools(vec![ToolSchema::new(
            "tool",
            "description",
            json!({"type":"object"}),
        )]);
    assert!(
        complete_once(
            model.clone(),
            invalid,
            OneshotOptions {
                budget: Some(budget.clone()),
            },
        )
        .await
        .is_err()
    );
    assert_eq!(budget.ledger.snapshot().spent.tokens, 0);
    assert_eq!(budget.ledger.snapshot().reserved.tokens, 0);
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);

    complete_once(
        model.clone(),
        ModelRequest::new(vec![Message::user("hi")]).with_model("model"),
        OneshotOptions {
            budget: Some(budget.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.ledger.snapshot().spent.tokens, 2);
}

#[tokio::test]
async fn budget_settles_a_call_once_and_refuses_when_reservation_exceeds_capacity() {
    let model = Arc::new(Capture::default());
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(8193),
            cost_micros: None,
        }),
        CallBudget {
            input_tokens: 8192,
            output_tokens: 1,
            cost_micros: 1,
        },
    );
    let request = || {
        ModelRequest::new(vec![Message::user("hi")])
            .with_model("model")
            .with_max_tokens(1)
    };
    complete_once(
        model.clone(),
        request(),
        OneshotOptions {
            budget: Some(budget.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.ledger.snapshot().spent.tokens, 2);
    assert_eq!(budget.ledger.snapshot().reserved.tokens, 0);

    assert!(
        complete_once(
            model.clone(),
            request(),
            OneshotOptions {
                budget: Some(budget.clone()),
            },
        )
        .await
        .is_err()
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.ledger.snapshot().spent.tokens, 2);
    assert_eq!(budget.ledger.snapshot().reserved.tokens, 0);
}
