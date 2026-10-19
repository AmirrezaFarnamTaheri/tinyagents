use super::*;
use crate::cost::ModelBudget;
use crate::tinyinference_llm::model::budget::{Budget, CallBudget, SpendLimits};
use crate::tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};
use crate::tinyinference_llm::{Message, ToolSchema};

use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};

#[derive(Default)]
struct Capture {
    requests: Mutex<Vec<ModelRequest>>,
    calls: AtomicUsize,
    response: Mutex<Option<ModelResponse>>,
}

#[async_trait]
impl ChatModel<()> for Capture {
    async fn invoke(&self, _state: &(), request: ModelRequest) -> crate::tinyinference_llm::Result<ModelResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        Ok(self.response.lock().unwrap().take().unwrap_or_else(|| ModelResponse {
            message: assistant("reply"),
            usage: None,
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

fn assistant(text: &str) -> crate::tinyinference_llm::AssistantMessage {
    match Message::assistant(text) { Message::Assistant(message) => message, _ => unreachable!() }
}

fn options() -> OneshotOptions {
    OneshotOptions::default()
}

#[tokio::test]
async fn complete_once_requires_nonblank_model_and_preserves_the_raw_id() {
    let model = Arc::new(Capture::default());
    let missing = complete_once(model.clone(), ModelRequest::default(), options()).await;
    assert!(missing.is_err());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);

    let mut request = ModelRequest::new(vec![Message::user("hi")]);
    request.model = Some(" model with spaces ".into());
    complete_once(model.clone(), request, options()).await.unwrap();
    assert_eq!(model.requests.lock().unwrap()[0].model.as_deref(), Some(" model with spaces "));
}

#[tokio::test]
async fn complete_once_rejects_tools_and_reserved_provider_options_before_calling() {
    let model = Arc::new(Capture::default());
    let mut request = ModelRequest::new(vec![]).with_model("model");
    request.tools.push(ToolSchema::new("tool", "description", json!({"type":"object"})));
    assert!(complete_once(model.clone(), request, options()).await.is_err());

    for key in RESERVED_PROVIDER_OPTIONS {
        let mut request = ModelRequest::new(vec![]).with_model("model");
        request.provider_options = json!({ key: "caller value" });
        assert!(complete_once(model.clone(), request, options()).await.is_err(), "{key}");
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn complete_once_forwards_request_and_full_response() {
    let expected = ModelResponse {
        message: assistant("answer"),
        usage: Some(crate::tinyinference_llm::usage::Usage { input_tokens: 3, output_tokens: 4, ..Default::default() }),
        finish_reason: Some("stop".into()),
        raw: Some(json!({"provider":"kept"})),
        resolved_model: None,
        continue_turn: None,
        served_from_cache: true,
        correlation: None,
        resolved_route: None,
    };
    let model = Arc::new(Capture::default());
    *model.response.lock().unwrap() = Some(expected.clone());
    let mut request = ModelRequest::new(vec![Message::system("s"), Message::user("u")]).with_model("model");
    request.max_tokens = Some(42);
    request.provider_options = json!({"temperature": 0.2});
    request.metadata = json!({"caller": true});
    let actual = complete_once(model.clone(), request, options()).await.unwrap();
    assert_eq!(actual.usage.unwrap().total_tokens, expected.usage.unwrap().total_tokens);
    assert_eq!(actual.raw, expected.raw);
    assert_eq!(actual.finish_reason, expected.finish_reason);
    let captured = model.requests.lock().unwrap();
    assert_eq!(captured[0].messages.len(), 2);
    assert_eq!(captured[0].max_tokens, Some(42));
    assert_eq!(captured[0].provider_options, json!({"temperature": 0.2}));
    assert_eq!(captured[0].metadata, json!({"caller": true}));
}

#[tokio::test]
async fn simple_chat_appends_prompt_after_ordered_history() {
    let model = Arc::new(Capture::default());
    simple_chat(model.clone(), vec![Message::assistant("prior")], "next")
        .await.unwrap();
    let captured = model.requests.lock().unwrap();
    assert_eq!(captured[0].messages, vec![
        Message::assistant("prior"),
        Message::user("next"),
    ]);
}

#[tokio::test]
async fn budget_rejection_never_reaches_model() {
    let model = Arc::new(Capture::default());
    let budget = ModelBudget::new(Budget::new(SpendLimits { tokens: Some(0), cost_micros: None }), CallBudget { input_tokens: 8, output_tokens: 8, cost_micros: 1 });
    let mut request = ModelRequest::new(vec![Message::user("hi")]).with_model("model");
    request.max_tokens = Some(8);
    assert!(complete_once(model.clone(), request, OneshotOptions { budget: Some(budget) }).await.is_err());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
