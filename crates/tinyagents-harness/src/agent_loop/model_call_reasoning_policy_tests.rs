use super::*;

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{
    ChatModel, ModelRequest, ModelResponse, ModelStream, ModelStreamItem, ReasoningConfig,
    ReasoningEffort,
};

use crate::context::{RunConfig, RunContext};
use crate::events::AgentEvent;
use crate::runtime::{AgentHarness, RunPolicy};
use crate::testkit::EventRecorder;

/// A model that replays one scripted stream per call and records the
/// requests it was given, so a test can see what the loop sent after the
/// model completed a call.
struct ScriptedStreams {
    streams: Mutex<VecDeque<Vec<ModelStreamItem>>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedStreams {
    fn new(streams: Vec<Vec<ModelStreamItem>>) -> Self {
        Self {
            streams: Mutex::new(streams.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn next_items(&self, request: ModelRequest) -> Vec<ModelStreamItem> {
        self.requests.lock().unwrap().push(request);
        self.streams
            .lock()
            .unwrap()
            .pop_front()
            .expect("more model calls than scripted streams")
    }
}

#[async_trait]
impl ChatModel<()> for ScriptedStreams {
    async fn invoke(
        &self,
        _state: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        let mut accumulator = tinyinference_llm::model::StreamAccumulator::new();
        for item in self.next_items(request) {
            accumulator.push(&item);
        }
        accumulator.finish()
    }

    async fn stream(
        &self,
        _state: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        let items = self.next_items(request);
        Ok(ModelStream::new(Box::pin(futures::stream::iter(items))))
    }
}

fn reasoning_delta(chars: usize) -> ModelStreamItem {
    ModelStreamItem::MessageDelta(MessageDelta {
        text: String::new(),
        reasoning: "x".repeat(chars),
        tool_call: None,
    })
}

fn text_delta(text: &str) -> ModelStreamItem {
    ModelStreamItem::MessageDelta(MessageDelta {
        text: text.to_string(),
        reasoning: String::new(),
        tool_call: None,
    })
}

/// A stream that reasons for `chars` characters, then answers `text`.
fn reasoning_then_text(chars: usize, text: &str) -> Vec<ModelStreamItem> {
    let mut items = vec![ModelStreamItem::Started];
    for _ in 0..(chars / 100) {
        items.push(reasoning_delta(100));
    }
    items.push(text_delta(text));
    items.push(ModelStreamItem::Completed(ModelResponse::assistant(text)));
    items
}

#[tokio::test]
async fn reasoning_past_the_requested_budget_finishes_without_client_interruption() {
    let model = Arc::new(ScriptedStreams::new(vec![reasoning_then_text(
        4_000,
        "complete answer",
    )]));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(tinyagents_harness::host::FixedModelResolver::new(Arc::clone(&model) as _)));
    harness.with_policy(RunPolicy {
        default_reasoning: Some(ReasoningConfig {
            effort: Some(ReasoningEffort::High),
            budget_tokens: Some(200),
            ..ReasoningConfig::default()
        }),
        ..RunPolicy::default()
    });
    let recorder = EventRecorder::new();
    let ctx =
        RunContext::new(RunConfig::new("preserve-reasoning"), ()).with_events(recorder.sink());
    let run = harness
        .invoke_streaming_in_context(&(), ctx, vec![Message::user("answer")])
        .await
        .expect("stream completes");
    assert_eq!(run.text().as_deref(), Some("complete answer"));
    assert_eq!(model.requests().len(), 1);
    assert!(!recorder.events().iter().any(|e| matches!(e, AgentEvent::ControlApplied { control, .. } if control == "reasoning_watchdog" || control == "reasoning_fallback")));
}
