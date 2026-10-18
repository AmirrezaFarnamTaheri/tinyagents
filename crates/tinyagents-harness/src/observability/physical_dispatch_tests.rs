use super::*;

use futures::StreamExt;
use std::sync::Mutex;
use tinyinference_llm::model::ModelResponse;

#[derive(Default)]
struct Recorder(Mutex<Vec<PhysicalDispatchOutcome>>);

struct Terminal(Arc<Recorder>);

impl PhysicalDispatchTerminal for Terminal {
    fn on_terminal(self: Box<Self>, outcome: PhysicalDispatchOutcome) {
        self.0.0.lock().unwrap().push(outcome);
    }
}

struct Observer(Arc<Recorder>);

impl PhysicalDispatchObserver for Observer {
    fn on_start(&self, _: &PhysicalDispatchMetadata) -> Box<dyn PhysicalDispatchTerminal> {
        Box::new(Terminal(Arc::clone(&self.0)))
    }
}

struct PendingModel;

#[async_trait::async_trait]
impl ChatModel<()> for PendingModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn dropping_an_inflight_provider_future_reports_one_cancelled_terminal() {
    let recorder = Arc::new(Recorder::default());
    let observer: Arc<dyn PhysicalDispatchObserver> = Arc::new(Observer(recorder.clone()));
    let model = Arc::new(ObservedChatModel::new(Arc::new(PendingModel), observer));
    let call = tokio::spawn(async move { model.invoke(&(), ModelRequest::new(Vec::new())).await });
    tokio::task::yield_now().await;
    call.abort();
    let _ = call.await;

    let outcomes = recorder.0.lock().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, PhysicalDispatchStatus::Cancelled);
}

struct StreamingModel;

#[async_trait::async_trait]
impl ChatModel<()> for StreamingModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        Ok(ModelResponse::assistant("invoke fallback"))
    }

    async fn stream(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelStream> {
        let items =
            futures::stream::iter([ModelStreamItem::Started]).chain(futures::stream::pending());
        Ok(ModelStream::new(Box::pin(items)))
    }
}

#[tokio::test]
async fn dropping_a_stream_after_a_chunk_reports_cancellation() {
    let recorder = Arc::new(Recorder::default());
    let observer: Arc<dyn PhysicalDispatchObserver> = Arc::new(Observer(recorder.clone()));
    let model = ObservedChatModel::new(Arc::new(StreamingModel), observer);
    let mut stream = model
        .stream(&(), ModelRequest::new(Vec::new()))
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(ModelStreamItem::Started)
    ));
    drop(stream);

    let outcomes = recorder.0.lock().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, PhysicalDispatchStatus::Cancelled);
}

#[test]
fn refusal_metadata_is_reported_without_including_provider_text() {
    let response = ModelResponse {
        raw: Some(serde_json::json!({
            "choices": [{"message": {"refusal": "private refusal text"}}]
        })),
        ..ModelResponse::assistant("private refusal text")
    };
    let outcome = dispatch_outcome(Some(&response), true);

    assert_eq!(outcome.status, PhysicalDispatchStatus::Refused);
    assert!(outcome.refused);
    assert_eq!(
        format!("{outcome:?}").contains("private refusal text"),
        false
    );
}

#[test]
fn finishing_a_dispatch_does_not_emit_a_second_cancelled_terminal_on_drop() {
    let recorder = Arc::new(Recorder::default());
    let observer: Arc<dyn PhysicalDispatchObserver> = Arc::new(Observer(recorder.clone()));
    let guard = PhysicalDispatchGuard::new(observer.on_start(&PhysicalDispatchMetadata::default()));
    guard.finish(PhysicalDispatchOutcome {
        status: PhysicalDispatchStatus::Succeeded,
        refused: false,
        duration: Duration::ZERO,
        usage: None,
        answered_model: None,
        finish_reason: None,
    });

    let outcomes = recorder.0.lock().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, PhysicalDispatchStatus::Succeeded);
}
