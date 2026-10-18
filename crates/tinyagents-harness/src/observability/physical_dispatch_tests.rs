use super::*;

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

#[test]
fn finishing_a_dispatch_does_not_emit_a_second_cancelled_terminal_on_drop() {
    let recorder = Arc::new(Recorder::default());
    let observer: Arc<dyn PhysicalDispatchObserver> = Arc::new(Observer(recorder.clone()));
    let guard = PhysicalDispatchGuard::new(observer.on_start(&PhysicalDispatchMetadata::default()));
    guard.finish(PhysicalDispatchOutcome {
        status: PhysicalDispatchStatus::Succeeded,
        duration: Duration::ZERO,
        usage: None,
        answered_model: None,
        finish_reason: None,
    });

    let outcomes = recorder.0.lock().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, PhysicalDispatchStatus::Succeeded);
}
