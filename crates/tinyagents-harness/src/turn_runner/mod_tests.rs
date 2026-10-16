use super::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Host {
    queued: Mutex<Vec<QueueDisposition>>,
}

#[async_trait::async_trait]
impl TurnHost for Host {
    async fn checkout(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: request.thread_id.clone(),
            fingerprint: Some("stable".into()),
        })
    }

    fn run_origin(&self, request: &TurnRequest) -> serde_json::Value {
        serde_json::json!({"request": request.request_id})
    }

    async fn run(
        &self,
        _request: &TurnRequest,
        _session: &TurnSession,
        _origin: &serde_json::Value,
        _cancellation: crate::CancellationToken,
        _progress: tokio::sync::mpsc::Sender<crate::progress::AgentProgress>,
    ) -> Result<TurnResult, String> {
        Ok(TurnResult {
            reply: "done".into(),
            data: serde_json::Value::Null,
        })
    }

    async fn checkin(&self, _request: &TurnRequest) {}
    async fn cancel_children(&self, _request: &TurnRequest) {}

    async fn queue(&self, _request: &TurnRequest, mode: QueueDisposition) -> Result<bool, String> {
        self.queued.lock().unwrap().push(mode);
        Ok(true)
    }
}

#[derive(Default)]
struct Ingress(Mutex<Vec<IngressStage>>);

#[async_trait::async_trait]
impl IngressPolicy for Ingress {
    async fn apply(
        &self,
        stage: IngressStage,
        _request: &mut TurnRequest,
    ) -> Result<IngressDecision, String> {
        self.0.lock().unwrap().push(stage);
        Ok(IngressDecision::Continue)
    }
}

#[derive(Default)]
struct Replies(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl ReplyStore for Replies {
    async fn persist(
        &self,
        _request: &TurnRequest,
        reply_id: &str,
        _result: &TurnResult,
    ) -> Result<(), String> {
        self.0.lock().unwrap().push(reply_id.to_owned());
        Ok(())
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<TerminalEvent>>);

#[async_trait::async_trait]
impl EventSink for Events {
    async fn progress(&self, _request: &TurnRequest, _event: crate::progress::AgentProgress) {}
    async fn terminal(&self, _request: &TurnRequest, event: TerminalEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct Failures;
impl FailureClassifier for Failures {
    fn classify(&self, error: &str) -> (String, String) {
        ("internal".into(), error.into())
    }
}

struct Summary;
#[async_trait::async_trait]
impl Summarizer for Summary {
    async fn grounded_close(&self, _request: &TurnRequest, _reason: &str) -> Option<TurnResult> {
        None
    }
}

fn runner(
    host: Arc<Host>,
    ingress: Arc<Ingress>,
    replies: Arc<Replies>,
    events: Arc<Events>,
) -> TurnRunner<Host, Ingress, Replies, Events, Failures, Summary> {
    TurnRunner::new(
        host,
        ingress,
        replies,
        events,
        Arc::new(Failures),
        Arc::new(Summary),
        TurnRunnerConfig::default(),
    )
}

fn request(disposition: QueueDisposition, host_authored: bool) -> TurnRequest {
    TurnRequest {
        scope: ScopeKey {
            agent_id: "agent-a".into(),
            profile_id: None,
        },
        thread_id: "thread-a".into(),
        request_id: "request-a".into(),
        text: "hello".into(),
        host_authored,
        disposition,
        deadline: None,
        idle_timeout: Some(std::time::Duration::from_secs(5)),
    }
}

#[tokio::test]
async fn ingress_stages_run_in_order_before_queue_acceptance() {
    let host = Arc::new(Host::default());
    let ingress = Arc::new(Ingress::default());
    let runner = runner(
        host.clone(),
        ingress.clone(),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    );

    assert!(matches!(
        runner.run(request(QueueDisposition::Steer, false)).await,
        Err(TurnRunnerError::Queued)
    ));
    assert_eq!(
        *ingress.0.lock().unwrap(),
        [
            IngressStage::StageAttachments,
            IngressStage::Guardrail,
            IngressStage::ApprovalInterception,
            IngressStage::BeforeSubmit,
        ]
    );
    assert_eq!(*host.queued.lock().unwrap(), [QueueDisposition::Steer]);
}

#[tokio::test]
async fn host_authored_turn_persists_reply_without_an_inflight_slot() {
    let events = Arc::new(Events::default());
    let replies = Arc::new(Replies::default());
    let runner = runner(
        Arc::new(Host::default()),
        Arc::new(Ingress::default()),
        replies.clone(),
        events.clone(),
    );
    let request = request(QueueDisposition::Interrupt, true);
    let scope = request.scope.clone();
    let thread = request.thread_id.clone();

    let result = runner.run(request.clone()).await.unwrap();

    assert_eq!(result.reply, "done");
    assert!(!runner.is_in_flight(&scope, &thread).await);
    assert_eq!(replies.0.lock().unwrap().len(), 1);
    assert_eq!(events.0.lock().unwrap().len(), 1);
    assert_eq!(
        replies.0.lock().unwrap()[0],
        deterministic_reply_id(&scope, &thread, &request.request_id)
    );
}

#[tokio::test]
async fn every_queued_mode_is_delegated_to_the_host_after_ingress() {
    let host = Arc::new(Host::default());
    let runner = runner(
        host.clone(),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    );
    for mode in [
        QueueDisposition::Steer,
        QueueDisposition::Followup,
        QueueDisposition::Collect,
    ] {
        assert!(matches!(
            runner.run(request(mode, false)).await,
            Err(TurnRunnerError::Queued)
        ));
    }
    assert_eq!(
        *host.queued.lock().unwrap(),
        [
            QueueDisposition::Steer,
            QueueDisposition::Followup,
            QueueDisposition::Collect,
        ]
    );
}
