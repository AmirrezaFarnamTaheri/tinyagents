//! Red integration tests for physical admission around the real model wrapper.
//!
//! The channel gates below hold acquisition open until the test explicitly
//! releases it. This makes wait ordering and call-local refusal assertions
//! deterministic without clocks or sleeps.

use super::{
    ControlledChatModel, ControlledDispatchError, DispatchModelOptions, DispatchRefusal,
    DispatchRefusalReason, ModelBudget, PhysicalDispatchAdmission, PhysicalDispatchPermit,
};
use crate::context::RunConfig;
use crate::error::TinyAgentsError;
use crate::observability::{
    PhysicalDispatchMetadata, PhysicalDispatchObserver, PhysicalDispatchOutcome,
    PhysicalDispatchTerminal,
};
use crate::runtime::AgentHarness;
use async_trait::async_trait;
use futures::{StreamExt, poll};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tinyinference_llm::model::budget::{Budget, CallBudget, SpendLimits};
use tinyinference_llm::model::{
    ChatModel, DeferredHandle, DeferredStatus, InputModality, InputSource, ModelProfile,
    ModelRequest, ModelResponse, ModelStream, ModelStreamItem, ResponseFormat,
};
use tinyinference_llm::usage::Usage;

type Action = Result<(), DispatchRefusalReason>;

struct PermitState {
    committed: AtomicUsize,
    active: AtomicUsize,
    checks: AtomicUsize,
    physical_attempts: AtomicUsize,
    physical_attempt_limit: AtomicUsize,
    refuse_commit: std::sync::atomic::AtomicBool,
    refuse_recheck: std::sync::atomic::AtomicBool,
}

struct TestPermit {
    state: Arc<PermitState>,
    committed: std::sync::atomic::AtomicBool,
}

impl PhysicalDispatchPermit for TestPermit {
    fn mark_dispatched(&self) -> Result<(), DispatchRefusal> {
        self.state.checks.fetch_add(1, Ordering::SeqCst);
        if self.state.refuse_commit.load(Ordering::SeqCst)
            || (self.committed.load(Ordering::SeqCst)
                && self.state.refuse_recheck.load(Ordering::SeqCst))
        {
            return Err(DispatchRefusal::new(DispatchRefusalReason::Deadline));
        }
        if self.committed.load(Ordering::SeqCst) {
            return Ok(());
        }
        if self
            .committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.state.committed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn mark_physical_attempt(&self) -> Result<(), DispatchRefusal> {
        let attempt = self.state.physical_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt >= self.state.physical_attempt_limit.load(Ordering::SeqCst) {
            return Err(DispatchRefusal::new(DispatchRefusalReason::CallLimit));
        }
        Ok(())
    }
}

impl Drop for TestPermit {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Gate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<Action>>>,
}

impl Gate {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Sender<Action>) {
        let (release, receiver) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                entered: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Mutex::new(Some(receiver)),
            }),
            release,
        )
    }

    async fn wait(&self) -> Action {
        self.entered.add_permits(1);
        self.release
            .lock()
            .await
            .take()
            .expect("gate receiver is consumed once")
            .await
            .expect("test releases every gated admission")
    }

    async fn wait_until_entered(&self) {
        self.entered
            .acquire()
            .await
            .expect("gate semaphore remains open")
            .forget();
    }
}

enum Step {
    Immediate(Action),
    Gated(Arc<Gate>),
}

struct ScriptedAdmission {
    steps: Mutex<VecDeque<Step>>,
    acquired: AtomicUsize,
    permits: Arc<PermitState>,
}

impl ScriptedAdmission {
    fn new(steps: impl IntoIterator<Item = Step>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            acquired: AtomicUsize::new(0),
            permits: Arc::new(PermitState {
                committed: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                checks: AtomicUsize::new(0),
                physical_attempts: AtomicUsize::new(0),
                physical_attempt_limit: AtomicUsize::new(usize::MAX),
                refuse_commit: std::sync::atomic::AtomicBool::new(false),
                refuse_recheck: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }
}

#[async_trait]
impl PhysicalDispatchAdmission for ScriptedAdmission {
    async fn acquire(&self) -> Result<Arc<dyn PhysicalDispatchPermit>, DispatchRefusal> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .expect("admission steps lock")
            .pop_front()
            .expect("one scripted step per invocation");
        let action = match step {
            Step::Immediate(action) => action,
            Step::Gated(gate) => gate.wait().await,
        };
        action.map_err(DispatchRefusal::new)?;
        self.permits.active.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(TestPermit {
            state: self.permits.clone(),
            committed: std::sync::atomic::AtomicBool::new(false),
        }))
    }
}

#[derive(Default)]
struct Counts {
    provider: AtomicUsize,
    observer_start: AtomicUsize,
    observer_terminal: AtomicUsize,
}

struct TestObserver(Arc<Counts>);
struct TestTerminal(Arc<Counts>);

impl PhysicalDispatchObserver for TestObserver {
    fn on_start(&self, _: &PhysicalDispatchMetadata) -> Box<dyn PhysicalDispatchTerminal> {
        self.0.observer_start.fetch_add(1, Ordering::SeqCst);
        Box::new(TestTerminal(self.0.clone()))
    }
}

impl PhysicalDispatchTerminal for TestTerminal {
    fn on_terminal(self: Box<Self>, _: PhysicalDispatchOutcome) {
        self.0.observer_terminal.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum ModelBehavior {
    Succeed,
    Fail,
    CompletedStream,
    EmptyStream,
    PendingStream,
    FailedStream,
}

struct TestModel {
    counts: Arc<Counts>,
    behavior: ModelBehavior,
    profile: ModelProfile,
}

struct FirstCallGateModel {
    counts: Arc<Counts>,
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl ChatModel<()> for FirstCallGateModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        let invocation = self.counts.provider.fetch_add(1, Ordering::SeqCst);
        if invocation == 0 {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("provider release semaphore remains open")
                .forget();
        }
        Ok(ModelResponse {
            usage: Some(Usage::new(2, 1)),
            raw: Some(serde_json::json!({"usage":{"buyer_cost_micro":3}})),
            ..ModelResponse::assistant("paid")
        })
    }
}

#[async_trait]
impl ChatModel<()> for TestModel {
    fn profile(&self) -> Option<&ModelProfile> {
        Some(&self.profile)
    }

    fn supports_input(&self, modality: InputModality, mime: &str, source: InputSource) -> bool {
        modality == InputModality::Image && mime == "image/png" && source == InputSource::Url
    }

    fn cache_identity(&self) -> Option<String> {
        Some("dispatch-test-model".to_owned())
    }

    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        self.counts.provider.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            ModelBehavior::Fail => Err(tinyinference_llm::Error::Model("fixture failure".into())),
            _ => Ok(ModelResponse::assistant("ok")),
        }
    }

    async fn stream(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelStream> {
        self.counts.provider.fetch_add(1, Ordering::SeqCst);
        let stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = tinyinference_llm::model::ModelStreamItem> + Send>,
        > = match self.behavior {
            ModelBehavior::CompletedStream => Box::pin(futures::stream::iter([
                ModelStreamItem::Completed(ModelResponse::assistant("ok")),
            ])),
            ModelBehavior::EmptyStream => Box::pin(futures::stream::empty()),
            ModelBehavior::PendingStream => Box::pin(futures::stream::pending()),
            ModelBehavior::FailedStream => {
                Box::pin(futures::stream::iter([ModelStreamItem::Failed(
                    "fixture stream error".to_owned(),
                )]))
            }
            ModelBehavior::Fail => {
                return Err(tinyinference_llm::Error::Model(
                    "fixture stream error".into(),
                ));
            }
            _ => Box::pin(futures::stream::empty()),
        };
        Ok(ModelStream::new(stream))
    }

    async fn fetch_deferred(
        &self,
        _: &DeferredHandle,
    ) -> tinyinference_llm::Result<DeferredStatus> {
        Ok(DeferredStatus::Pending)
    }
}

struct RetryingOpenAiModel {
    inner: Arc<tinyinference_llm::providers::openai::OpenAiModel>,
}

#[async_trait]
impl ChatModel<()> for RetryingOpenAiModel {
    fn profile(&self) -> Option<&ModelProfile> {
        <tinyinference_llm::providers::openai::OpenAiModel as ChatModel<()>>::profile(&self.inner)
    }

    fn supports_input(&self, modality: InputModality, mime: &str, source: InputSource) -> bool {
        <tinyinference_llm::providers::openai::OpenAiModel as ChatModel<()>>::supports_input(
            &self.inner,
            modality,
            mime,
            source,
        )
    }

    fn cache_identity(&self) -> Option<String> {
        <tinyinference_llm::providers::openai::OpenAiModel as ChatModel<()>>::cache_identity(
            &self.inner,
        )
    }

    async fn invoke(
        &self,
        state: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.inner.invoke(state, request).await
    }

    async fn stream(
        &self,
        state: &(),
        mut request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        // OpenAI's stream call sends eagerly. Force its real one-shot
        // response-format degradation retry so the test can revoke admission
        // after the first physical send and before the next one.
        request.response_format = Some(ResponseFormat::JsonObject);
        self.inner.stream(state, request).await
    }
}

fn options(
    admission: Option<Arc<dyn PhysicalDispatchAdmission>>,
    budget: Option<ModelBudget>,
    counts: &Arc<Counts>,
) -> DispatchModelOptions {
    DispatchModelOptions::new()
        .admission(admission)
        .monetary_budget(budget)
        .observer(Some(Arc::new(TestObserver(counts.clone()))))
}

fn text_request() -> ModelRequest {
    ModelRequest::new(vec![tinyinference_llm::message::Message::User(
        tinyinference_llm::message::UserMessage {
            content: vec![tinyinference_llm::message::ContentBlock::Text(
                "fixture prompt".to_owned(),
            )],
        },
    )])
}

fn model(counts: &Arc<Counts>, behavior: ModelBehavior) -> Arc<dyn ChatModel<()>> {
    let profile = ModelProfile {
        model: Some("dispatch-test-model".to_owned()),
        ..ModelProfile::default()
    };
    Arc::new(TestModel {
        counts: counts.clone(),
        behavior,
        profile,
    })
}

#[tokio::test]
async fn admission_refuses_before_provider_or_physical_observer_start() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Err(DispatchRefusalReason::Deadline))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );

    let error = controlled
        .invoke_controlled(&(), ModelRequest::default())
        .await
        .expect_err("deadline refusal is typed");
    assert!(matches!(
        error,
        ControlledDispatchError::Admission(refusal)
            if refusal.reason() == DispatchRefusalReason::Deadline
    ));
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn chat_model_trait_consumer_observes_admission_refusal() {
    let counts = Arc::new(Counts::default());
    let admission =
        ScriptedAdmission::new([Step::Immediate(Err(DispatchRefusalReason::CallLimit))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );

    let result = ChatModel::invoke(&controlled, &(), ModelRequest::default()).await;

    assert!(matches!(
        result,
        Err(tinyinference_llm::Error::Validation(_))
    ));
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
}

#[test]
fn controlled_chat_model_preserves_inner_attempt_tracking_capability() {
    let counts = Arc::new(Counts::default());
    let model: Arc<dyn ChatModel<()>> = Arc::new(
        tinyinference_llm::providers::openai::OpenAiModel::new("fixture"),
    );
    let controlled = ControlledChatModel::new(model, options(None, None, &counts));

    assert!(ChatModel::tracks_physical_attempts(&controlled));
}

#[tokio::test]
async fn agent_turn_preserves_typed_admission_refusal_from_chat_model() {
    let counts = Arc::new(Counts::default());
    let admission =
        ScriptedAdmission::new([Step::Immediate(Err(DispatchRefusalReason::CallLimit))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("controlled", Arc::new(controlled));

    let error = harness
        .invoke(
            &(),
            (),
            RunConfig::new("dispatch-turn"),
            vec![tinyinference_llm::message::Message::user("hello")],
        )
        .await
        .expect_err("host admission refusal reaches the turn caller");

    assert!(matches!(
        error,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::CallLimit
    ));
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_agent_turns_keep_typed_admission_refusals_call_local() {
    let counts = Arc::new(Counts::default());
    let (first_gate, first_release) = Gate::new();
    let (second_gate, second_release) = Gate::new();
    let admission = ScriptedAdmission::new([
        Step::Gated(first_gate.clone()),
        Step::Gated(second_gate.clone()),
    ]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("controlled", Arc::new(controlled));
    let harness = Arc::new(harness);
    let invoke = |harness: Arc<AgentHarness<()>>| async move {
        harness
            .invoke(
                &(),
                (),
                RunConfig::new("concurrent-dispatch-turn"),
                vec![tinyinference_llm::message::Message::user("hello")],
            )
            .await
    };
    let mut first = tokio::spawn(invoke(harness.clone()));
    tokio::select! {
        _ = first_gate.wait_until_entered() => {}
        result = &mut first => panic!("first turn skipped admission: {result:?}"),
    }
    let mut second = tokio::spawn(invoke(harness));
    tokio::select! {
        _ = second_gate.wait_until_entered() => {}
        result = &mut second => panic!("second turn skipped admission: {result:?}"),
    }

    first_release
        .send(Err(DispatchRefusalReason::CallLimit))
        .expect("first turn waits at its own gate");
    second_release
        .send(Err(DispatchRefusalReason::Cancelled))
        .expect("second turn waits at its own gate");

    let first_error = first
        .await
        .expect("first turn task")
        .expect_err("first refused");
    let second_error = second
        .await
        .expect("second turn task")
        .expect_err("second refused");
    assert!(matches!(
        first_error,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::CallLimit
    ));
    assert!(matches!(
        second_error,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::Cancelled
    ));
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admitted_agent_turn_still_completes_normally() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("controlled", Arc::new(controlled));

    harness
        .invoke(
            &(),
            (),
            RunConfig::new("normal-dispatch-turn"),
            vec![tinyinference_llm::message::Message::user("hello")],
        )
        .await
        .expect("admitted turn succeeds");

    assert_eq!(counts.provider.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn streaming_agent_turn_preserves_typed_admission_refusal() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Err(DispatchRefusalReason::Deadline))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("controlled", Arc::new(controlled));

    let error = harness
        .invoke_streaming(
            &(),
            (),
            RunConfig::new("streaming-dispatch-turn"),
            vec![tinyinference_llm::message::Message::user("hello")],
        )
        .await
        .expect_err("stream admission refusal reaches the turn caller");

    assert!(matches!(
        error,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::Deadline
    ));
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn openai_retry_rechecks_committed_admission_without_money_budget() {
    assert_openai_retry_rechecks_committed_admission(false).await;
}

#[tokio::test]
async fn openai_retry_rechecks_committed_admission_with_money_budget() {
    assert_openai_retry_rechecks_committed_admission(true).await;
}

async fn assert_openai_retry_rechecks_committed_admission(with_money_budget: bool) {
    use tinyinference_llm::providers::openai::OpenAiModel;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local fake provider"),
    );
    let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let provider = Arc::new(RetryingOpenAiModel {
        inner: Arc::new(OpenAiModel::new("test-key").with_base_url(endpoint)),
    });
    let money_budget = with_money_budget.then(|| {
        ModelBudget::new(
            Budget::new(SpendLimits::default()),
            CallBudget {
                input_tokens: 1_000,
                output_tokens: 32,
                cost_micros: 1,
            },
        )
    });
    let controlled = ControlledChatModel::new(
        provider,
        options(Some(admission.clone()), money_budget, &counts),
    );
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("controlled", Arc::new(controlled));
    let server_permit_state = admission.permits.clone();
    let server_listener = listener.clone();
    let (first_response_sent, first_response_received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = server_listener
            .accept()
            .await
            .expect("first OpenAI request");
        let mut request = vec![0; 8192];
        let _ = socket.read(&mut request).await.expect("read first request");
        server_permit_state
            .refuse_recheck
            .store(true, Ordering::SeqCst);
        let body = br#"{"error":{"message":"response_format json_object unsupported"}}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("write retryable response headers");
        socket
            .write_all(body)
            .await
            .expect("write retryable response body");
        first_response_sent
            .send(())
            .expect("test waits for the first response before observing retry");
    });
    let mut turn = tokio::spawn(async move {
        harness
            .invoke_streaming(
                &(),
                (),
                RunConfig::new("openai-retry-dispatch"),
                vec![tinyinference_llm::message::Message::user("hello")],
            )
            .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), first_response_received)
        .await
        .expect("first HTTP attempt returns a retryable response before timeout")
        .expect("first HTTP server signals its response");

    tokio::select! {
        result = &mut turn => {
            let error = result.expect("turn task").expect_err("late deadline refusal");
            assert!(matches!(
                error,
                TinyAgentsError::PhysicalDispatchAdmission(refusal)
                    if refusal.reason() == DispatchRefusalReason::Deadline
            ));
        }
        Ok(connection) = tokio::time::timeout(std::time::Duration::from_millis(250), listener.accept()) => {
            turn.abort();
            panic!("local provider received a retry connection after late deadline refusal (permit checks {}): {connection:?}", admission.permits.checks.load(Ordering::SeqCst));
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
            turn.abort();
            panic!("turn hung instead of returning the typed late admission refusal");
        }
    }
    server.await.expect("local server task");
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
}

struct LazyTypedRefusalModel {
    reason: DispatchRefusalReason,
    barrier: Option<Arc<tokio::sync::Barrier>>,
}

#[async_trait]
impl ChatModel<()> for LazyTypedRefusalModel {
    async fn invoke(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        unreachable!("typed refusal fixture is exercised through its stream")
    }

    async fn stream(&self, _: &(), _: ModelRequest) -> tinyinference_llm::Result<ModelStream> {
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        let reason = self.reason;
        let mut recorded = false;
        let stream = futures::stream::poll_fn(move |_| {
            if !recorded {
                recorded = true;
                super::dispatch_admission::record_invocation_refusal(DispatchRefusal::new(reason));
                Poll::Ready(Some(ModelStreamItem::Failed(
                    "lazy test stream terminated".to_owned(),
                )))
            } else {
                Poll::Ready(None)
            }
        });
        Ok(ModelStream::new(Box::pin(stream)))
    }
}

#[tokio::test]
async fn turn_keeps_typed_dispatch_outcome_scope_through_lazy_stream_poll() {
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model(
        "controlled",
        Arc::new(LazyTypedRefusalModel {
            reason: DispatchRefusalReason::CallLimit,
            barrier: None,
        }),
    );

    let error = harness
        .invoke_streaming(
            &(),
            (),
            RunConfig::new("lazy-typed-refusal-scope"),
            vec![tinyinference_llm::message::Message::user("hello")],
        )
        .await
        .expect_err("poll-time typed refusal survives the stream outcome");

    assert!(matches!(
        error,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::CallLimit
    ));
}

#[tokio::test]
async fn concurrent_lazy_turns_keep_distinct_typed_refusals_call_local() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut first_harness: AgentHarness<()> = AgentHarness::new();
    first_harness.register_model(
        "controlled",
        Arc::new(LazyTypedRefusalModel {
            reason: DispatchRefusalReason::CallLimit,
            barrier: Some(barrier.clone()),
        }),
    );
    let mut second_harness: AgentHarness<()> = AgentHarness::new();
    second_harness.register_model(
        "controlled",
        Arc::new(LazyTypedRefusalModel {
            reason: DispatchRefusalReason::Cancelled,
            barrier: Some(barrier),
        }),
    );

    let first = tokio::spawn(async move {
        first_harness
            .invoke_streaming(
                &(),
                (),
                RunConfig::new("lazy-call-limit-refusal"),
                vec![tinyinference_llm::message::Message::user("first")],
            )
            .await
    });
    let second = tokio::spawn(async move {
        second_harness
            .invoke_streaming(
                &(),
                (),
                RunConfig::new("lazy-cancel-refusal"),
                vec![tinyinference_llm::message::Message::user("second")],
            )
            .await
    });

    let first = first
        .await
        .expect("first turn task")
        .expect_err("first refusal reaches its own turn");
    let second = second
        .await
        .expect("second turn task")
        .expect_err("second refusal reaches its own turn");
    assert!(matches!(
        first,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::CallLimit
    ));
    assert!(matches!(
        second,
        TinyAgentsError::PhysicalDispatchAdmission(refusal)
            if refusal.reason() == DispatchRefusalReason::Cancelled
    ));
}

#[tokio::test]
async fn money_refusal_does_not_commit_admission() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(0),
            cost_micros: Some(0),
        }),
        CallBudget {
            input_tokens: 100,
            output_tokens: 1,
            cost_micros: 1,
        },
    );
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), Some(budget.clone()), &counts),
    );

    let error = controlled
        .invoke_controlled(&(), ModelRequest::default())
        .await
        .expect_err("monetary admission refuses before dispatch");
    assert!(matches!(error, ControlledDispatchError::Model(_)));
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(budget.ledger.snapshot().spent.cost_micros, 0);
    assert_eq!(budget.ledger.snapshot().reserved.cost_micros, 0);
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn late_refusal_after_money_wait_refunds_the_uncommitted_call() {
    let counts = Arc::new(Counts::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(())), Step::Immediate(Ok(()))]);
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(2_000),
            cost_micros: Some(15),
        })
        .wait_for_capacity(),
        CallBudget {
            input_tokens: 1_000,
            output_tokens: 5,
            cost_micros: 10,
        },
    );
    let controlled = ControlledChatModel::new(
        Arc::new(FirstCallGateModel {
            counts: counts.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }),
        options(Some(admission.clone()), Some(budget.clone()), &counts),
    );

    let mut first = Box::pin(controlled.invoke_controlled(&(), ModelRequest::default()));
    assert!(poll!(first.as_mut()).is_pending());
    entered
        .acquire()
        .await
        .expect("first provider call entered")
        .forget();
    let mut second = Box::pin(controlled.invoke_controlled(&(), ModelRequest::default()));
    assert!(
        poll!(second.as_mut()).is_pending(),
        "second waits for money"
    );
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 2);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(budget.ledger.snapshot().reserved.cost_micros, 10);

    admission
        .permits
        .refuse_commit
        .store(true, Ordering::SeqCst);
    release.add_permits(1);
    assert!(matches!(poll!(first.as_mut()), Poll::Ready(Ok(_))));
    assert!(matches!(
        second.await,
        Err(ControlledDispatchError::Admission(refusal))
            if refusal.reason() == DispatchRefusalReason::Deadline
    ));

    assert_eq!(counts.provider.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(budget.ledger.snapshot().spent.cost_micros, 3);
    assert_eq!(
        budget.ledger.snapshot().reserved,
        tinyinference_llm::model::budget::Spend::default()
    );
}

#[tokio::test]
async fn cancel_while_waiting_does_not_commit_admission() {
    let counts = Arc::new(Counts::default());
    let (gate, release) = Gate::new();
    let admission = ScriptedAdmission::new([Step::Gated(gate.clone())]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    );
    let mut call = tokio::spawn(async move {
        controlled
            .invoke_controlled(&(), ModelRequest::default())
            .await
    });
    tokio::select! {
        _ = gate.wait_until_entered() => {}
        result = &mut call => panic!("call completed without entering admission: {result:?}"),
    }
    release
        .send(Err(DispatchRefusalReason::Cancelled))
        .expect("call is waiting at admission");

    assert!(matches!(
        call.await.expect("controlled call task"),
        Err(ControlledDispatchError::Admission(refusal))
            if refusal.reason() == DispatchRefusalReason::Cancelled
    ));
    assert_eq!(counts.provider.load(Ordering::SeqCst), 0);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_invocation_commits_once_and_observes_only_after_commit() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Fail),
        options(Some(admission.clone()), None, &counts),
    );

    assert!(matches!(
        controlled
            .invoke_controlled(&(), ModelRequest::default())
            .await,
        Err(ControlledDispatchError::Model(_))
    ));
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(counts.provider.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_terminal.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_eof_releases_admission_and_is_fused() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::EmptyStream),
        options(Some(admission.clone()), None, &counts),
    );
    let mut stream = controlled
        .stream_controlled(&(), ModelRequest::default())
        .await
        .expect("stream dispatch");

    assert!(stream.next().await.is_none());
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert!(stream.next().await.is_none(), "terminal EOF stays fused");
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_stream_releases_admission() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::PendingStream),
        options(Some(admission.clone()), None, &counts),
    );
    let stream = controlled
        .stream_controlled(&(), ModelRequest::default())
        .await
        .expect("stream dispatch");
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 1);
    drop(stream);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_failure_releases_admission() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::FailedStream),
        options(Some(admission.clone()), None, &counts),
    );
    let mut stream = controlled
        .stream_controlled(&(), ModelRequest::default())
        .await
        .expect("stream dispatch");

    assert!(matches!(
        stream.next().await,
        Some(ModelStreamItem::Failed(_))
    ));
    assert!(stream.next().await.is_none());
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn monetarily_budgeted_stream_releases_admission_at_terminal_item() {
    for behavior in [
        ModelBehavior::CompletedStream,
        ModelBehavior::FailedStream,
        ModelBehavior::EmptyStream,
    ] {
        let counts = Arc::new(Counts::default());
        let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
        let budget = ModelBudget::new(Budget::new(SpendLimits::default()), CallBudget {
            input_tokens: 10_000,
            output_tokens: 10,
            cost_micros: 100,
        });
        let controlled = ControlledChatModel::new(
            model(&counts, behavior),
            options(Some(admission.clone()), Some(budget), &counts),
        );
        let mut stream = controlled
            .stream_controlled(&(), text_request())
            .await
            .expect("budgeted stream is admitted");

        assert_eq!(admission.permits.active.load(Ordering::SeqCst), 1);
        let terminal = stream.next().await;
        match behavior {
            ModelBehavior::CompletedStream => {
                assert!(matches!(terminal, Some(ModelStreamItem::Completed(_))));
            }
            ModelBehavior::FailedStream => {
                assert!(matches!(terminal, Some(ModelStreamItem::Failed(_))));
            }
            ModelBehavior::EmptyStream => assert!(terminal.is_none()),
            _ => unreachable!("only terminal stream behaviors are tested"),
        }
        assert_eq!(
            admission.permits.active.load(Ordering::SeqCst),
            0,
            "the host permit is released at terminal observation before another poll or drop"
        );
    }
}

#[tokio::test]
async fn monetarily_budgeted_pending_stream_keeps_admission_until_drop() {
    let counts = Arc::new(Counts::default());
    let admission = ScriptedAdmission::new([Step::Immediate(Ok(()))]);
    let budget = ModelBudget::new(Budget::new(SpendLimits::default()), CallBudget {
        input_tokens: 10_000,
        output_tokens: 10,
        cost_micros: 100,
    });
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::PendingStream),
        options(Some(admission.clone()), Some(budget), &counts),
    );
    let stream = controlled
        .stream_controlled(&(), text_request())
        .await
        .expect("budgeted stream is admitted");

    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 1);
    drop(stream);
    assert_eq!(admission.permits.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn controlled_chat_model_delegates_capabilities_and_deferred_polling() {
    let counts = Arc::new(Counts::default());
    let controlled = ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(None, None, &counts),
    );

    assert_eq!(
        controlled
            .profile()
            .and_then(|profile| profile.model.as_deref()),
        Some("dispatch-test-model")
    );
    assert!(controlled.supports_input(InputModality::Image, "image/png", InputSource::Url));
    assert_eq!(
        controlled.cache_identity().as_deref(),
        Some("dispatch-test-model")
    );
    assert!(matches!(
        controlled
            .fetch_deferred(&DeferredHandle::new("fixture", "deferred-1"))
            .await
            .expect("deferred status"),
        DeferredStatus::Pending
    ));
}

#[tokio::test]
async fn concurrent_refusals_remain_call_local() {
    let counts = Arc::new(Counts::default());
    let (first_gate, first_release) = Gate::new();
    let (second_gate, second_release) = Gate::new();
    let admission = ScriptedAdmission::new([
        Step::Gated(first_gate.clone()),
        Step::Gated(second_gate.clone()),
    ]);
    let controlled = Arc::new(ControlledChatModel::new(
        model(&counts, ModelBehavior::Succeed),
        options(Some(admission.clone()), None, &counts),
    ));
    let mut first = {
        let controlled = controlled.clone();
        tokio::spawn(async move {
            controlled
                .invoke_controlled(&(), ModelRequest::default())
                .await
        })
    };
    tokio::select! {
        _ = first_gate.wait_until_entered() => {}
        result = &mut first => panic!("first call skipped admission: {result:?}"),
    }
    let mut second = {
        let controlled = controlled.clone();
        tokio::spawn(async move {
            controlled
                .invoke_controlled(&(), ModelRequest::default())
                .await
        })
    };
    tokio::select! {
        _ = second_gate.wait_until_entered() => {}
        result = &mut second => panic!("second call skipped admission: {result:?}"),
    }

    first_release
        .send(Err(DispatchRefusalReason::CallLimit))
        .expect("first call waits at its own gate");
    second_release
        .send(Ok(()))
        .expect("second call waits at its own gate");

    assert!(matches!(
        first.await.expect("first controlled task"),
        Err(ControlledDispatchError::Admission(refusal))
            if refusal.reason() == DispatchRefusalReason::CallLimit
    ));
    assert!(second.await.expect("second controlled task").is_ok());
    assert_eq!(counts.provider.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 2);
}
