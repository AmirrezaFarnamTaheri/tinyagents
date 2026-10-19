//! Red integration tests for physical admission around the real model wrapper.
//!
//! The channel gates below hold acquisition open until the test explicitly
//! releases it. This makes wait ordering and call-local refusal assertions
//! deterministic without clocks or sleeps.

use super::{
    ControlledChatModel, ControlledDispatchError, DispatchModelOptions, DispatchRefusal,
    DispatchRefusalReason, ModelBudget, PhysicalDispatchAdmission, PhysicalDispatchPermit,
};
use crate::observability::{
    PhysicalDispatchMetadata, PhysicalDispatchObserver, PhysicalDispatchOutcome,
    PhysicalDispatchTerminal,
};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tinyinference_llm::model::budget::{Budget, CallBudget, SpendLimits};
use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse, ModelStream};

type Action = Result<(), DispatchRefusalReason>;

struct PermitState {
    committed: AtomicUsize,
    active: AtomicUsize,
}

struct TestPermit(Arc<PermitState>);

impl PhysicalDispatchPermit for TestPermit {
    fn mark_dispatched(&self) -> Result<(), DispatchRefusal> {
        self.0.committed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for TestPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
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
        Ok(Arc::new(TestPermit(self.permits.clone())))
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
    EmptyStream,
    PendingStream,
}

struct TestModel {
    counts: Arc<Counts>,
    behavior: ModelBehavior,
}

#[async_trait]
impl ChatModel<()> for TestModel {
    async fn invoke(
        &self,
        _: &(),
        _: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.counts.provider.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            ModelBehavior::Fail => Err(tinyinference_llm::Error::Model("fixture failure".into())),
            _ => Ok(ModelResponse::assistant("ok")),
        }
    }

    async fn stream(
        &self,
        _: &(),
        _: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        self.counts.provider.fetch_add(1, Ordering::SeqCst);
        let stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = tinyinference_llm::model::ModelStreamItem> + Send>,
        > = match self.behavior {
            ModelBehavior::EmptyStream => Box::pin(futures::stream::empty()),
            ModelBehavior::PendingStream => Box::pin(futures::stream::pending()),
            _ => Box::pin(futures::stream::empty()),
        };
        Ok(ModelStream::new(stream))
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

fn model(counts: &Arc<Counts>, behavior: ModelBehavior) -> Arc<dyn ChatModel<()>> {
    Arc::new(TestModel {
        counts: counts.clone(),
        behavior,
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
    assert!(second
        .await
        .expect("second controlled task")
        .is_ok());
    assert_eq!(counts.provider.load(Ordering::SeqCst), 1);
    assert_eq!(counts.observer_start.load(Ordering::SeqCst), 1);
    assert_eq!(admission.permits.committed.load(Ordering::SeqCst), 1);
    assert_eq!(admission.acquired.load(Ordering::SeqCst), 2);
}
