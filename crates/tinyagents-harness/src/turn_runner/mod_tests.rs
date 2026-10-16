use super::*;
use crate::run_queue::{QueueLane, QueueMode};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Host {
    started: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
    fingerprint_error: bool,
    checkins: std::sync::atomic::AtomicUsize,
    runs: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl TurnHost for Host {
    async fn checkout(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: request.thread_id.clone(),
            fingerprint: Some("stable".into()),
        })
    }

    async fn checkout_fork(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: format!("fork:{}", request.thread_id),
            fingerprint: Some("fork".into()),
        })
    }

    async fn validate_fingerprint(
        &self,
        _request: &TurnRequest,
        _session: &TurnSession,
    ) -> Result<(), String> {
        if self.fingerprint_error {
            Err("fingerprint mismatch".into())
        } else {
            Ok(())
        }
    }

    fn run_origin(&self, request: &TurnRequest) -> serde_json::Value {
        serde_json::json!({"request": request.request_id})
    }

    async fn run(
        &self,
        _request: &TurnRequest,
        _session: &TurnSession,
        _origin: &serde_json::Value,
        cancellation: crate::CancellationToken,
        _progress: tokio::sync::mpsc::Sender<TurnSignal>,
    ) -> Result<TurnResult, String> {
        self.runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let (Some(started), Some(release)) = (&self.started, &self.release) {
            started.notify_one();
            tokio::select! {
                _ = release.notified() => {},
                _ = cancellation.cancelled() => return Err("cancelled".into()),
            }
        }
        Ok(TurnResult {
            reply: "done".into(),
            data: serde_json::Value::Null,
        })
    }

    async fn checkin(&self, _request: &TurnRequest) {
        self.checkins
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    async fn cancel_children(&self, _request: &TurnRequest) {}
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
        payload: serde_json::json!({"attachment": "file-token"}),
        host_authored,
        disposition,
        deadline: None,
        idle_timeout: Some(std::time::Duration::from_secs(5)),
    }
}

#[tokio::test]
async fn ingress_stages_run_in_order_before_queue_acceptance() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let host = Arc::new(Host {
        started: Some(started.clone()),
        release: Some(release.clone()),
        ..Host::default()
    });
    let ingress = Arc::new(Ingress::default());
    let runner = Arc::new(runner(
        host.clone(),
        ingress.clone(),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    ));

    let primary = {
        let runner = runner.clone();
        tokio::spawn(async move {
            runner
                .run(request(QueueDisposition::Interrupt, false))
                .await
        })
    };
    started.notified().await;
    for (mode, id) in [
        (QueueDisposition::Steer, "steered"),
        (QueueDisposition::Followup, "followup"),
        (QueueDisposition::Collect, "collect"),
    ] {
        let mut incoming = request(mode, false);
        incoming.request_id = id.into();
        assert!(matches!(
            runner.run(incoming).await,
            Err(TurnRunnerError::Queued)
        ));
    }
    let scope = request(QueueDisposition::Interrupt, false).scope;
    let status = runner.queue_status(&scope, "thread-a").await;
    assert_eq!(
        (status.steers, status.followups, status.collects),
        (1, 1, 1)
    );
    assert_eq!(
        runner
            .take_queued(&scope, "thread-a", QueueLane::Steer, QueueMode::OneAtATime)
            .await[0]
            .request_id,
        "steered"
    );
    assert_eq!(
        runner
            .take_queued(&scope, "thread-a", QueueLane::Followup, QueueMode::All)
            .await[0]
            .request_id,
        "followup"
    );
    assert_eq!(
        runner
            .take_queued(&scope, "thread-a", QueueLane::Collect, QueueMode::All)
            .await[0]
            .request_id,
        "collect"
    );
    assert_eq!(runner.clear_queue(&scope, "thread-a").await, 0);
    release.notify_one();
    primary.await.unwrap().unwrap();
    let ingress = ingress.0.lock().unwrap();
    assert_eq!(ingress.len(), 4 * 4);
    assert_eq!(
        ingress[12..],
        [
            IngressStage::StageAttachments,
            IngressStage::Guardrail,
            IngressStage::ApprovalInterception,
            IngressStage::BeforeSubmit,
        ]
    );
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
async fn host_authored_turn_never_enters_inflight_while_running() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let runner = Arc::new(runner(
        Arc::new(Host {
            started: Some(started.clone()),
            release: Some(release.clone()),
            ..Host::default()
        }),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    ));
    let request = request(QueueDisposition::Interrupt, true);
    let scope = request.scope.clone();
    let thread = request.thread_id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(request).await })
    };

    started.notified().await;
    assert!(!runner.is_in_flight(&scope, &thread).await);
    release.notify_one();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn stale_or_cross_agent_cancel_cannot_cancel_the_current_turn() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let runner = Arc::new(runner(
        Arc::new(Host {
            started: Some(started.clone()),
            release: Some(release.clone()),
            ..Host::default()
        }),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    ));
    let mut request = request(QueueDisposition::Interrupt, false);
    request.request_id = "current-request".into();
    let scope = request.scope.clone();
    let thread = request.thread_id.clone();
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(request).await })
    };

    started.notified().await;
    assert!(!runner.cancel(&scope, &thread, "stale-request").await);
    let another_agent = ScopeKey {
        agent_id: "agent-b".into(),
        profile_id: None,
    };
    assert!(
        !runner
            .cancel(&another_agent, &thread, "current-request")
            .await
    );
    assert!(runner.cancel(&scope, &thread, "current-request").await);
    assert!(matches!(
        run.await.unwrap(),
        Err(TurnRunnerError::Cancelled)
    ));
}

#[tokio::test]
async fn checked_out_session_is_checked_in_when_fingerprint_validation_fails() {
    let host = Arc::new(Host {
        fingerprint_error: true,
        ..Host::default()
    });
    let runner = runner(
        host.clone(),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
    );

    assert!(matches!(
        runner
            .run(request(QueueDisposition::Interrupt, false))
            .await,
        Err(TurnRunnerError::Failed(_))
    ));
    assert_eq!(host.checkins.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn two_agents_can_run_the_same_thread_id_concurrently() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let host = Arc::new(ConcurrentHost {
        barrier: barrier.clone(),
        sessions: Mutex::default(),
    });
    let runner = Arc::new(TurnRunner::new(
        host.clone(),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
        Arc::new(Failures),
        Arc::new(Summary),
        TurnRunnerConfig::default(),
    ));
    let mut first = request(QueueDisposition::Interrupt, false);
    first.thread_id = "shared-name".into();
    let mut second = first.clone();
    second.scope.agent_id = "agent-b".into();
    second.request_id = "request-b".into();
    let first = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(first).await })
    };
    let second = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(second).await })
    };

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    })
    .await
    .expect("agent-scoped sessions must not share a lock");
    assert_eq!(host.sessions.lock().unwrap().len(), 2);
}

struct ConcurrentHost {
    barrier: Arc<tokio::sync::Barrier>,
    sessions: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl TurnHost for ConcurrentHost {
    async fn checkout(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: request.request_id.clone(),
            fingerprint: None,
        })
    }
    async fn checkout_fork(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: format!("fork:{}", request.request_id),
            fingerprint: None,
        })
    }
    fn run_origin(&self, _request: &TurnRequest) -> serde_json::Value {
        serde_json::Value::Null
    }
    async fn run(
        &self,
        request: &TurnRequest,
        session: &TurnSession,
        _origin: &serde_json::Value,
        _cancellation: crate::CancellationToken,
        _progress: tokio::sync::mpsc::Sender<TurnSignal>,
    ) -> Result<TurnResult, String> {
        let expected_session = if request.disposition == QueueDisposition::Parallel {
            format!("fork:{}", request.request_id)
        } else {
            request.request_id.clone()
        };
        assert_eq!(session.session_id, expected_session);
        self.sessions
            .lock()
            .unwrap()
            .push(session.session_id.clone());
        self.barrier.wait().await;
        Ok(TurnResult {
            reply: "ok".into(),
            data: serde_json::Value::Null,
        })
    }
    async fn checkin(&self, _request: &TurnRequest) {}
    async fn cancel_children(&self, _request: &TurnRequest) {}
}

#[tokio::test]
async fn parallel_turns_use_independent_fork_sessions_and_run_concurrently() {
    let host = Arc::new(ConcurrentHost {
        barrier: Arc::new(tokio::sync::Barrier::new(2)),
        sessions: Mutex::default(),
    });
    let runner = Arc::new(TurnRunner::new(
        host.clone(),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
        Arc::new(Failures),
        Arc::new(Summary),
        TurnRunnerConfig::default(),
    ));
    let mut first = request(QueueDisposition::Parallel, false);
    first.request_id = "parallel-a".into();
    let mut second = first.clone();
    second.request_id = "parallel-b".into();
    let first = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(first).await })
    };
    let second = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(second).await })
    };

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    })
    .await
    .expect("parallel branches must not serialize");
    let sessions = host.sessions.lock().unwrap();
    assert_eq!(sessions.as_slice(), ["parallel-a", "parallel-b"]);
}

struct QuietWorkHost {
    started: Arc<tokio::sync::Notify>,
    finish: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl TurnHost for QuietWorkHost {
    async fn checkout(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        Ok(TurnSession {
            session_id: request.thread_id.clone(),
            fingerprint: None,
        })
    }
    async fn checkout_fork(&self, request: &TurnRequest) -> Result<TurnSession, String> {
        self.checkout(request).await
    }
    fn run_origin(&self, _request: &TurnRequest) -> serde_json::Value {
        serde_json::Value::Null
    }
    async fn run(
        &self,
        _request: &TurnRequest,
        _session: &TurnSession,
        _origin: &serde_json::Value,
        _cancellation: crate::CancellationToken,
        progress: tokio::sync::mpsc::Sender<TurnSignal>,
    ) -> Result<TurnResult, String> {
        progress.send(TurnSignal::WorkStarted).await.unwrap();
        self.started.notify_one();
        self.finish.notified().await;
        progress.send(TurnSignal::WorkFinished).await.unwrap();
        std::future::pending().await
    }
    async fn checkin(&self, _request: &TurnRequest) {}
    async fn cancel_children(&self, _request: &TurnRequest) {}
}

#[tokio::test(start_paused = true)]
async fn idle_guard_restarts_when_quiet_work_finishes() {
    let started = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let runner = Arc::new(TurnRunner::new(
        Arc::new(QuietWorkHost {
            started: started.clone(),
            finish: finish.clone(),
        }),
        Arc::new(Ingress::default()),
        Arc::new(Replies::default()),
        Arc::new(Events::default()),
        Arc::new(Failures),
        Arc::new(Summary),
        TurnRunnerConfig {
            idle_timeout: std::time::Duration::from_secs(5),
            deadline: None,
            close_timeout: std::time::Duration::from_secs(1),
        },
    ));
    let mut turn = request(QueueDisposition::Interrupt, false);
    turn.idle_timeout = Some(std::time::Duration::from_secs(5));
    let run = {
        let runner = runner.clone();
        tokio::spawn(async move { runner.run(turn).await })
    };
    started.notified().await;
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    finish.notify_one();
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(6)).await;

    assert!(
        matches!(run.await.unwrap(), Err(TurnRunnerError::Failed(message)) if message.contains("idle timeout"))
    );
}
