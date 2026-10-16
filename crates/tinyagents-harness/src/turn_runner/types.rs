use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;

use crate::progress::AgentProgress;
use crate::run_queue::{QueueLane, QueueMode, QueueStatus, RunQueue};

/// Identity that isolates session locks and active turns between agents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeKey {
    pub agent_id: String,
    pub profile_id: Option<String>,
}

/// A request after the host has supplied its channel-independent metadata.
#[derive(Debug, Clone)]
pub struct TurnRequest<P = serde_json::Value> {
    pub scope: ScopeKey,
    pub thread_id: String,
    pub request_id: String,
    pub text: String,
    /// Lossless host-defined input metadata such as attachments, access flags,
    /// locale, or channel attribution.
    pub payload: P,
    pub host_authored: bool,
    pub disposition: QueueDisposition,
    pub deadline: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

/// Host-owned identity and fingerprint for the checked-out durable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSession {
    pub session_id: String,
    pub fingerprint: Option<String>,
}

/// Pipeline stages are called in this order for every submitted request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressStage {
    StageAttachments,
    Guardrail,
    ApprovalInterception,
    BeforeSubmit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressDecision {
    Continue,
    /// The policy consumed the request, for example by routing an approval reply.
    Consumed,
    Reject(String),
}

#[async_trait]
pub trait IngressPolicy<P = serde_json::Value>: Send + Sync {
    async fn apply(
        &self,
        stage: IngressStage,
        request: &mut TurnRequest<P>,
    ) -> Result<IngressDecision, String>;
}

/// The five acceptance behaviors exposed by channel turn APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueDisposition {
    Interrupt,
    Steer,
    Followup,
    Collect,
    Parallel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalEvent {
    Completed {
        request_id: String,
        reply_id: String,
    },
    Failed {
        request_id: String,
        message: String,
        class: String,
    },
    Cancelled {
        request_id: String,
    },
}

/// Activity updates sent by the host so the idle guard distinguishes silence
/// from a model or tool call that is still running.
#[derive(Debug, Clone)]
pub enum TurnSignal {
    Progress(Box<AgentProgress>),
    WorkStarted,
    WorkFinished,
}

#[async_trait]
pub trait TurnHost<P = serde_json::Value>: Send + Sync {
    /// Check out or create the session for this request and verify its fingerprint.
    async fn checkout(&self, request: &TurnRequest<P>) -> Result<TurnSession, String>;
    /// Check out an independent transcript/session snapshot for Parallel mode.
    async fn checkout_fork(&self, request: &TurnRequest<P>) -> Result<TurnSession, String>;
    /// Confirm the checked-out session still matches the request's expected
    /// transcript/session identity before any model work begins.
    async fn validate_fingerprint(
        &self,
        _request: &TurnRequest<P>,
        _session: &TurnSession,
    ) -> Result<(), String> {
        Ok(())
    }
    /// Build the host's origin value (channel, actor and request attribution).
    fn run_origin(&self, request: &TurnRequest<P>) -> serde_json::Value;
    /// Run the actual harness/session turn. Progress is forwarded to the sink.
    async fn run(
        &self,
        request: &TurnRequest<P>,
        session: &TurnSession,
        origin: &serde_json::Value,
        cancellation: crate::CancellationToken,
        queue: Arc<RunQueue<TurnRequest<P>>>,
        progress: mpsc::Sender<TurnSignal>,
    ) -> Result<TurnResult, String>;
    /// Release the session after the run, including on cancellation or failure.
    async fn checkin(&self, request: &TurnRequest<P>);
    /// Cancel child runs belonging to this parent request.
    async fn cancel_children(&self, request: &TurnRequest<P>);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnResult {
    pub reply: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[async_trait]
pub trait ReplyStore<P = serde_json::Value>: Send + Sync {
    async fn persist(
        &self,
        request: &TurnRequest<P>,
        reply_id: &str,
        result: &TurnResult,
    ) -> Result<(), String>;
}

#[async_trait]
pub trait EventSink<P = serde_json::Value>: Send + Sync
where
    P: Send + Sync,
{
    async fn progress(&self, request: &TurnRequest<P>, event: AgentProgress);
    async fn terminal(&self, request: &TurnRequest<P>, event: TerminalEvent);
    async fn suggestions(&self, _request: &TurnRequest<P>, _suggestions: Vec<String>) {}
    async fn collected(&self, _request: &TurnRequest<P>, _items: Vec<TurnRequest<P>>) {}
}

pub trait FailureClassifier: Send + Sync {
    fn classify(&self, error: &str) -> (String, String);
}

#[async_trait]
pub trait Summarizer<P = serde_json::Value>: Send + Sync {
    /// Optional next-message suggestions for a successfully completed turn.
    async fn suggestions(&self, _request: &TurnRequest<P>, _result: &TurnResult) -> Vec<String> {
        Vec::new()
    }
    /// Ground a bounded close in the work completed so far after a guard fires.
    async fn grounded_close(&self, request: &TurnRequest<P>, reason: &str) -> Option<TurnResult>;
}

/// Runner guard defaults. The idle timeout catches silence; the deadline is a
/// separate total wall-clock ceiling and can be overridden per request.
#[derive(Debug, Clone, Copy)]
pub struct TurnRunnerConfig {
    pub idle_timeout: Duration,
    pub deadline: Option<Duration>,
    pub close_timeout: Duration,
}

impl Default for TurnRunnerConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(900),
            deadline: None,
            close_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TurnRunnerError {
    #[error("turn was consumed by ingress policy")]
    Consumed,
    #[error("turn was accepted into the host queue")]
    Queued,
    #[error("turn rejected: {0}")]
    Rejected(String),
    #[error("turn failed: {0}")]
    Failed(String),
    #[error("turn cancelled")]
    Cancelled,
}

#[derive(Clone)]
struct ActiveTurn<P> {
    request_id: String,
    cancel: crate::CancellationToken,
    generation: u64,
    queue: Arc<RunQueue<TurnRequest<P>>>,
}

type ThreadLockKey = (ScopeKey, String);
type ThreadLocks = Mutex<HashMap<ThreadLockKey, Weak<Mutex<()>>>>;
type ParallelQueueKey = (ScopeKey, String, String);
type ParallelQueueMap<P> = Mutex<HashMap<ParallelQueueKey, Arc<RunQueue<TurnRequest<P>>>>>;

fn queue_lane(disposition: QueueDisposition) -> Option<QueueLane> {
    match disposition {
        QueueDisposition::Steer => Some(QueueLane::Steer),
        QueueDisposition::Followup => Some(QueueLane::Followup),
        QueueDisposition::Collect => Some(QueueLane::Collect),
        QueueDisposition::Interrupt | QueueDisposition::Parallel => None,
    }
}

/// Reusable turn lifecycle coordinator, parameterized entirely by host ports.
pub struct TurnRunner<H, I, R, E, F, S, P = serde_json::Value> {
    host: Arc<H>,
    ingress: Arc<I>,
    replies: Arc<R>,
    events: Arc<E>,
    failures: Arc<F>,
    summarizer: Arc<S>,
    config: TurnRunnerConfig,
    locks: ThreadLocks,
    parallel_queues: ParallelQueueMap<P>,
    active: Mutex<HashMap<(ScopeKey, String), ActiveTurn<P>>>,
    parallel_active: Mutex<HashMap<(ScopeKey, String, String), crate::CancellationToken>>,
    next_generation: std::sync::atomic::AtomicU64,
}

impl<H, I, R, E, F, S, P> TurnRunner<H, I, R, E, F, S, P>
where
    H: TurnHost<P> + 'static,
    I: IngressPolicy<P> + 'static,
    R: ReplyStore<P> + 'static,
    E: EventSink<P> + 'static,
    F: FailureClassifier + 'static,
    S: Summarizer<P> + 'static,
    P: Clone + Send + Sync + 'static,
{
    pub fn new(
        host: Arc<H>,
        ingress: Arc<I>,
        replies: Arc<R>,
        events: Arc<E>,
        failures: Arc<F>,
        summarizer: Arc<S>,
        config: TurnRunnerConfig,
    ) -> Self {
        Self {
            host,
            ingress,
            replies,
            events,
            failures,
            summarizer,
            config,
            locks: Mutex::new(HashMap::new()),
            parallel_queues: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            parallel_active: Mutex::new(HashMap::new()),
            next_generation: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Cancel only the active generation named by `request_id`; a stale cancel
    /// cannot target a newer turn that reused the same thread.
    pub async fn cancel(
        &self,
        scope: &ScopeKey,
        thread_id: &str,
        request_id: Option<&str>,
    ) -> bool {
        let key = (scope.clone(), thread_id.to_owned());
        let active = self.active.lock().await;
        let cancelled_primary = if let Some(turn) = active
            .get(&key)
            .filter(|turn| request_id.is_none_or(|id| turn.request_id == id))
        {
            turn.queue.clear().await;
            turn.cancel.cancel();
            true
        } else {
            false
        };
        drop(active);
        let mut parallel = self.parallel_active.lock().await;
        let keys: Vec<_> = parallel
            .keys()
            .filter(|(owner, thread, req)| {
                owner == scope && thread == thread_id && request_id.is_none_or(|id| req == id)
            })
            .cloned()
            .collect();
        for key in &keys {
            if let Some(token) = parallel.remove(key) {
                token.cancel();
            }
            self.parallel_queues.lock().await.remove(key);
        }
        cancelled_primary || !keys.is_empty()
    }

    /// Whether the thread owns a primary user-authored turn cancellation slot.
    pub async fn is_in_flight(&self, scope: &ScopeKey, thread_id: &str) -> bool {
        self.active
            .lock()
            .await
            .contains_key(&(scope.clone(), thread_id.to_owned()))
    }

    pub async fn in_flight_request_id(&self, scope: &ScopeKey, thread_id: &str) -> Option<String> {
        self.active
            .lock()
            .await
            .get(&(scope.clone(), thread_id.to_owned()))
            .map(|turn| turn.request_id.clone())
    }

    pub async fn run(&self, request: TurnRequest<P>) -> Result<TurnResult, TurnRunnerError> {
        let mut state = None;
        let result = self.run_one(request.clone(), None, &mut state).await;
        if result.is_ok() && request.disposition != QueueDisposition::Parallel {
            let mut owner = request.clone();
            let mut current = state.clone();
            loop {
                let Some(owner_state) = current.as_ref() else {
                    break;
                };
                let followup = self
                    .take_next_followup_or_release(
                        &owner.scope,
                        &owner.thread_id,
                        owner_state.generation,
                    )
                    .await;
                let Some((followup, reserved)) = followup else {
                    break;
                };
                owner = followup.clone();
                current = Some(reserved.clone());
                if self
                    .run_one(followup, Some(reserved), &mut current)
                    .await
                    .is_err()
                {
                    if let Some(state) = current.as_ref() {
                        self.clear_generation_queue(
                            &owner.scope,
                            &owner.thread_id,
                            state.generation,
                        )
                        .await;
                        self.clear_tracking(&owner, state.generation).await;
                    }
                    break;
                }
            }
        } else {
            if let Some(state) = state.as_ref() {
                if result.is_err() {
                    self.clear_generation_queue(
                        &request.scope,
                        &request.thread_id,
                        state.generation,
                    )
                    .await;
                }
                self.clear_tracking(&request, state.generation).await;
            } else if request.disposition == QueueDisposition::Parallel {
                self.clear_parallel_tracking(&request).await;
            }
        }
        result
    }

    async fn run_one(
        &self,
        mut request: TurnRequest<P>,
        reservation: Option<ActiveTurn<P>>,
        state_out: &mut Option<ActiveTurn<P>>,
    ) -> Result<TurnResult, TurnRunnerError> {
        for stage in [
            IngressStage::StageAttachments,
            IngressStage::Guardrail,
            IngressStage::ApprovalInterception,
            IngressStage::BeforeSubmit,
        ] {
            match self.ingress.apply(stage, &mut request).await {
                Ok(IngressDecision::Continue) => {}
                Ok(IngressDecision::Consumed) => return Err(TurnRunnerError::Consumed),
                Ok(IngressDecision::Reject(reason)) => {
                    return Err(TurnRunnerError::Rejected(reason));
                }
                Err(error) => return self.fail(&request, error).await,
            }
        }

        let key = (request.scope.clone(), request.thread_id.clone());
        let cancellation = reservation
            .as_ref()
            .map(|state| state.cancel.clone())
            .unwrap_or_default();
        let mut primary_queue = reservation.as_ref().map(|state| state.queue.clone());
        if !request.host_authored {
            if request.disposition == QueueDisposition::Parallel {
                self.parallel_active.lock().await.insert(
                    (
                        request.scope.clone(),
                        request.thread_id.clone(),
                        request.request_id.clone(),
                    ),
                    cancellation.clone(),
                );
            } else if let Some(reserved) = reservation.as_ref() {
                let mut active = self.active.lock().await;
                if active
                    .get(&key)
                    .is_none_or(|current| current.generation != reserved.generation)
                {
                    self.events
                        .terminal(
                            &request,
                            TerminalEvent::Cancelled {
                                request_id: request.request_id.clone(),
                            },
                        )
                        .await;
                    return Err(TurnRunnerError::Cancelled);
                }
                let mut state = reserved.clone();
                state.request_id = request.request_id.clone();
                state.cancel = cancellation.clone();
                active.insert(key.clone(), state.clone());
                *state_out = Some(state);
            } else {
                let mut active = self.active.lock().await;
                if let Some(lane) = queue_lane(request.disposition)
                    && let Some(current) = active.get(&key)
                {
                    current.queue.push(lane, request.clone()).await;
                    return Err(TurnRunnerError::Queued);
                }
                let queue = Arc::new(RunQueue::new());
                let state = ActiveTurn {
                    request_id: request.request_id.clone(),
                    cancel: cancellation.clone(),
                    generation: self
                        .next_generation
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    queue: queue.clone(),
                };
                let previous = active.insert(key.clone(), state.clone());
                *state_out = Some(state);
                primary_queue = Some(queue);
                if let Some(previous) = previous {
                    if request.disposition == QueueDisposition::Interrupt {
                        previous.queue.clear().await;
                    }
                    previous.cancel.cancel();
                }
            }
        }
        let run_queue = if request.disposition == QueueDisposition::Parallel {
            self.parallel_queue_for((
                request.scope.clone(),
                request.thread_id.clone(),
                request.request_id.clone(),
            ))
            .await
        } else if let Some(queue) = primary_queue {
            queue
        } else {
            Arc::new(RunQueue::new())
        };
        let lock = {
            let mut locks = self.locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(key.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let _session_guard = if request.disposition == QueueDisposition::Parallel {
            None
        } else {
            match tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                guard = lock.lock_owned() => Some(guard),
            } {
                Some(guard) => Some(guard),
                None => {
                    self.events
                        .terminal(
                            &request,
                            TerminalEvent::Cancelled {
                                request_id: request.request_id.clone(),
                            },
                        )
                        .await;
                    return Err(TurnRunnerError::Cancelled);
                }
            }
        };
        let checkout = if request.disposition == QueueDisposition::Parallel {
            self.host.checkout_fork(&request).await
        } else {
            self.host.checkout(&request).await
        };
        let session = match checkout {
            Ok(session) => session,
            Err(error) => {
                return self.fail(&request, error).await;
            }
        };
        if let Err(error) = self.host.validate_fingerprint(&request, &session).await {
            self.host.checkin(&request).await;
            return self.fail(&request, error).await;
        }
        let origin = self.host.run_origin(&request);

        let result = self
            .run_guarded(
                &request,
                &session,
                &origin,
                cancellation.clone(),
                Arc::clone(&run_queue),
            )
            .await;
        self.host.checkin(&request).await;

        match result {
            Ok(result) => {
                let reply_id =
                    deterministic_reply_id(&request.scope, &request.thread_id, &request.request_id);
                if let Err(error) = self.replies.persist(&request, &reply_id, &result).await {
                    return self.fail(&request, error).await;
                }
                let suggestions = self.summarizer.suggestions(&request, &result).await;
                self.events.suggestions(&request, suggestions).await;
                if !request.host_authored && request.disposition != QueueDisposition::Parallel {
                    let collected = run_queue.take(QueueLane::Collect, QueueMode::All).await;
                    self.events.collected(&request, collected).await;
                }
                self.events
                    .terminal(
                        &request,
                        TerminalEvent::Completed {
                            request_id: request.request_id.clone(),
                            reply_id,
                        },
                    )
                    .await;
                Ok(result)
            }
            Err(_error) if cancellation.is_cancelled() => {
                self.events
                    .terminal(
                        &request,
                        TerminalEvent::Cancelled {
                            request_id: request.request_id.clone(),
                        },
                    )
                    .await;
                Err(TurnRunnerError::Cancelled)
            }
            Err(error) => self.fail(&request, error).await,
        }
    }

    async fn parallel_queue_for(&self, key: ParallelQueueKey) -> Arc<RunQueue<TurnRequest<P>>> {
        let mut queues = self.parallel_queues.lock().await;
        Arc::clone(
            queues
                .entry(key)
                .or_insert_with(|| Arc::new(RunQueue::new())),
        )
    }

    async fn take_next_followup_or_release(
        &self,
        scope: &ScopeKey,
        thread_id: &str,
        owner_generation: u64,
    ) -> Option<(TurnRequest<P>, ActiveTurn<P>)> {
        let key = (scope.clone(), thread_id.to_owned());
        let mut active = self.active.lock().await;
        if active
            .get(&key)
            .is_none_or(|turn| turn.generation != owner_generation)
        {
            return None;
        }
        let current = active.get(&key)?.clone();
        let next = current
            .queue
            .take(QueueLane::Followup, QueueMode::OneAtATime)
            .await
            .into_iter()
            .next();
        if let Some(request) = next {
            let reserved = ActiveTurn {
                request_id: request.request_id.clone(),
                cancel: crate::CancellationToken::new(),
                generation: self
                    .next_generation
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                queue: current.queue,
            };
            active.insert(key, reserved.clone());
            Some((request, reserved))
        } else {
            active.remove(&key);
            None
        }
    }

    /// Take queued work from one lane. `OneAtATime` is useful for steering;
    /// `All` preserves FIFO while applying every accepted item at a boundary.
    pub async fn take_queued(
        &self,
        scope: &ScopeKey,
        thread_id: &str,
        lane: QueueLane,
        mode: QueueMode,
    ) -> Vec<TurnRequest<P>> {
        let key = (scope.clone(), thread_id.to_owned());
        let queue = self
            .active
            .lock()
            .await
            .get(&key)
            .map(|turn| turn.queue.clone());
        match queue {
            Some(queue) => queue.take(lane, mode).await,
            None => Vec::new(),
        }
    }

    pub async fn queue_status(&self, scope: &ScopeKey, thread_id: &str) -> QueueStatus {
        let key = (scope.clone(), thread_id.to_owned());
        let queue = self
            .active
            .lock()
            .await
            .get(&key)
            .map(|turn| turn.queue.clone());
        match queue {
            Some(queue) => queue.status().await,
            None => QueueStatus {
                steers: 0,
                followups: 0,
                collects: 0,
                total: 0,
            },
        }
    }

    pub async fn clear_queue(&self, scope: &ScopeKey, thread_id: &str) -> usize {
        let key = (scope.clone(), thread_id.to_owned());
        let queue = self
            .active
            .lock()
            .await
            .get(&key)
            .map(|turn| turn.queue.clone());
        match queue {
            Some(queue) => queue.clear().await,
            None => 0,
        }
    }

    async fn clear_generation_queue(
        &self,
        scope: &ScopeKey,
        thread_id: &str,
        generation: u64,
    ) -> usize {
        let key = (scope.clone(), thread_id.to_owned());
        let active = self.active.lock().await;
        match active
            .get(&key)
            .filter(|turn| turn.generation == generation)
        {
            Some(turn) => turn.queue.clear().await,
            None => 0,
        }
    }

    pub async fn remove_queued(
        &self,
        scope: &ScopeKey,
        thread_id: &str,
        request_id: &str,
    ) -> usize {
        let key = (scope.clone(), thread_id.to_owned());
        let queue = self
            .active
            .lock()
            .await
            .get(&key)
            .map(|turn| turn.queue.clone());
        match queue {
            Some(queue) => {
                queue
                    .remove_where(|turn| turn.request_id == request_id)
                    .await
            }
            None => 0,
        }
    }

    async fn run_guarded(
        &self,
        request: &TurnRequest<P>,
        session: &TurnSession,
        origin: &serde_json::Value,
        cancellation: crate::CancellationToken,
        queue: Arc<RunQueue<TurnRequest<P>>>,
    ) -> Result<TurnResult, String> {
        let (progress_tx, mut progress_rx) = mpsc::channel(32);
        let task_cancellation = crate::CancellationToken::new();
        let run = self.host.run(
            request,
            session,
            origin,
            task_cancellation.clone(),
            queue,
            progress_tx,
        );
        let idle = request.idle_timeout.unwrap_or(self.config.idle_timeout);
        let deadline = request.deadline.or(self.config.deadline);
        let started = Instant::now();
        let mut last_progress = started;
        let mut work_in_flight = 0usize;
        let mut progress_open = true;
        tokio::pin!(run);
        loop {
            let idle_left = idle.saturating_sub(last_progress.elapsed());
            let deadline_left = deadline.map(|d| d.saturating_sub(started.elapsed()));
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    task_cancellation.cancel();
                    self.host.cancel_children(request).await;
                    return Err("turn cancelled".into());
                }
                signal = progress_rx.recv(), if progress_open => match signal {
                    Some(signal) => match signal {
                        TurnSignal::Progress(event) => {
                            last_progress = Instant::now();
                            self.events.progress(request, *event).await;
                        }
                        TurnSignal::WorkStarted => work_in_flight = work_in_flight.saturating_add(1),
                        TurnSignal::WorkFinished => {
                            work_in_flight = work_in_flight.saturating_sub(1);
                            if work_in_flight == 0 {
                                last_progress = Instant::now();
                            }
                        }
                    },
                    None => progress_open = false,
                },
                result = &mut run => return result,
                _ = tokio::time::sleep(idle_left), if work_in_flight == 0 => {
                    task_cancellation.cancel();
                    self.host.cancel_children(request).await;
                    return self.close_or_timeout(request, "idle timeout").await;
                }
                _ = async { if let Some(left) = deadline_left { tokio::time::sleep(left).await } else { std::future::pending().await } }, if deadline_left.is_some() => {
                    task_cancellation.cancel();
                    self.host.cancel_children(request).await;
                    return self.close_or_timeout(request, "wall-clock deadline").await;
                }
            }
        }
    }

    async fn close_or_timeout(
        &self,
        request: &TurnRequest<P>,
        reason: &str,
    ) -> Result<TurnResult, String> {
        match tokio::time::timeout(
            self.config.close_timeout,
            self.summarizer.grounded_close(request, reason),
        )
        .await
        {
            Ok(Some(result)) => Ok(result),
            _ => Err(format!("turn {reason}")),
        }
    }

    async fn clear_tracking(&self, request: &TurnRequest<P>, generation: u64) {
        let key = (request.scope.clone(), request.thread_id.clone());
        let mut active = self.active.lock().await;
        if active
            .get(&key)
            .is_some_and(|turn| turn.generation == generation)
        {
            active.remove(&key);
        }
    }

    async fn clear_parallel_tracking(&self, request: &TurnRequest<P>) {
        let key = (
            request.scope.clone(),
            request.thread_id.clone(),
            request.request_id.clone(),
        );
        self.parallel_active.lock().await.remove(&key);
        self.parallel_queues.lock().await.remove(&key);
    }

    async fn fail<T>(&self, request: &TurnRequest<P>, error: String) -> Result<T, TurnRunnerError> {
        let (class, message) = self.failures.classify(&error);
        self.events
            .terminal(
                request,
                TerminalEvent::Failed {
                    request_id: request.request_id.clone(),
                    message: message.clone(),
                    class,
                },
            )
            .await;
        Err(TurnRunnerError::Failed(message))
    }
}

/// Stable reply identity for idempotent host persistence.
pub fn deterministic_reply_id(scope: &ScopeKey, thread_id: &str, request_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(
        serde_json::to_vec(&(&scope.agent_id, &scope.profile_id, thread_id, request_id))
            .expect("serializing a tuple of strings cannot fail"),
    );
    let encoded = hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("reply-{encoded}")
}
