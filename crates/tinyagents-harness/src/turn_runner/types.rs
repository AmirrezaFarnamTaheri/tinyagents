use std::{collections::HashMap, hash::Hash, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;

use crate::progress::AgentProgress;

/// Identity that isolates session locks and active turns between agents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeKey {
    pub agent_id: String,
    pub profile_id: Option<String>,
}

/// A request after the host has supplied its channel-independent metadata.
#[derive(Debug, Clone)]
pub struct TurnRequest {
    pub scope: ScopeKey,
    pub thread_id: String,
    pub request_id: String,
    pub text: String,
    pub host_authored: bool,
    pub disposition: QueueDisposition,
    pub deadline: Option<Duration>,
    pub idle_timeout: Option<Duration>,
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
pub trait IngressPolicy: Send + Sync {
    async fn apply(
        &self,
        stage: IngressStage,
        request: &mut TurnRequest,
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
    Completed { request_id: String, reply_id: String },
    Failed { request_id: String, message: String, class: String },
    Cancelled { request_id: String },
}

#[async_trait]
pub trait TurnHost: Send + Sync {
    /// Check out or create the session for this request and verify its fingerprint.
    async fn checkout(&self, request: &TurnRequest) -> Result<(), String>;
    /// Run the actual harness/session turn. Progress is forwarded to the sink.
    async fn run(
        &self,
        request: &TurnRequest,
        cancellation: crate::CancellationToken,
        progress: mpsc::Sender<AgentProgress>,
    ) -> Result<TurnResult, String>;
    /// Release the session after the run, including on cancellation or failure.
    async fn checkin(&self, request: &TurnRequest);
    /// Cancel child runs belonging to this parent request.
    async fn cancel_children(&self, request: &TurnRequest);
    /// Accept an incoming request into the host's turn queue. Return `true`
    /// when it was queued instead of run immediately.
    async fn queue(&self, _request: &TurnRequest, _mode: QueueDisposition) -> Result<bool, String> {
        Ok(false)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnResult {
    pub reply: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[async_trait]
pub trait ReplyStore: Send + Sync {
    async fn persist(&self, request: &TurnRequest, reply_id: &str, result: &TurnResult)
        -> Result<(), String>;
}

#[async_trait]
pub trait EventSink: Send + Sync {
    async fn progress(&self, request: &TurnRequest, event: AgentProgress);
    async fn terminal(&self, request: &TurnRequest, event: TerminalEvent);
    async fn suggestions(&self, _request: &TurnRequest, _suggestions: Vec<String>) {}
}

pub trait FailureClassifier: Send + Sync {
    fn classify(&self, error: &str) -> (String, String);
}

#[async_trait]
pub trait Summarizer: Send + Sync {
    /// Optional next-message suggestions for a successfully completed turn.
    async fn suggestions(&self, _request: &TurnRequest, _result: &TurnResult) -> Vec<String> {
        Vec::new()
    }
    /// Ground a bounded close in the work completed so far after a guard fires.
    async fn grounded_close(&self, request: &TurnRequest, reason: &str) -> Option<TurnResult>;
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

struct ActiveTurn {
    request_id: String,
    request: TurnRequest,
    cancel: crate::CancellationToken,
}

/// Reusable turn lifecycle coordinator, parameterized entirely by host ports.
pub struct TurnRunner<H, I, R, E, F, S> {
    host: Arc<H>,
    ingress: Arc<I>,
    replies: Arc<R>,
    events: Arc<E>,
    failures: Arc<F>,
    summarizer: Arc<S>,
    config: TurnRunnerConfig,
    locks: Mutex<HashMap<(ScopeKey, String), Arc<Mutex<()>>>>,
    active: Mutex<HashMap<(ScopeKey, String), ActiveTurn>>,
}

impl<H, I, R, E, F, S> TurnRunner<H, I, R, E, F, S>
where
    H: TurnHost + 'static,
    I: IngressPolicy + 'static,
    R: ReplyStore + 'static,
    E: EventSink + 'static,
    F: FailureClassifier + 'static,
    S: Summarizer + 'static,
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
            active: Mutex::new(HashMap::new()),
        }
    }

    /// Cancel only the active generation named by `request_id`; a stale cancel
    /// cannot target a newer turn that reused the same thread.
    pub async fn cancel(&self, scope: &ScopeKey, thread_id: &str, request_id: &str) -> bool {
        let key = (scope.clone(), thread_id.to_owned());
        let active = self.active.lock().await;
        if let Some(turn) = active.get(&key).filter(|turn| turn.request_id == request_id) {
            turn.cancel.cancel();
            true
        } else {
            false
        }
    }

    pub async fn run(&self, mut request: TurnRequest) -> Result<TurnResult, TurnRunnerError> {
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
        let cancellation = crate::CancellationToken::new();
        if !request.host_authored {
            if !matches!(request.disposition, QueueDisposition::Interrupt | QueueDisposition::Parallel)
                && self.host.queue(&request, request.disposition).await.unwrap_or(false)
            {
                return Err(TurnRunnerError::Queued);
            }
            let previous = if request.disposition == QueueDisposition::Parallel {
                None
            } else {
                self.active.lock().await.insert(
                    key.clone(),
                    ActiveTurn {
                        request_id: request.request_id.clone(),
                        request: request.clone(),
                        cancel: cancellation.clone(),
                    },
                )
            };
            if let Some(previous) = previous {
                previous.cancel.cancel();
                self.host.cancel_children(&previous.request).await;
            }
        }
        let lock = {
            let mut locks = self.locks.lock().await;
            Arc::clone(locks.entry(key.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _session_guard = if request.disposition == QueueDisposition::Parallel {
            None
        } else {
            Some(lock.lock_owned().await)
        };
        if let Err(error) = self.host.checkout(&request).await {
            return self.fail(&request, error).await;
        }

        let result = self.run_guarded(&request, cancellation.clone()).await;
        self.host.checkin(&request).await;
        if !request.host_authored {
            let mut active = self.active.lock().await;
            if active.get(&key).is_some_and(|turn| turn.request_id == request.request_id) {
                active.remove(&key);
            }
        }

        match result {
            Ok(result) => {
                let reply_id = deterministic_reply_id(&request.scope, &request.thread_id, &request.request_id);
                if let Err(error) = self.replies.persist(&request, &reply_id, &result).await {
                    return self.fail(&request, error).await;
                }
                let suggestions = self.summarizer.suggestions(&request, &result).await;
                self.events.suggestions(&request, suggestions).await;
                self.events.terminal(
                    &request,
                    TerminalEvent::Completed { request_id: request.request_id.clone(), reply_id },
                ).await;
                Ok(result)
            }
            Err(_error) if cancellation.is_cancelled() => {
                self.events.terminal(&request, TerminalEvent::Cancelled { request_id: request.request_id.clone() }).await;
                Err(TurnRunnerError::Cancelled)
            }
            Err(error) => self.fail(&request, error).await,
        }
    }

    async fn run_guarded(&self, request: &TurnRequest, cancellation: crate::CancellationToken) -> Result<TurnResult, String> {
        let (progress_tx, mut progress_rx) = mpsc::channel(32);
        let run = self.host.run(request, cancellation.clone(), progress_tx);
        let idle = request.idle_timeout.unwrap_or(self.config.idle_timeout);
        let deadline = request.deadline.or(self.config.deadline);
        let started = Instant::now();
        let mut last_progress = started;
        tokio::pin!(run);
        loop {
            let idle_left = idle.saturating_sub(last_progress.elapsed());
            let deadline_left = deadline.map(|d| d.saturating_sub(started.elapsed()));
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    self.host.cancel_children(request).await;
                    return Err("turn cancelled".into());
                }
                event = progress_rx.recv() => if let Some(event) = event {
                    last_progress = Instant::now();
                    self.events.progress(request, event).await;
                },
                result = &mut run => return result,
                _ = tokio::time::sleep(idle_left), if idle_left > Duration::ZERO => {
                    cancellation.cancel();
                    self.host.cancel_children(request).await;
                    return self.close_or_timeout(request, "idle timeout").await;
                }
                _ = async { if let Some(left) = deadline_left { tokio::time::sleep(left).await } else { std::future::pending().await } }, if deadline_left.is_some() => {
                    cancellation.cancel();
                    self.host.cancel_children(request).await;
                    return self.close_or_timeout(request, "wall-clock deadline").await;
                }
            }
        }
    }

    async fn close_or_timeout(&self, request: &TurnRequest, reason: &str) -> Result<TurnResult, String> {
        match tokio::time::timeout(self.config.close_timeout, self.summarizer.grounded_close(request, reason)).await {
            Ok(Some(result)) => Ok(result),
            _ => Err(format!("turn {reason}")),
        }
    }

    async fn fail<T>(&self, request: &TurnRequest, error: String) -> Result<T, TurnRunnerError> {
        let (class, message) = self.failures.classify(&error);
        self.events.terminal(request, TerminalEvent::Failed {
            request_id: request.request_id.clone(),
            message: message.clone(),
            class,
        }).await;
        Err(TurnRunnerError::Failed(message))
    }
}

/// Stable reply identity for idempotent host persistence.
pub fn deterministic_reply_id(scope: &ScopeKey, thread_id: &str, request_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(scope.agent_id.as_bytes());
    hash.update([0]);
    hash.update(scope.profile_id.as_deref().unwrap_or_default().as_bytes());
    hash.update([0]);
    hash.update(thread_id.as_bytes());
    hash.update([0]);
    hash.update(request_id.as_bytes());
    format!("reply-{:x}", hash.finalize())
}
