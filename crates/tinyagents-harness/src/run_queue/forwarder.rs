//! Abort-on-drop steering forwarder: bridges a host-owned [`RunQueue`] into a
//! running turn's [`SteeringHandle`].
//!
//! A host that owns a session-level queue of messages typed while a turn is
//! active spawns a 50 ms poll loop per turn that drains the queue's **steer**
//! and **collect** lanes and forwards them into the run's steering handle as
//! injected user messages; the harness applies them at the next iteration
//! checkpoint.
//!
//! Cancellation in hosts is usually **drop-based** (the turn future is dropped by
//! a `select!`; detached sub-agents are hard-aborted). A forwarder that only
//! cleaned up on normal return would detach and loop forever, pinning the queue
//! and handle and racing the next turn's forwarder for the session-owned queue.
//! [`SteeringForwarderGuard`] fixes this with RAII: its `Drop` aborts the poll
//! task, runs the host's cleanup hook (e.g. deregistering a steering-registry
//! entry), and drains residual (delivered-but-unapplied) steers back into the
//! queue so a late steer becomes the *next* turn's input instead of vanishing.
//! The requeue is synchronous: once the guard has dropped, every steer the run
//! did not apply is in the queue, so a host can read the leftovers right away.
//!
//! The queued payload, the delivery notifications and the cleanup are host
//! seams: [`QueuedMessage`] for the payload, [`ForwardEvent`] for notifications.

use std::sync::Arc;
use std::time::Duration;

use tinyinference_llm::message::Message as TaMessage;

use super::{QueueLane, RunQueue};
use crate::steering::{SteeringCommand, SteeringHandle};

/// Framing prepended to a queued **steer** message when it is injected as a user
/// turn. Shared so the residual-requeue path can strip it and avoid
/// double-prefixing when the next turn re-forwards the recovered text.
pub const STEER_PREFIX: &str = "[User steering message]: ";

/// Framing prepended to a queued **collect** message (orchestrator/monitor
/// context lines). See [`STEER_PREFIX`].
pub const COLLECT_PREFIX: &str = "[Additional context from user]: ";

/// Poll interval of the forwarder task.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A host payload the forwarder can read text from and re-create when requeuing
/// a recovered steer.
pub trait QueuedMessage: Send + 'static {
    /// Stable id of the queued item.
    fn id(&self) -> &str;
    /// The message text to inject.
    fn text(&self) -> &str;
    /// Build a recovered item to push back onto the queue. `id` is freshly
    /// minted, `thread_label` is the forwarder's thread label, and `queued_at_ms`
    /// is the wall clock in epoch milliseconds.
    fn requeued(id: String, text: String, thread_label: &str, queued_at_ms: u64) -> Self;
}

/// A delivery or requeue the forwarder performed, for host observability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardEvent {
    /// Queued messages were forwarded into the running steering handle.
    Delivered {
        /// Thread label the forwarder was armed with.
        thread_label: String,
        /// `"steer"` or `"collect"`.
        mode: &'static str,
        /// Number of messages delivered.
        delivered: usize,
        /// Id of the first delivered item.
        item_id: Option<String>,
        /// Full text of the first delivered item (hosts clip it for display).
        text: Option<String>,
    },
    /// Delivered-but-unapplied steers were pushed back onto the queue.
    Requeued {
        /// Thread label the forwarder was armed with.
        thread_label: String,
        /// Number of messages requeued.
        requeued: usize,
        /// Minted id of the first requeued item.
        item_id: Option<String>,
        /// Full text of the first requeued item.
        text: Option<String>,
    },
}

/// Host callback receiving [`ForwardEvent`]s.
pub type ForwardEventSink = Arc<dyn Fn(ForwardEvent) + Send + Sync>;

/// Host cleanup run when the guard drops (e.g. deregistering a sub-agent's
/// steering handle from a registry).
pub type ForwarderCleanup = Box<dyn FnOnce() + Send>;

/// Milliseconds since the Unix epoch (best-effort; `0` on a pre-epoch clock).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Shared shutdown gate between the poll task and the guard's `Drop`.
///
/// The poll task drains the queue and sends into the steering handle only
/// while holding this lock and seeing `false`; `Drop` sets it to `true` under
/// the same lock before it drains the handle. So an item is always either in
/// the queue or in the handle, never in flight, and once `Drop` has drained,
/// no later send can land in a handle nobody will read again.
type ClosedGate = std::sync::Mutex<bool>;

fn lock_gate(gate: &ClosedGate) -> std::sync::MutexGuard<'_, bool> {
    match gate.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct ForwardedItem {
    id: Option<String>,
    message: TaMessage,
}

trait QueueAdapter: Send + Sync {
    fn drain(&self, lane: QueueLane) -> Vec<ForwardedItem>;
    fn requeue(&self, lane: QueueLane, message: TaMessage, thread_label: &str) -> Option<String>;
}

struct BorrowedQueue<'a, T>(&'a RunQueue<T>);

impl<T: QueuedMessage> QueueAdapter for BorrowedQueue<'_, T> {
    fn drain(&self, lane: QueueLane) -> Vec<ForwardedItem> {
        self.0
            .drain_now(lane)
            .into_iter()
            .map(|item| ForwardedItem {
                id: Some(item.id().to_owned()),
                message: TaMessage::user(item.text().to_owned()),
            })
            .collect()
    }

    fn requeue(&self, lane: QueueLane, message: TaMessage, thread_label: &str) -> Option<String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.0.push_now(
            lane,
            T::requeued(id.clone(), message.text(), thread_label, now_ms()),
        );
        Some(id)
    }
}

struct SharedQueue<T>(Arc<RunQueue<T>>);

impl<T: QueuedMessage> QueueAdapter for SharedQueue<T> {
    fn drain(&self, lane: QueueLane) -> Vec<ForwardedItem> {
        BorrowedQueue(self.0.as_ref()).drain(lane)
    }

    fn requeue(&self, lane: QueueLane, message: TaMessage, thread_label: &str) -> Option<String> {
        BorrowedQueue(self.0.as_ref()).requeue(lane, message, thread_label)
    }
}

struct BorrowedMessageQueue<'a>(&'a RunQueue<TaMessage>);

impl QueueAdapter for BorrowedMessageQueue<'_> {
    fn drain(&self, lane: QueueLane) -> Vec<ForwardedItem> {
        self.0
            .drain_where_now(lane, |message| matches!(message, TaMessage::User(_)))
            .into_iter()
            .map(|message| ForwardedItem { id: None, message })
            .collect()
    }

    fn requeue(&self, lane: QueueLane, message: TaMessage, _thread_label: &str) -> Option<String> {
        self.0.push_now(lane, message);
        None
    }
}

struct SharedMessageQueue(Arc<RunQueue<TaMessage>>);

impl QueueAdapter for SharedMessageQueue {
    fn drain(&self, lane: QueueLane) -> Vec<ForwardedItem> {
        BorrowedMessageQueue(self.0.as_ref()).drain(lane)
    }

    fn requeue(&self, lane: QueueLane, message: TaMessage, thread_label: &str) -> Option<String> {
        BorrowedMessageQueue(self.0.as_ref()).requeue(lane, message, thread_label)
    }
}

fn add_prefix(mut message: TaMessage, prefix: &str) -> TaMessage {
    if let TaMessage::User(user) = &mut message {
        user.content.insert(
            0,
            tinyinference_llm::message::ContentBlock::Text(prefix.into()),
        );
    }
    message
}

fn strip_prefix(mut message: TaMessage) -> (TaMessage, QueueLane) {
    let first = match &message {
        TaMessage::User(user) => user.content.first(),
        _ => None,
    };
    let (prefix, lane) = if matches!(first, Some(tinyinference_llm::message::ContentBlock::Text(text)) if text.starts_with(STEER_PREFIX))
    {
        (STEER_PREFIX, QueueLane::Steer)
    } else if matches!(first, Some(tinyinference_llm::message::ContentBlock::Text(text)) if text.starts_with(COLLECT_PREFIX))
    {
        (COLLECT_PREFIX, QueueLane::Collect)
    } else {
        return (message, QueueLane::Steer);
    };
    if let TaMessage::User(user) = &mut message
        && let Some(tinyinference_llm::message::ContentBlock::Text(text)) = user.content.first_mut()
        && let Some(stripped) = text.strip_prefix(prefix)
    {
        *text = stripped.to_owned();
    }
    (message, lane)
}

/// Drain `lane` and forward it into `handle`. With a `gate`, the drain and
/// the delivery both happen while holding it, so once the gate is closed no
/// item is ever out of the queue without being in the handle; returns `false`
/// when the gate is already closed (the caller should stop polling).
#[allow(clippy::too_many_arguments)]
fn forward_lane(
    queue: &dyn QueueAdapter,
    handle: &SteeringHandle,
    thread_label: &str,
    lane: QueueLane,
    mode: &'static str,
    prefix: &str,
    sink: &ForwardEventSink,
    gate: Option<&ClosedGate>,
) -> bool {
    let (delivered, item_id, text) = {
        let closed = gate.map(lock_gate);
        if closed.as_deref().copied().unwrap_or(false) {
            return false;
        }
        let drained = queue.drain(lane);
        if drained.is_empty() {
            return true;
        }
        for msg in &drained {
            handle.send(SteeringCommand::InjectMessage(add_prefix(
                msg.message.clone(),
                prefix,
            )));
        }
        let (item_id, text) = drained
            .first()
            .map(|msg| (msg.id.clone(), Some(msg.message.text())))
            .unwrap_or((None, None));
        (drained.len(), item_id, text)
    };
    tracing::debug!(
        thread_id = thread_label,
        delivered,
        mode,
        "[run_queue] delivered message(s) into running steering handle"
    );
    sink(ForwardEvent::Delivered {
        thread_label: thread_label.to_string(),
        mode,
        delivered,
        item_id,
        text,
    });
    true
}

/// Drain the queue's pending **steer** messages and forward them to the
/// [`SteeringHandle`] as injected user turns (the harness applies them to the
/// working transcript at the next iteration checkpoint). Emits a
/// [`ForwardEvent::Delivered`] when at least one message is delivered.
pub async fn forward_steers<T: QueuedMessage>(
    queue: &RunQueue<T>,
    handle: &SteeringHandle,
    thread_label: &str,
    sink: &ForwardEventSink,
) {
    forward_lane(
        &BorrowedQueue(queue),
        handle,
        thread_label,
        QueueLane::Steer,
        "steer",
        STEER_PREFIX,
        sink,
        None,
    );
}

/// Forward any queued **collect** messages as injected user turns so they reach
/// the next LLM call as additional context, framed with [`COLLECT_PREFIX`].
/// Emits a [`ForwardEvent::Delivered`] on delivery.
///
/// This drains the lane, so an item forwarded here is no longer handed back on
/// [`AgentRun::collected`][crate::middleware::AgentRun::collected]: a host
/// that arms a forwarder over a queue opts its collect lane into injection
/// (see [`QueueLane::Collect`]).
pub async fn forward_collects<T: QueuedMessage>(
    queue: &RunQueue<T>,
    handle: &SteeringHandle,
    thread_label: &str,
    sink: &ForwardEventSink,
) {
    forward_lane(
        &BorrowedQueue(queue),
        handle,
        thread_label,
        QueueLane::Collect,
        "collect",
        COLLECT_PREFIX,
        sink,
        None,
    );
}

/// Forward native `Message` values from a queue into a steering handle.
pub async fn forward_message_steers(
    queue: &RunQueue<TaMessage>,
    handle: &SteeringHandle,
    thread_label: &str,
    sink: &ForwardEventSink,
) {
    forward_lane(
        &BorrowedMessageQueue(queue),
        handle,
        thread_label,
        QueueLane::Steer,
        "steer",
        STEER_PREFIX,
        sink,
        None,
    );
}

/// Forward native `Message` collect values into a steering handle.
pub async fn forward_message_collects(
    queue: &RunQueue<TaMessage>,
    handle: &SteeringHandle,
    thread_label: &str,
    sink: &ForwardEventSink,
) {
    forward_lane(
        &BorrowedMessageQueue(queue),
        handle,
        thread_label,
        QueueLane::Collect,
        "collect",
        COLLECT_PREFIX,
        sink,
        None,
    );
}

/// Shared poll and residual-recovery lifecycle for all queue payload adapters.
struct SteeringForwarderCore {
    forwarder: Option<tokio::task::JoinHandle<()>>,
    closed: Arc<ClosedGate>,
    handle: SteeringHandle,
    run_queue: Option<Arc<dyn QueueAdapter>>,
    cleanup: Option<ForwarderCleanup>,
    thread_label: String,
    sink: ForwardEventSink,
}

impl SteeringForwarderCore {
    fn new(
        handle: SteeringHandle,
        run_queue: Option<Arc<dyn QueueAdapter>>,
        cleanup: Option<ForwarderCleanup>,
        thread_label: String,
        sink: ForwardEventSink,
    ) -> Self {
        let closed = Arc::new(ClosedGate::new(false));
        let forwarder = run_queue.as_ref().map(|queue| {
            let loop_queue = queue.clone();
            let loop_handle = handle.clone();
            let loop_label = thread_label.clone();
            let loop_sink = sink.clone();
            let loop_closed = closed.clone();
            tokio::spawn(async move {
                for (lane, mode, prefix) in [
                    (QueueLane::Steer, "steer", STEER_PREFIX),
                    (QueueLane::Collect, "collect", COLLECT_PREFIX),
                ]
                .into_iter()
                .cycle()
                {
                    if lane == QueueLane::Steer {
                        tokio::time::sleep(POLL_INTERVAL).await;
                    }
                    if *lock_gate(&loop_closed) {
                        break;
                    }
                    if !forward_lane(
                        loop_queue.as_ref(),
                        &loop_handle,
                        &loop_label,
                        lane,
                        mode,
                        prefix,
                        &loop_sink,
                        Some(&loop_closed),
                    ) {
                        break;
                    }
                }
            })
        });
        tracing::debug!(
            thread_id = thread_label.as_str(),
            has_queue = run_queue.is_some(),
            "[run_queue] steering forwarder guard armed (abort-on-drop)"
        );
        Self {
            forwarder,
            closed,
            handle,
            run_queue,
            cleanup,
            thread_label,
            sink,
        }
    }
}

impl Drop for SteeringForwarderCore {
    fn drop(&mut self) {
        *lock_gate(&self.closed) = true;
        if self.forwarder.take().is_some() {
            tracing::debug!(
                thread_id = self.thread_label.as_str(),
                "[run_queue] closed steering forwarder (guard drop)"
            );
        }
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
            tracing::debug!("[run_queue] ran forwarder cleanup (guard drop)");
        }

        let residual = self.handle.drain();
        let requeue_items: Vec<_> = residual
            .into_iter()
            .filter_map(|command| match command {
                SteeringCommand::InjectMessage(message) => Some(strip_prefix(message)),
                _ => None,
            })
            .collect();
        let Some(queue) = self.run_queue.take() else {
            return;
        };
        if requeue_items.is_empty() {
            return;
        }

        let requeued = requeue_items.len();
        let text = requeue_items.first().map(|(message, _)| message.text());
        let mut item_id = None;
        for (message, lane) in requeue_items {
            let id = queue.requeue(lane, message, &self.thread_label);
            if item_id.is_none() {
                item_id = id;
            }
        }
        tracing::debug!(
            thread_id = self.thread_label.as_str(),
            requeued,
            "[run_queue] requeued residual steer(s) as next-turn input (guard drop)"
        );
        (self.sink)(ForwardEvent::Requeued {
            thread_label: self.thread_label.clone(),
            requeued,
            item_id,
            text,
        });
    }
}

/// Stop-on-drop guard for host-defined queued payloads.
pub struct SteeringForwarderGuard<T: QueuedMessage> {
    _core: SteeringForwarderCore,
    _payload: std::marker::PhantomData<fn() -> T>,
}

impl<T: QueuedMessage> SteeringForwarderGuard<T> {
    /// Arm the guard; cleanup still runs when there is no queue.
    pub fn new(
        handle: SteeringHandle,
        run_queue: Option<Arc<RunQueue<T>>>,
        cleanup: Option<ForwarderCleanup>,
        thread_label: String,
        sink: ForwardEventSink,
    ) -> Self {
        Self {
            _core: SteeringForwarderCore::new(
                handle,
                run_queue.map(|queue| Arc::new(SharedQueue(queue)) as Arc<dyn QueueAdapter>),
                cleanup,
                thread_label,
                sink,
            ),
            _payload: std::marker::PhantomData,
        }
    }
}

/// Stop-on-drop guard for a driver's native `RunQueue<Message>`.
pub struct MessageSteeringForwarderGuard {
    _core: SteeringForwarderCore,
}

impl MessageSteeringForwarderGuard {
    /// Arm the native-message queue forwarder without flattening user content.
    pub fn new(
        handle: SteeringHandle,
        run_queue: Option<Arc<RunQueue<TaMessage>>>,
        cleanup: Option<ForwarderCleanup>,
        thread_label: String,
        sink: ForwardEventSink,
    ) -> Self {
        Self {
            _core: SteeringForwarderCore::new(
                handle,
                run_queue.map(|queue| Arc::new(SharedMessageQueue(queue)) as Arc<dyn QueueAdapter>),
                cleanup,
                thread_label,
                sink,
            ),
        }
    }
}

#[cfg(test)]
#[path = "forwarder_tests.rs"]
mod tests;
