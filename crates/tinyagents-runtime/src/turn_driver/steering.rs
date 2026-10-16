//! Seams that connect a run's steering handle to the outside world: a
//! session-owned run queue feeding mid-flight messages in, and a registry a
//! child run's handle is published to.

use std::sync::Arc;

use async_trait::async_trait;
use tinyagents_harness::run_queue::{
    self, ForwardEventSink, ForwarderCleanup, MessageSteeringForwarderGuard, QueuedMessage,
    RunQueue, RunQueueHandle, SteeringForwarderGuard,
};
use tinyagents_harness::steering::SteeringHandle;

/// Keeps a feed's background work alive; dropping it stops the work, runs the
/// cleanup and hands undelivered messages back.
pub type FeedGuard = Box<dyn Send>;

/// Source of messages injected into a running turn.
#[async_trait]
pub trait SteeringFeed: Send + Sync {
    /// Delivers everything already queued so a message queued before the turn
    /// started lands ahead of the first model call.
    async fn prime(&self, handle: &SteeringHandle, thread_label: &str);

    /// Starts forwarding messages queued from now on. The returned guard must
    /// be held until the run settles; `cleanup` runs once when it drops, on
    /// every exit path including cancellation.
    fn arm(
        &self,
        handle: SteeringHandle,
        thread_label: String,
        cleanup: Option<ForwarderCleanup>,
    ) -> FeedGuard;
}

/// Where a child run's steering handle is published while it runs.
pub trait SteeringRegistrar: Send + Sync {
    /// Called before the run starts with the child's task id.
    fn register(&self, task_id: &str, handle: &SteeringHandle);
    /// Called exactly once when the run is over, however it ended.
    fn deregister(&self, task_id: &str);
}

/// A [`SteeringFeed`] over a harness [`RunQueue`]: steer and collect lanes are
/// injected as user turns, and steers the run never applied are requeued.
pub struct RunQueueFeed<T: QueuedMessage> {
    queue: Arc<RunQueue<T>>,
    sink: ForwardEventSink,
}

impl<T: QueuedMessage> RunQueueFeed<T> {
    /// Feeds from `queue`, reporting deliveries and requeues to `sink`.
    pub fn new(queue: Arc<RunQueue<T>>, sink: ForwardEventSink) -> Self {
        Self { queue, sink }
    }

    /// Feeds from `queue` without observing deliveries.
    pub fn silent(queue: Arc<RunQueue<T>>) -> Self {
        Self::new(queue, Arc::new(|_| {}))
    }
}

#[async_trait]
impl<T: QueuedMessage> SteeringFeed for RunQueueFeed<T> {
    async fn prime(&self, handle: &SteeringHandle, thread_label: &str) {
        run_queue::forward_steers(&self.queue, handle, thread_label, &self.sink).await;
        run_queue::forward_collects(&self.queue, handle, thread_label, &self.sink).await;
    }

    fn arm(
        &self,
        handle: SteeringHandle,
        thread_label: String,
        cleanup: Option<ForwarderCleanup>,
    ) -> FeedGuard {
        Box::new(SteeringForwarderGuard::new(
            handle,
            Some(self.queue.clone()),
            cleanup,
            thread_label,
            self.sink.clone(),
        ))
    }
}

/// A [`SteeringFeed`] for the driver's native [`Message`] queue.
///
/// Unlike host-defined [`RunQueueFeed`] payloads, native messages have no
/// stable queue item ID. This adapter preserves complete user message content
/// while forwarding and leaves unsupported non-user messages in their lane.
pub struct MessageRunQueueFeed {
    queue: RunQueueHandle,
    sink: ForwardEventSink,
}

impl MessageRunQueueFeed {
    /// Feeds from a native message queue, reporting deliveries and requeues.
    pub fn new(queue: RunQueueHandle, sink: ForwardEventSink) -> Self {
        Self { queue, sink }
    }

    /// Feeds from a native message queue without observing deliveries.
    pub fn silent(queue: RunQueueHandle) -> Self {
        Self::new(queue, Arc::new(|_| {}))
    }
}

#[async_trait]
impl SteeringFeed for MessageRunQueueFeed {
    async fn prime(&self, handle: &SteeringHandle, thread_label: &str) {
        run_queue::forward_message_steers(&self.queue, handle, thread_label, &self.sink).await;
        run_queue::forward_message_collects(&self.queue, handle, thread_label, &self.sink).await;
    }

    fn arm(
        &self,
        handle: SteeringHandle,
        thread_label: String,
        cleanup: Option<ForwarderCleanup>,
    ) -> FeedGuard {
        Box::new(MessageSteeringForwarderGuard::new(
            handle,
            Some(self.queue.clone()),
            cleanup,
            thread_label,
            self.sink.clone(),
        ))
    }
}

/// A message type for a steering-only guard that has no queue.
pub(crate) struct NoQueued;

impl QueuedMessage for NoQueued {
    fn id(&self) -> &str {
        ""
    }
    fn text(&self) -> &str {
        ""
    }
    fn requeued(_: String, _: String, _: &str, _: u64) -> Self {
        Self
    }
}

/// Runs `cleanup` on drop; used when a child run has a registrar but no queue.
pub(crate) fn cleanup_only_guard(
    handle: SteeringHandle,
    cleanup: ForwarderCleanup,
    thread_label: String,
) -> FeedGuard {
    Box::new(SteeringForwarderGuard::<NoQueued>::new(
        handle,
        None,
        Some(cleanup),
        thread_label,
        Arc::new(|_| {}),
    ))
}

#[cfg(test)]
#[path = "steering_tests.rs"]
mod tests;
