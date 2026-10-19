//! Public types for the active-run queue.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tinyinference_llm::message::Message;

/// The shared queue the agent loop drains: a [`RunQueue`][super::RunQueue]
/// of transcript-ready [`Message`]s.
///
/// Attach one to a run with
/// [`RunContext::with_run_queue`][crate::context::RunContext::with_run_queue]
/// and keep a clone to push into from outside the run. `Steer` and
/// `Followup` items are appended to the transcript verbatim, so push them as
/// [`Message::user`] (or [`Message::system`]) — the host chooses the role.
pub type RunQueueHandle = Arc<super::RunQueue<Message>>;

/// A retained capability checked when a queued item is consumed by an agent
/// turn. Hosts can attach durable lifecycle admission to messages without
/// changing the transcript payload.
pub trait QueueAdmission: std::fmt::Debug + Send + Sync {
    /// Acquires a short-lived permit that linearizes this item with lifecycle
    /// closure. Implementations must return an error when the lifecycle no
    /// longer admits the item. The permit is held only while the harness
    /// transfers the item into a turn, never across a provider await.
    fn acquire(&self) -> Result<std::sync::Arc<dyn QueueAdmissionPermit>, QueueAdmissionError>;

    /// Acquires the identity returned by the queue's immediately preceding
    /// `batch_key` call. Implementations with replaceable authority should
    /// reject a changed identity so a permit is never cached under a stale key.
    fn acquire_for(
        &self,
        _expected_key: Option<&str>,
    ) -> Result<std::sync::Arc<dyn QueueAdmissionPermit>, QueueAdmissionError> {
        self.acquire()
    }

    /// Whether a failed acquisition is temporary and the item should remain
    /// queued for a later attempt. Lifecycle rejection is permanent by default.
    fn retry_after_rejection(&self, _expected_key: Option<&str>) -> bool {
        false
    }

    /// Stable identity for capabilities that may share one batch permit.
    /// `None` means each item requires an independent acquisition.
    fn batch_key(&self) -> Option<String> {
        None
    }
}

/// Owned authority held across the queue-to-turn transfer.
pub trait QueueAdmissionPermit: Send + Sync {}

impl QueueAdmissionPermit for () {}

/// The queued item's admission capability is no longer valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueAdmissionError;

/// A queue lane consumed by the agent runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueLane {
    /// Inject at the next safe iteration boundary as an instruction.
    Steer,
    /// Dispatch as a fresh turn after the active run completes.
    Followup,
    /// Collected context handed back to the host on
    /// [`AgentRun::collected`][crate::middleware::AgentRun::collected] at
    /// run end, and not injected into the transcript by the harness itself.
    ///
    /// A host that arms a
    /// [`SteeringForwarderGuard`][crate::run_queue::SteeringForwarderGuard]
    /// over the queue opts into the other path: the forwarder drains this lane
    /// during the run and injects each item as framed additional context, so
    /// those items no longer appear on `AgentRun::collected`.
    Collect,
}

impl QueueLane {
    /// Returns a stable, snake_case name for this lane, suitable for logging
    /// and event labels (e.g. `"followup"`).
    pub fn as_str(self) -> &'static str {
        match self {
            QueueLane::Steer => "steer",
            QueueLane::Followup => "followup",
            QueueLane::Collect => "collect",
        }
    }
}

/// How many queued items the agent loop takes from a lane at one safe
/// boundary. Mirrors pi's `QueueMode` (`"one-at-a-time" | "all"`).
///
/// Set on [`crate::runtime::RunPolicy::queue_mode`]; consulted by
/// [`RunQueue::take`][crate::run_queue::RunQueue::take].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueMode {
    /// Apply only the oldest queued item per boundary; the rest wait for the
    /// next one. Gives the model a chance to react to each instruction
    /// separately.
    OneAtATime,
    /// Apply every item queued in the lane at the boundary, in FIFO order.
    /// The default.
    #[default]
    All,
}

/// Snapshot of the queue depth per lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct QueueStatus {
    /// Number of pending steer items.
    pub steers: usize,
    /// Number of pending follow-up items.
    pub followups: usize,
    /// Number of pending collected-context items.
    pub collects: usize,
    /// Total number of pending items across all lanes.
    pub total: usize,
}
