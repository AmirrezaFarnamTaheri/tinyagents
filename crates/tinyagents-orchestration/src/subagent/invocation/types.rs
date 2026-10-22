//! Type definitions for driver-backed asynchronous subagent jobs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use tinyagents_harness::cancel::CancellationToken;
use tinyagents_tasks::{DetachedTaskRegistry, SteeringRegistry};
use tokio::sync::watch;

/// Stable identifier returned immediately when a subagent job is spawned.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SubAgentJobId(pub(crate) String);

impl SubAgentJobId {
    /// Returns the host-safe identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SubAgentJobId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Observable lifecycle state of an asynchronous subagent job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubAgentJobStatus {
    /// Accepted and waiting for its Tokio task to begin polling.
    Queued,
    /// The child agent loop is executing.
    Running,
    /// The durable child is paused for host/user input and can be resumed.
    AwaitingInput,
    /// The child completed successfully.
    Completed,
    /// The child failed.
    Failed,
    /// The child stopped without a complete answer (timeout or budget); see
    /// [`SubAgentJob::incomplete_kind`].
    Incomplete,
    /// The child observed cooperative cancellation.
    Cancelled,
}

impl SubAgentJobStatus {
    /// Whether no further execution transition can occur.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Incomplete | Self::Cancelled
        )
    }
}

/// Host-queryable snapshot of one asynchronous subagent job.
///
/// The registry is the source of job snapshots. The link fields are populated
/// by the registry when a job is created; callers should obtain snapshots from
/// it rather than constructing this record directly.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubAgentJob {
    /// Stable job identifier returned by the spawning tool.
    pub id: SubAgentJobId,
    /// Named subagent executing the work.
    pub agent: String,
    /// Current lifecycle status.
    pub status: SubAgentJobStatus,
    /// Latest successful assistant text, once completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Host-safe failure text, once failed or cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Run id of the child run executing this job; the same id the child's
    /// own events and transcript use. Absent on jobs created before the link
    /// existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_run_id: Option<String>,
    /// Durable worker conversation id supplied by the host executor, when
    /// available for projecting a product-facing result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_thread_id: Option<String>,
    /// Host-owned result metadata carried through the neutral outcome
    /// extension map (for example, iteration count or durable session id).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extensions: std::collections::BTreeMap<String, serde_json::Value>,
    /// Id of the parent tool call that spawned this job, when the dispatcher
    /// supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_tool_call_id: Option<String>,
    /// Typed cause when [`Self::status`] is `Incomplete`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_kind: Option<crate::subagent::IncompleteKind>,
    /// Artifacts the result policy stored for an oversized output.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<crate::subagent::ArtifactReference>,
    /// Why the output failed the result policy's schema, if one was set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_error: Option<String>,
    /// Why an oversized output could not be stored as an artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_error: Option<String>,
}

/// Shared registry behind asynchronous subagent spawning and host controls.
///
/// This is a thin adapter over [`tinyagents_tasks::DetachedTaskRegistry`], the
/// one live detached-task registry implementation. The registry holds each
/// job's [`SubAgentJob`] snapshot as its watched status (so ownership,
/// snapshots, steering lookup and request-id dedupe are the detached
/// registry's), registered cooperatively because a job is stopped through its
/// own cancellation token rather than hard-aborted. The adapter adds only what
/// is specific to subagent jobs: the lifecycle transitions, the
/// cancel-then-settle protocol, and bounded retention of terminal jobs.
#[derive(Clone)]
pub struct SubAgentJobRegistry {
    // Private to `jobs.rs`. Invariant: only cooperative calls (register,
    // cancel_cooperative, release_cancellation, snapshots) may touch it, under
    // `controls`. Adapter cleanup removes from all three maps under the lock
    // order driver_jobs -> root_leases -> controls -> tasks.
    pub(super) tasks: DetachedTaskRegistry<JobMeta, SubAgentJob>,
    pub(crate) steering: SteeringRegistry,
    /// Status senders and cancel flags. Also the transition gate: every
    /// Mutation holds this lock, so a settle and a cancel never interleave.
    pub(crate) controls: Arc<Mutex<HashMap<SubAgentJobId, JobControl>>>,
    /// Scoped bridge from driver task identity to the public job control id.
    pub(crate) driver_jobs: Arc<Mutex<HashMap<crate::subagent::SubagentTaskKey, SubAgentJobId>>>,
    /// Monotonic settlement order for deterministic bounded terminal retention.
    pub(crate) next_terminal_sequence: Arc<AtomicU64>,
    /// Changes whenever root-lineage membership or a job lifecycle changes.
    pub(crate) changes: watch::Sender<u64>,
    /// Root-scoped retention leases; lock order is `root_leases -> controls`.
    pub(crate) root_leases: Arc<Mutex<HashMap<u64, RootLeaseRecord>>>,
    pub(crate) next_root_lease_id: Arc<AtomicU64>,
}

impl Default for SubAgentJobRegistry {
    fn default() -> Self {
        let steering = SteeringRegistry::new();
        Self {
            tasks: DetachedTaskRegistry::new(
                steering.clone(),
                SUBAGENT_JOB_SOFT_CAP,
                |job: &SubAgentJob| job.status.is_terminal(),
            ),
            steering,
            controls: Arc::default(),
            driver_jobs: Arc::default(),
            next_terminal_sequence: Arc::new(AtomicU64::new(1)),
            changes: watch::channel(0).0,
            root_leases: Arc::default(),
            next_root_lease_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

/// Maximum terminal-job retention before a registration prunes old entries.
pub(super) const SUBAGENT_JOB_SOFT_CAP: usize = 128;

/// Application metadata kept with each job in the detached registry.
#[derive(Debug, Default)]
pub(crate) struct JobMeta {
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) workspace: Option<tinytools::WorkspaceDescriptor>,
}

impl Clone for JobMeta {
    fn clone(&self) -> Self {
        #[cfg(test)]
        if PANIC_ON_JOB_META_CLONE.with(std::cell::Cell::get) {
            panic!("poison subagent detached registry during metadata clone");
        }
        Self {
            parent_thread_id: self.parent_thread_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

/// Exact workspace and parent-thread identity used for scoped job control.
/// Thread ids are only unique within a workspace, so both values are required.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentJobScope {
    /// Workspace owning the parent conversation.
    pub workspace: tinytools::WorkspaceDescriptor,
    /// Stable parent thread id within that workspace.
    pub thread_id: String,
}

impl ParentJobScope {
    /// Creates an exact scope. Missing identity cannot act as a wildcard.
    pub fn new(workspace: tinytools::WorkspaceDescriptor, thread_id: impl Into<String>) -> Self {
        Self {
            workspace,
            thread_id: thread_id.into(),
        }
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static PANIC_ON_JOB_META_CLONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The adapter-owned half of a job: the status publisher and cancel flag.
pub(crate) struct JobControl {
    pub(crate) status: watch::Sender<SubAgentJob>,
    pub(crate) owner: String,
    /// Whether cancellation was requested while the child was still running.
    /// The job remains non-terminal until the child reports its result.
    pub(crate) cancellation_requested: bool,
    /// Optional loop-consumed message queue for typed message aliases.
    pub run_queue: Option<tinyagents_harness::run_queue::RunQueueHandle>,
    pub(crate) steering: tinyagents_harness::steering::SteeringHandle,
    pub cancellation: CancellationToken,
    /// Watchers keep a terminal job from cap pruning until they consume its
    /// final status or drop their wait future.
    pub(crate) waiters: Arc<AtomicUsize>,
    /// Settlement order used to evict the oldest unpinned terminal record.
    pub(crate) terminal_sequence: AtomicU64,
    /// Native parent-to-attempt edges, including every resumed attempt.
    pub(crate) lineage: JobLineage,
}

pub(crate) struct RootLeaseRecord {
    pub(crate) owner: String,
    pub(crate) reachable_runs: std::collections::HashSet<tinyagents_harness::ids::RunId>,
    pub(crate) pinned_jobs: std::collections::HashSet<SubAgentJobId>,
}

#[derive(Default)]
pub(crate) struct JobLineage {
    pub(crate) parents: std::collections::HashSet<tinyagents_harness::ids::RunId>,
    pub(crate) children: std::collections::HashSet<tinyagents_harness::ids::RunId>,
}

impl JobLineage {
    pub(crate) fn new(
        parent: Option<tinyagents_harness::ids::RunId>,
        child: Option<tinyagents_harness::ids::RunId>,
    ) -> Self {
        let mut lineage = Self::default();
        lineage.add(parent, child);
        lineage
    }

    pub(crate) fn add(
        &mut self,
        parent: Option<tinyagents_harness::ids::RunId>,
        child: Option<tinyagents_harness::ids::RunId>,
    ) {
        if let Some(parent) = parent {
            self.parents.insert(parent);
        }
        if let Some(child) = child {
            self.children.insert(child);
        }
    }
}

pub struct DriverJobRegistration {
    pub key: crate::subagent::SubagentTaskKey,
    pub agent: String,
    pub owner: String,
    pub workspace: Option<tinytools::WorkspaceDescriptor>,
    pub cancellation: CancellationToken,
    pub steering: Option<tinyagents_harness::steering::SteeringHandle>,
    pub run_queue: Option<tinyagents_harness::run_queue::RunQueueHandle>,
    pub continuing: bool,
}

/// Error returned by job lookup or live-message delivery.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SubAgentJobError {
    /// No job exists for the supplied id.
    #[error("unknown subagent job `{0}`")]
    NotFound(String),
    /// A lane-aware message was requested for a job without a message queue.
    #[error("subagent job `{0}` has no lane-aware message queue")]
    LaneUnavailable(String),
    /// A job with this public id is already registered.
    #[error("subagent job `{0}` is already registered")]
    AlreadyExists(String),
    /// The underlying detached task registry rejected a driver bridge.
    #[error("subagent job registry failed: {0}")]
    Registry(String),
    /// Messages and cancellation only apply while a job is queued or running.
    #[error("subagent job `{job_id}` is already {status:?}")]
    Terminal {
        /// Target job id.
        job_id: String,
        /// Terminal status observed by the registry.
        status: SubAgentJobStatus,
    },
    /// Cancellation has been requested and queued messages will not be read.
    #[error("subagent job `{0}` is cancelling")]
    Cancelling(String),
    /// The request identifier exceeds the bounded registry size.
    #[error("subagent request id is too long")]
    RequestIdTooLong,
    /// Descendant settlement reached its one shared absolute deadline.
    #[error("timed out while settling descendant subagent jobs")]
    SettlementTimedOut,
    /// Descendant settlement was interrupted by its caller's cancellation.
    #[error("descendant subagent settlement was cancelled")]
    SettlementCancelled,
}
