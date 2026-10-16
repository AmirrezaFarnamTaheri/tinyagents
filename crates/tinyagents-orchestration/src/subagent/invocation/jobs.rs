//! Asynchronous subagent job registry and host-facing control tools.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::{RunId, TaskId, next_seq};
use tinyagents_harness::run_queue::QueueLane;
use tinyagents_harness::steering::{
    SteeringCommand, SteeringCommandKind, SteeringHandle, SteeringPolicy,
};
use tinyagents_harness::tool::{ToolDispatch, ToolRegistry};
use tinyinference_llm::message::Message;
use tinytools::{Tool, ToolResult};

use crate::subagent::{
    AppliedResult, DurableSubagentSessionSummary, IncompleteKind, SubagentSessionStore,
};

use tinyagents_tasks::{DetachedTaskRegistryError, OrchestrationTaskFilter, TaskStore};
use tokio::sync::watch;

use super::types::{DriverJobRegistration, JobControl, JobMeta};
use super::{
    JobLink, SubAgentJob, SubAgentJobError, SubAgentJobId, SubAgentJobRegistry, SubAgentJobStatus,
};

type SessionOverlay = (
    Arc<dyn SubagentSessionStore>,
    Arc<dyn Fn(u64) -> String + Send + Sync>,
);
type TaskOverlay = (Arc<dyn TaskStore>, Arc<dyn Fn(u64) -> RunId + Send + Sync>);

const LOG_PREFIX: &str = "[subagent-jobs]";
struct JobExecutionControls {
    cancellation: CancellationToken,
    steering: SteeringHandle,
    run_queue: Option<tinyagents_harness::run_queue::RunQueueHandle>,
}

/// A fixed operation for a host-named alias of [`SubAgentJobsTool`].
///
/// The default tool continues to read its `action` argument. Hosts that keep
/// separate model-facing operations can bind one action while supplying the
/// existing tool name and schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubAgentJobAction {
    /// Read one job or list jobs when no identifier is supplied.
    Query,
    /// Cancel one owned live job.
    Cancel,
    /// Close one owned durable session.
    Close,
    /// Wait for one job, or for all jobs when no identifier is supplied.
    Await,
}

impl SubAgentJobAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Cancel => "cancel",
            Self::Close => "close",
            Self::Await => "await",
        }
    }
}

#[derive(Clone)]
struct JobArgumentNames {
    action: String,
    identifier: String,
}

#[derive(Clone)]
struct AwaitTimeout {
    field: String,
    default_secs: u64,
    max_secs: u64,
}

impl AwaitTimeout {
    fn requested_secs(&self, object: &serde_json::Map<String, Value>) -> u64 {
        object
            .get(&self.field)
            .and_then(Value::as_u64)
            .unwrap_or(self.default_secs)
            .clamp(1, self.max_secs)
    }
}

impl Default for JobArgumentNames {
    fn default() -> Self {
        Self {
            action: "action".into(),
            identifier: "job_id".into(),
        }
    }
}

#[derive(Clone)]
struct MessageArgumentNames {
    job_id: String,
    session_id: String,
    message: String,
    request_id: String,
}

impl Default for MessageArgumentNames {
    fn default() -> Self {
        Self {
            job_id: "job_id".into(),
            session_id: "session_id".into(),
            message: "message".into(),
            request_id: "request_id".into(),
        }
    }
}

/// Settles an inline job if its tool future is dropped (tool timeout, parent
/// stream drop) or unwinds from a panic before the result is recorded.
/// Call [`Self::disarm`] once the result has been written.
pub(crate) struct InlineJobGuard {
    jobs: SubAgentJobRegistry,
    id: SubAgentJobId,
    armed: bool,
}

impl InlineJobGuard {
    pub(crate) fn new(jobs: SubAgentJobRegistry, id: SubAgentJobId) -> Self {
        Self {
            jobs,
            id,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InlineJobGuard {
    fn drop(&mut self) {
        if self.armed {
            self.jobs.mark_aborted(&self.id, std::thread::panicking());
        }
    }
}

impl SubAgentJobRegistry {
    /// Creates an empty asynchronous job registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// The detached-task registry that owns this registry's live state.
    #[cfg(test)]
    pub(crate) fn tasks(&self) -> &tinyagents_tasks::DetachedTaskRegistry<JobMeta, SubAgentJob> {
        &self.tasks
    }

    /// Whether the job still holds a live (unreleased) cancellation token.
    #[cfg(test)]
    pub(crate) fn holds_live_cancellation(&self, id: &SubAgentJobId) -> bool {
        self.tasks
            .holds_cancellation(&TaskId::new(id.as_str()))
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(crate) fn drain_lane_for_test(&self, id: &SubAgentJobId, lane: QueueLane) -> Vec<Message> {
        self.controls()
            .get(id)
            .and_then(|control| control.run_queue.as_ref())
            .map(|queue| queue.drain_now(lane))
            .unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn create(&self, agent: &str, owner: u64) -> (SubAgentJobId, SteeringHandle) {
        self.create_with_cancellation(agent, owner, CancellationToken::new(), JobLink::default())
    }

    /// Registers a job whose child run observes `cancellation`, so
    /// [`Self::cancel_owned`] can stop exactly this job.
    pub(crate) fn create_with_cancellation(
        &self,
        agent: &str,
        owner: u64,
        cancellation: CancellationToken,
        link: JobLink,
    ) -> (SubAgentJobId, SteeringHandle) {
        self.create_with_cancellation_and_queue(agent, owner, cancellation, link, None)
    }

    /// Registers a job with an optional loop-consumed queue for content
    /// messages routed to a specific lane.
    pub(crate) fn create_with_cancellation_and_queue(
        &self,
        agent: &str,
        owner: u64,
        cancellation: CancellationToken,
        link: JobLink,
        run_queue: Option<tinyagents_harness::run_queue::RunQueueHandle>,
    ) -> (SubAgentJobId, SteeringHandle) {
        let id = SubAgentJobId(format!("subagent-job-{}", next_seq()));
        let steering =
            SteeringHandle::new(SteeringPolicy::new().allow(SteeringCommandKind::InjectMessage));
        if let Err(error) = self.insert_job(
            id.clone(),
            agent,
            owner,
            link,
            JobExecutionControls {
                cancellation,
                steering: steering.clone(),
                run_queue,
            },
        ) {
            tracing::error!("{LOG_PREFIX} register.failed job_id={id} error={error}");
        }
        (id, steering)
    }

    fn insert_job(
        &self,
        id: SubAgentJobId,
        agent: &str,
        owner: u64,
        link: JobLink,
        execution: JobExecutionControls,
    ) -> Result<(), SubAgentJobError> {
        if id.as_str().is_empty() {
            return Err(SubAgentJobError::Registry("empty job id".into()));
        }
        let task_id = TaskId::new(id.as_str());
        let job = SubAgentJob {
            id: id.clone(),
            agent: agent.to_owned(),
            status: SubAgentJobStatus::Queued,
            output: None,
            error: None,
            subagent_run_id: link.subagent_run_id,
            parent_tool_call_id: link.parent_tool_call_id,
            incomplete_kind: None,
            artifacts: Vec::new(),
            schema_error: None,
            artifact_error: None,
        };
        let (status, watcher) = watch::channel(job);
        let mut controls = self.controls();
        if controls.contains_key(&id) {
            return Err(SubAgentJobError::AlreadyExists(id.as_str().to_owned()));
        }
        self.steering
            .register(task_id.clone(), execution.steering.clone());
        if let Err(error) = self.tasks.register_cooperative(
            task_id.clone(),
            owner.to_string(),
            JobMeta,
            watcher,
            execution.cancellation.clone(),
        ) {
            self.steering.deregister(&task_id);
            return Err(SubAgentJobError::Registry(error.to_string()));
        }
        controls.insert(
            id.clone(),
            JobControl {
                status,
                cancellation_requested: false,
                run_queue: execution.run_queue,
                steering: execution.steering,
                cancellation: execution.cancellation,
            },
        );
        Ok(())
    }

    /// Registers or reuses the public job bridge for one scoped driver task.
    /// Repeated/coalesced requests reuse the same channels; task ids in a
    /// different root/parent/thread scope receive a distinct public job id.
    pub(crate) fn register_driver_job(
        &self,
        registration: DriverJobRegistration,
    ) -> Result<
        (
            SubAgentJobId,
            SteeringHandle,
            tinyagents_harness::run_queue::RunQueueHandle,
            CancellationToken,
            bool,
        ),
        SubAgentJobError,
    > {
        let DriverJobRegistration {
            key,
            agent,
            owner,
            cancellation,
            steering,
            run_queue,
            continuing,
        } = registration;
        let mut driver_jobs = self
            .driver_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(id) = driver_jobs.get(&key) {
            if self.get_owned(id.as_str(), owner).is_none() {
                return Err(SubAgentJobError::NotFound(id.as_str().to_owned()));
            }
            let controls = self.controls();
            let control = controls
                .get(id)
                .ok_or_else(|| SubAgentJobError::NotFound(id.as_str().to_owned()))?;
            let status = control.status.borrow().status;
            if status.is_terminal() {
                return Ok((
                    id.clone(),
                    control.steering.clone(),
                    control.run_queue.clone().unwrap_or_else(|| {
                        Arc::new(tinyagents_harness::run_queue::RunQueue::new())
                    }),
                    control.cancellation.clone(),
                    false,
                ));
            }
            if control.cancellation_requested {
                return Err(SubAgentJobError::Cancelling(id.as_str().to_owned()));
            }
            let queue = control
                .run_queue
                .clone()
                .ok_or_else(|| SubAgentJobError::LaneUnavailable(id.as_str().to_owned()))?;
            if status == SubAgentJobStatus::AwaitingInput && continuing {
                control.status.send_modify(|job| {
                    job.status = SubAgentJobStatus::Running;
                    job.error = None;
                });
                return Ok((
                    id.clone(),
                    control.steering.clone(),
                    queue,
                    control.cancellation.clone(),
                    true,
                ));
            }
            return Ok((
                id.clone(),
                control.steering.clone(),
                queue,
                control.cancellation.clone(),
                false,
            ));
        }

        let id = SubAgentJobId(format!("subagent-job-{}", next_seq()));
        let queue =
            run_queue.unwrap_or_else(|| Arc::new(tinyagents_harness::run_queue::RunQueue::new()));
        let steering = steering.unwrap_or_else(|| {
            SteeringHandle::new(SteeringPolicy::new().allow(SteeringCommandKind::InjectMessage))
        });
        self.insert_job(
            id.clone(),
            &agent,
            owner,
            JobLink::default(),
            JobExecutionControls {
                cancellation,
                steering: steering.clone(),
                run_queue: Some(queue.clone()),
            },
        )?;
        driver_jobs.insert(key, id.clone());
        let token = self
            .controls()
            .get(&id)
            .expect("inserted control")
            .cancellation
            .clone();
        Ok((id, steering, queue, token, true))
    }

    pub(crate) fn mark_running(&self, id: &SubAgentJobId) {
        if let Some(control) = self.controls().get(id) {
            control.status.send_if_modified(|job| {
                if matches!(
                    job.status,
                    SubAgentJobStatus::Queued | SubAgentJobStatus::AwaitingInput
                ) {
                    job.status = SubAgentJobStatus::Running;
                    true
                } else {
                    false
                }
            });
        }
    }

    /// Applies `settle` to a job that has not yet reached a terminal state,
    /// releasing its cancellation token and steering handle in the same step.
    /// `settle` receives whether cancellation was requested. The first
    /// terminal state wins: a settled job is left untouched and `false` is
    /// returned.
    fn settle(&self, id: &SubAgentJobId, settle: impl FnOnce(&mut SubAgentJob, bool)) -> bool {
        let controls = self.controls();
        let Some(control) = controls.get(id) else {
            return false;
        };
        if control.status.borrow().status.is_terminal() {
            return false;
        }
        let task_id = TaskId::new(id.as_str());
        let _ = self.tasks.release_cancellation_trusted(&task_id);
        let cancellation_requested = control.cancellation_requested;
        control
            .status
            .send_modify(|job| settle(job, cancellation_requested));
        self.steering.deregister(&task_id);
        true
    }

    pub(crate) fn mark_result(
        &self,
        id: &SubAgentJobId,
        result: Result<tinyagents_harness::middleware::AgentRun, TinyAgentsError>,
    ) {
        self.mark_result_applied(id, result, None);
    }

    /// Settles a job like [`Self::mark_result`], publishing the
    /// result-policy-applied output in the same registry write so no reader
    /// ever sees a terminal job with the raw, unpolicied output.
    pub(crate) fn mark_result_applied(
        &self,
        id: &SubAgentJobId,
        result: Result<tinyagents_harness::middleware::AgentRun, TinyAgentsError>,
        applied: Option<AppliedResult>,
    ) {
        let settled = self.settle(id, |job, cancellation_requested| match result {
            Ok(run) => {
                if cancellation_requested {
                    job.status = SubAgentJobStatus::Cancelled;
                    job.error = Some(TinyAgentsError::Cancelled.to_string());
                } else {
                    job.status = SubAgentJobStatus::Completed;
                    job.output = run.text();
                    if let Some(applied) = applied {
                        job.output = Some(applied.text);
                        job.artifacts.extend(applied.artifact);
                        job.schema_error = applied.schema_error;
                        job.artifact_error = applied.artifact_error;
                    }
                }
            }
            Err(TinyAgentsError::Cancelled) => {
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
            }
            Err(error @ TinyAgentsError::LimitExceeded(_)) => {
                job.status = SubAgentJobStatus::Incomplete;
                job.incomplete_kind = Some(IncompleteKind::BudgetExceeded);
                job.error = Some(error.to_string());
            }
            Err(error @ TinyAgentsError::Timeout(_)) => {
                job.status = SubAgentJobStatus::Incomplete;
                job.incomplete_kind = Some(IncompleteKind::Timeout);
                job.error = Some(error.to_string());
            }
            Err(error) => {
                job.status = SubAgentJobStatus::Failed;
                job.error = Some(error.to_string());
            }
        });
        if !settled {
            // Already settled (e.g. cancelled by the owner): the first
            // terminal state wins.
            tracing::debug!("{LOG_PREFIX} mark_result.ignored job_id={id}");
        }
    }

    /// Mirrors a host-driver completion into the same registry used by the
    /// Jobs and Message aliases. Durable pauses remain nonterminal and resumable.
    pub(crate) fn mark_driver_result(
        &self,
        id: &SubAgentJobId,
        result: Result<crate::subagent::SubagentOutcome, String>,
    ) {
        let is_pause = matches!(
            &result,
            Ok(outcome) if matches!(outcome.status, crate::subagent::SubagentOutcomeKind::AwaitingInput(_))
        );
        if is_pause {
            let controls = self.controls();
            if let Some(control) = controls.get(id) {
                let task_id = TaskId::new(id.as_str());
                if control.status.borrow().status.is_terminal() {
                    return;
                } else if control.cancellation_requested {
                    let _ = self.tasks.release_cancellation_trusted(&task_id);
                    control.status.send_modify(|job| {
                        job.status = SubAgentJobStatus::Cancelled;
                        job.error = Some(TinyAgentsError::Cancelled.to_string());
                    });
                    self.steering.deregister(&task_id);
                } else if let Ok(outcome) = result {
                    control.status.send_modify(|job| {
                        job.output = Some(outcome.output);
                        job.artifacts = outcome.artifacts;
                        job.schema_error = outcome.schema_error;
                        job.artifact_error = outcome.artifact_error;
                        job.status = SubAgentJobStatus::AwaitingInput;
                        job.error = None;
                    });
                }
            }
            return;
        }
        self.settle(id, |job, cancellation_requested| {
            if cancellation_requested {
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
                return;
            }
            match result {
                Ok(outcome) => {
                    job.output = Some(outcome.output);
                    job.artifacts = outcome.artifacts;
                    job.schema_error = outcome.schema_error;
                    job.artifact_error = outcome.artifact_error;
                    match outcome.status {
                        crate::subagent::SubagentOutcomeKind::Completed => {
                            job.status = SubAgentJobStatus::Completed;
                        }
                        crate::subagent::SubagentOutcomeKind::AwaitingInput(_) => {
                            unreachable!("pause handled above")
                        }
                        crate::subagent::SubagentOutcomeKind::Incomplete(incomplete) => {
                            job.status = SubAgentJobStatus::Incomplete;
                            job.incomplete_kind = Some(incomplete.kind);
                            job.error = Some(incomplete.reason);
                        }
                        crate::subagent::SubagentOutcomeKind::Cancelled => {
                            job.status = SubAgentJobStatus::Cancelled;
                            job.error = Some(TinyAgentsError::Cancelled.to_string());
                        }
                    }
                }
                Err(error) => {
                    job.status = SubAgentJobStatus::Failed;
                    job.error = Some(error);
                }
            }
        });
    }

    /// Points the job link at the attempt that is now running.
    pub(crate) fn set_attempt_run_id(&self, id: &SubAgentJobId, run_id: &str) {
        if let Some(control) = self.controls().get(id) {
            control.status.send_if_modified(|job| {
                if job.status.is_terminal() {
                    return false;
                }
                job.subagent_run_id = Some(run_id.to_owned());
                true
            });
        }
    }

    /// Settles a job whose run finished but overshot a budget: it keeps the
    /// (policy-applied) output and ends `Incomplete(BudgetExceeded)`.
    pub(crate) fn mark_budget_overrun(
        &self,
        id: &SubAgentJobId,
        applied: AppliedResult,
        reason: String,
    ) {
        self.settle(id, |job, cancellation_requested| {
            if cancellation_requested {
                // An owner cancellation that raced the finish wins, as in
                // `mark_result`.
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
                return;
            }
            job.status = SubAgentJobStatus::Incomplete;
            job.incomplete_kind = Some(IncompleteKind::BudgetExceeded);
            job.error = Some(reason);
            job.output = Some(applied.text);
            job.artifacts.extend(applied.artifact);
            job.schema_error = applied.schema_error;
            job.artifact_error = applied.artifact_error;
        });
    }

    /// Marks a job `Failed` because its child task panicked or was aborted
    /// before it could report a result.
    pub(crate) fn mark_aborted(&self, id: &SubAgentJobId, panicked: bool) {
        self.settle(id, |job, _| {
            tracing::warn!("{LOG_PREFIX} child_task.aborted job_id={id} panicked={panicked}");
            if panicked {
                job.status = SubAgentJobStatus::Failed;
                job.error = Some("subagent job panicked before completing".to_owned());
            } else {
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
            }
        });
    }

    /// Cancels one queued or running job owned by `owner` and marks it
    /// `Cancelled`. The job's own cancellation token is tripped, so the parent
    /// run and sibling jobs are unaffected.
    pub(crate) fn cancel_owned(
        &self,
        job_id: &str,
        owner: u64,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        let id = SubAgentJobId(job_id.to_owned());
        let mut controls = self.controls();
        let control = controls
            .get_mut(&id)
            .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
        let task_id = TaskId::new(job_id);
        let mut snapshot = match self.tasks.cancel_cooperative(&task_id, &owner.to_string()) {
            Ok(snapshot) => snapshot.status,
            Err(DetachedTaskRegistryError::AlreadyDone) => {
                return Err(SubAgentJobError::Terminal {
                    job_id: job_id.to_owned(),
                    status: control.status.borrow().status,
                });
            }
            Err(_) => return Err(SubAgentJobError::NotFound(job_id.to_owned())),
        };
        tracing::debug!("{LOG_PREFIX} cancel_owned job_id={job_id}");
        control.cancellation_requested = true;
        if control.status.borrow().status == SubAgentJobStatus::AwaitingInput {
            let _ = self.tasks.release_cancellation_trusted(&task_id);
            control.status.send_modify(|job| {
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
            });
            self.steering.deregister(&task_id);
            return Ok(control.status.borrow().clone());
        }
        snapshot.error =
            Some("cancellation requested; job will be cancelled when the child unwinds".to_owned());
        Ok(snapshot)
    }

    /// Returns a snapshot for `job_id` when it belongs to `owner`.
    pub(crate) fn get_owned(&self, job_id: &str, owner: u64) -> Option<SubAgentJob> {
        match self
            .tasks
            .snapshot(&TaskId::new(job_id), &owner.to_string())
        {
            Ok(snapshot) => Some(snapshot.status),
            Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => None,
            Err(error) => {
                tracing::warn!(
                    "{LOG_PREFIX} get_owned.registry_error job_id={job_id} error={error}"
                );
                None
            }
        }
    }

    /// Returns a job snapshot for trusted host-side supervision.
    ///
    /// Model-visible tools must use the run-scoped dispatch path instead.
    pub fn get(&self, job_id: &str) -> Option<SubAgentJob> {
        match self.tasks.snapshot_trusted(&TaskId::new(job_id)) {
            Ok(snapshot) => Some(snapshot.status),
            Err(DetachedTaskRegistryError::Unknown) => None,
            Err(error) => {
                tracing::warn!("{LOG_PREFIX} get.registry_error job_id={job_id} error={error}");
                None
            }
        }
    }

    /// Returns this run's jobs in stable id order.
    fn list_owned(&self, owner: u64) -> Vec<SubAgentJob> {
        Self::sorted(self.tasks.snapshots(Some(&owner.to_string())))
    }

    pub(crate) async fn wait_owned(
        &self,
        job_id: &str,
        owner: u64,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        self.get_owned(job_id, owner)
            .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
        let mut status = {
            let controls = self.controls();
            controls
                .get(&SubAgentJobId(job_id.to_owned()))
                .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?
                .status
                .subscribe()
        };
        loop {
            let snapshot = status.borrow().clone();
            if snapshot.status.is_terminal() || snapshot.status == SubAgentJobStatus::AwaitingInput
            {
                return Ok(snapshot);
            }
            if status.changed().await.is_err() {
                return Ok(status.borrow().clone());
            }
        }
    }

    /// Returns every job for trusted host-side supervision.
    ///
    /// Model-visible tools must use the run-scoped dispatch path instead.
    pub fn list(&self) -> Vec<SubAgentJob> {
        Self::sorted(self.tasks.snapshots(None))
    }

    /// Jobs in stable id order, independent of the detached registry's own
    /// iteration order. A registry failure is logged, never silent.
    fn sorted(
        snapshots: Result<
            Vec<tinyagents_tasks::DetachedTaskSnapshot<JobMeta, SubAgentJob>>,
            DetachedTaskRegistryError,
        >,
    ) -> Vec<SubAgentJob> {
        let mut jobs: Vec<SubAgentJob> = match snapshots {
            Ok(snapshots) => snapshots.into_iter().map(|s| s.status).collect(),
            Err(error) => {
                tracing::warn!("{LOG_PREFIX} list.registry_error error={error}");
                Vec::new()
            }
        };
        jobs.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        jobs
    }

    /// Queues a user message for delivery at the running child's next safe
    /// steering checkpoint.
    #[allow(dead_code)]
    pub(crate) fn send_message_owned(
        &self,
        job_id: &str,
        owner: u64,
        message: impl Into<String>,
    ) -> Result<(), SubAgentJobError> {
        self.send_message_with_request_id(job_id, owner, message, None)
            .map(|_| ())
    }

    /// Idempotent [`Self::send_message_owned`]: a `request_id` already applied
    /// to this job is acknowledged (`Ok(true)`, "duplicate") without queueing
    /// the message again. Only the last
    /// [`RecentRequestIds::DEFAULT_CAPACITY`] ids per job are remembered, and a
    /// rejected send (unknown, foreign or terminal job) never consumes its id.
    pub(crate) fn send_message_with_request_id(
        &self,
        job_id: &str,
        owner: u64,
        message: impl Into<String>,
        request_id: Option<&str>,
    ) -> Result<bool, SubAgentJobError> {
        self.send_message_with_lane_and_request_id(job_id, owner, message, request_id, None)
    }

    fn send_message_with_lane_and_request_id(
        &self,
        job_id: &str,
        owner: u64,
        message: impl Into<String>,
        request_id: Option<&str>,
        lane: Option<QueueLane>,
    ) -> Result<bool, SubAgentJobError> {
        let id = SubAgentJobId(job_id.to_owned());
        let controls = self.controls();
        let not_found = || SubAgentJobError::NotFound(job_id.to_owned());
        let control = controls.get(&id).ok_or_else(not_found)?;
        let task_id = TaskId::new(job_id);
        let owner = owner.to_string();
        let job = self
            .tasks
            .snapshot(&task_id, &owner)
            .map_err(|_| not_found())?
            .status;
        if job.status.is_terminal() {
            return Err(SubAgentJobError::Terminal {
                job_id: job_id.to_owned(),
                status: job.status,
            });
        }
        if control.cancellation_requested {
            return Err(SubAgentJobError::Cancelling(job_id.to_owned()));
        }
        let handle = self
            .tasks
            .steering_handle(&task_id, &owner)
            .map_err(|_| not_found())?;
        let lane_queue = if lane.is_some() {
            Some(
                control
                    .run_queue
                    .clone()
                    .ok_or_else(|| SubAgentJobError::LaneUnavailable(job_id.to_owned()))?,
            )
        } else {
            None
        };
        if let Some(request_id) = request_id {
            match self.tasks.claim_steer_request(&task_id, request_id) {
                Ok(false) => {
                    tracing::debug!("{LOG_PREFIX} send_message.duplicate job_id={job_id}");
                    return Ok(true);
                }
                Ok(true) => {}
                Err(DetachedTaskRegistryError::RequestIdTooLong) => {
                    return Err(SubAgentJobError::RequestIdTooLong);
                }
                Err(_) => return Err(not_found()),
            }
        }
        let message = message.into();
        if let Some(lane) = lane {
            lane_queue
                .expect("lane queue was validated before claiming the request id")
                .push_now(lane, Message::user(message));
        } else {
            handle.send(SteeringCommand::InjectMessage(Message::user(message)));
        }
        Ok(false)
    }

    fn controls(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<SubAgentJobId, JobControl>> {
        self.controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Typed host tool that queries a job or lists jobs owned by the requesting run.
#[derive(Clone)]
pub struct SubAgentJobsTool {
    jobs: SubAgentJobRegistry,
    sessions: Option<SessionOverlay>,
    tasks: Option<TaskOverlay>,
    tool_name: String,
    description: String,
    parameters: Option<Value>,
    fixed_action: Option<SubAgentJobAction>,
    argument_names: JobArgumentNames,
    await_timeout: Option<AwaitTimeout>,
    require_identifier: bool,
}

impl SubAgentJobsTool {
    /// Creates the query tool over `jobs`.
    pub fn new(jobs: SubAgentJobRegistry) -> Self {
        Self {
            jobs,
            sessions: None,
            tasks: None,
            tool_name: "subagent_jobs".into(),
            description: "Query an asynchronous subagent job by id, list all subagent jobs, or cancel one job with action \"cancel\".".into(),
            parameters: None,
            fixed_action: None,
            argument_names: JobArgumentNames::default(),
            await_timeout: None,
            require_identifier: false,
        }
    }

    /// Uses a host-facing name while retaining this tool's typed job logic.
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = name.into();
        self
    }

    /// Uses a host-facing description for a named operation alias.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Uses a host-facing schema for a named operation alias.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Binds this alias to one operation, so its schema need not expose the
    /// generic `action` field.
    pub fn with_fixed_action(mut self, action: SubAgentJobAction) -> Self {
        self.fixed_action = Some(action);
        self
    }

    /// Maps a host schema identifier field to the generic job/session id.
    pub fn with_identifier_field(mut self, identifier: impl Into<String>) -> Self {
        self.argument_names.identifier = identifier.into();
        self
    }

    /// Bounds a named await alias using the supplied integer argument field.
    /// Missing or invalid values use `default_secs`; valid values clamp to
    /// `1..=max_secs`.
    pub fn with_await_timeout_field(
        mut self,
        field: impl Into<String>,
        default_secs: u64,
        max_secs: u64,
    ) -> Self {
        let max_secs = max_secs.max(1);
        self.await_timeout = Some(AwaitTimeout {
            field: field.into(),
            default_secs: default_secs.clamp(1, max_secs),
            max_secs,
        });
        self
    }

    /// Requires a job/session identifier for this named alias.
    pub fn with_required_identifier(mut self) -> Self {
        self.require_identifier = true;
        self
    }

    /// Overlays durable sessions in list/query and enables durable close.
    /// `parent_key` maps the live run owner to the host's durable parent scope.
    pub fn with_sessions(
        mut self,
        sessions: Arc<dyn SubagentSessionStore>,
        parent_key: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.sessions = Some((sessions, Arc::new(parent_key)));
        self
    }

    /// Overlays host-managed task records owned by the requesting parent run.
    pub fn with_task_store(
        mut self,
        tasks: Arc<dyn TaskStore>,
        parent_run_id: impl Fn(u64) -> RunId + Send + Sync + 'static,
    ) -> Self {
        self.tasks = Some((tasks, Arc::new(parent_run_id)));
        self
    }
}

#[async_trait]
impl Tool for SubAgentJobsTool {
    fn name(&self) -> &str {
        &self.tool_name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> Value {
        self.parameters.clone().unwrap_or_else(|| json!({
            "type": "object",
            "properties": {
                "job_id": { "type": "string" },
                "action": {
                    "type": "string",
                    "enum": ["query", "cancel", "close", "await"],
                    "description": "`query` reads a job/session or lists work; `cancel` stops a live job; `close` prevents a durable session from being reused; `await` waits for the named live job."
                }
            }
        }))
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let _ = args;
        anyhow::bail!("subagent_jobs requires typed-parent dispatch")
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ToolDispatch<State, Ctx> for SubAgentJobsTool {
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(self.clone())
    }

    async fn execute(
        &self,
        _state: &State,
        _call_id: tinyagents_harness::ids::CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let object = args
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("arguments must be an object"))?;
        let action = match self.fixed_action {
            Some(action) => Some(Some(action.as_str())),
            None => object
                .get(&self.argument_names.action)
                .filter(|value| !value.is_null())
                .map(Value::as_str),
        };
        if self.require_identifier
            && object
                .get(&self.argument_names.identifier)
                .and_then(Value::as_str)
                .is_none()
        {
            anyhow::bail!("a job or session identifier is required");
        }
        match action {
            None | Some(Some("query")) => {}
            Some(Some("cancel")) => {
                let job_id = object
                    .get(&self.argument_names.identifier)
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow::anyhow!("job_id must be a string for action `cancel`")
                    })?;
                let job = self.jobs.cancel_owned(job_id, parent.instance_id())?;
                return Ok(ToolResult::json(serde_json::to_value(job)?));
            }
            Some(Some("close")) => {
                let id = object
                    .get(&self.argument_names.identifier)
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("job_id must be a string for action `close`"))?;
                let (sessions, key) = self
                    .sessions
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("durable sessions are not configured"))?;
                let Some(session) = sessions.get(id).await? else {
                    anyhow::bail!("durable subagent session not found");
                };
                if session.parent_session != key(parent.instance_id()) {
                    anyhow::bail!("durable subagent session not found");
                }
                return Ok(ToolResult::json(
                    json!({"closed": sessions.close(id).await?}),
                ));
            }
            Some(Some("await")) => {
                let Some(id) = object
                    .get(&self.argument_names.identifier)
                    .and_then(Value::as_str)
                else {
                    let mut tombstones = Vec::new();
                    for job in self.jobs.list_owned(parent.instance_id()) {
                        let job = self
                            .jobs
                            .wait_owned(job.id.as_str(), parent.instance_id())
                            .await?;
                        if job.status.is_terminal() {
                            tombstones.push(serde_json::to_value(job)?);
                        }
                    }
                    let awaiting_user = if let Some((sessions, key)) = &self.sessions {
                        sessions.list_for_parent(&key(parent.instance_id()), None).await?.into_iter()
                            .filter(|session| session.status == crate::subagent::DurableSubagentStatus::AwaitingUser)
                        .map(|session| {
                            let question = session.latest_history.as_ref()
                                .and_then(|history| history.last())
                                .map(tinyinference_llm::message::Message::text)
                                .unwrap_or_default();
                            let envelope = crate::subagent::SubagentAwaitingUser {
                                task_id: session.current_task_id.clone().unwrap_or_default(),
                                agent_id: session.agent_id.clone(),
                                worker_thread_id: session.worker_thread_id.clone(),
                                question,
                                checkpointed: true,
                            };
                            json!({"session_id":session.subagent_session_id,"task_id":session.current_task_id,"status":"awaiting_user","history":session.latest_history,"awaiting_user_envelope":envelope.render()})
                        })
                            .collect()
                    } else {
                        Vec::<Value>::new()
                    };
                    return Ok(ToolResult::json(
                        json!({"collected_tombstones":tombstones,"awaiting_user":awaiting_user}),
                    ));
                };
                if self.jobs.get_owned(id, parent.instance_id()).is_some() {
                    let job = if let Some(timeout) = &self.await_timeout {
                        let duration =
                            std::time::Duration::from_secs(timeout.requested_secs(object));
                        match tokio::time::timeout(
                            duration,
                            self.jobs.wait_owned(id, parent.instance_id()),
                        )
                        .await
                        {
                            Ok(result) => result?,
                            Err(_) => self
                                .jobs
                                .get_owned(id, parent.instance_id())
                                .ok_or_else(|| SubAgentJobError::NotFound(id.to_owned()))?,
                        }
                    } else {
                        self.jobs.wait_owned(id, parent.instance_id()).await?
                    };
                    return Ok(ToolResult::json(serde_json::to_value(job)?));
                }
                if let Some((sessions, key)) = &self.sessions
                    && let Some(session) = sessions
                        .get(id)
                        .await?
                        .filter(|session| session.parent_session == key(parent.instance_id()))
                {
                    let awaiting_user =
                        session.status == crate::subagent::DurableSubagentStatus::AwaitingUser;
                    let question = session
                        .latest_history
                        .as_ref()
                        .and_then(|history| history.last())
                        .map(tinyinference_llm::message::Message::text)
                        .unwrap_or_default();
                    let envelope = crate::subagent::SubagentAwaitingUser {
                        task_id: session.current_task_id.clone().unwrap_or_default(),
                        agent_id: session.agent_id.clone(),
                        worker_thread_id: session.worker_thread_id.clone(),
                        question,
                        checkpointed: true,
                    };
                    return Ok(ToolResult::json(json!({
                        "session_id": session.subagent_session_id,
                        "task_id": session.current_task_id,
                        "status": session.status,
                        "awaiting_user_envelope": awaiting_user.then_some(envelope.render()),
                        "latest_error": session.latest_error,
                    })));
                }
                return Err(SubAgentJobError::NotFound(id.to_owned()).into());
            }
            Some(_) => anyhow::bail!("action must be `query`, `cancel`, `close`, or `await`"),
        }
        if let Some(value) = object.get(&self.argument_names.identifier) {
            let job_id = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("job_id must be a string when provided"))?;
            let job = self.jobs.get_owned(job_id, parent.instance_id());
            if let Some(job) = job {
                return Ok(ToolResult::json(serde_json::to_value(job)?));
            }
            let (sessions, key) = self
                .sessions
                .as_ref()
                .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
            let session = sessions
                .get(job_id)
                .await?
                .filter(|session| session.parent_session == key(parent.instance_id()))
                .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
            Ok(ToolResult::json(serde_json::to_value(
                DurableSubagentSessionSummary::from(&session),
            )?))
        } else {
            let mut rows = self
                .jobs
                .list_owned(parent.instance_id())
                .into_iter()
                .map(|job| {
                    serde_json::to_value(job).map(|mut v| {
                        v["kind"] = Value::String("job".into());
                        v
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some((sessions, key)) = &self.sessions {
                let parent_key = key(parent.instance_id());
                rows.extend(
                    sessions
                        .list_for_parent(&parent_key, None)
                        .await?
                        .iter()
                        .map(|session| {
                            let mut value =
                                serde_json::to_value(DurableSubagentSessionSummary::from(session))
                                    .unwrap_or(Value::Null);
                            value["kind"] = Value::String("session".into());
                            value
                        }),
                );
            }
            if let Some((tasks, parent_run_id)) = &self.tasks {
                let filter = OrchestrationTaskFilter {
                    parent_run_id: Some(parent_run_id(parent.instance_id())),
                    ..OrchestrationTaskFilter::default()
                };
                rows.extend(tasks.list(filter).into_iter().map(|task| {
                    let mut value = serde_json::to_value(task).unwrap_or(Value::Null);
                    value["kind"] = Value::String("task".into());
                    value
                }));
            }
            Ok(ToolResult::json(Value::Array(rows)))
        }
    }
}

/// Host tool that sends a message to a queued or running subagent job.
#[derive(Clone)]
pub struct SubAgentMessageTool {
    jobs: SubAgentJobRegistry,
    sessions: Option<SessionOverlay>,
    tool_name: String,
    description: String,
    parameters: Option<Value>,
    argument_names: MessageArgumentNames,
    lane: Option<QueueLane>,
    lane_field: Option<String>,
}

impl SubAgentMessageTool {
    /// Creates the message tool over `jobs`.
    pub fn new(jobs: SubAgentJobRegistry) -> Self {
        Self {
            jobs,
            sessions: None,
            tool_name: "subagent_message".into(),
            description: "Send a message to a queued or running asynchronous subagent job.".into(),
            parameters: None,
            argument_names: MessageArgumentNames::default(),
            lane: None,
            lane_field: None,
        }
    }

    /// Uses a host-facing name while retaining this tool's typed message logic.
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.tool_name = name.into();
        self
    }

    /// Uses a host-facing description for a named steering alias.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Uses a host-facing schema for a named steering alias.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Maps host schema fields to the generic job/session and message fields.
    pub fn with_argument_fields(
        mut self,
        job_id: impl Into<String>,
        session_id: impl Into<String>,
        message: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        self.argument_names = MessageArgumentNames {
            job_id: job_id.into(),
            session_id: session_id.into(),
            message: message.into(),
            request_id: request_id.into(),
        };
        self
    }

    /// Binds a named message operation to one real job queue lane.
    ///
    /// The generic `subagent_message` tool leaves this unset and preserves its
    /// historical unframed steer behavior.
    pub fn with_lane(mut self, lane: QueueLane) -> Self {
        self.lane = Some(lane);
        self
    }

    /// Reads a lane field from an alias schema. `"collect"` routes to the
    /// collect lane; missing or unknown values preserve the host's default
    /// steer behavior.
    pub fn with_lane_field(mut self, field: impl Into<String>) -> Self {
        self.lane_field = Some(field.into());
        self
    }

    /// Enables steering a live durable session by its session id.
    pub fn with_sessions(
        mut self,
        sessions: Arc<dyn SubagentSessionStore>,
        parent_key: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.sessions = Some((sessions, Arc::new(parent_key)));
        self
    }
}

#[async_trait]
impl Tool for SubAgentMessageTool {
    fn name(&self) -> &str {
        &self.tool_name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> Value {
        self.parameters.clone().unwrap_or_else(|| json!({
            "type": "object",
            "properties": {
                "job_id": { "type": "string", "description": "Live job id. Use session_id for a durable reusable session." },
                "session_id": { "type": "string", "description": "Durable reusable session id." },
                "message": { "type": "string" },
                "request_id": {
                    "type": "string",
                    "description": "Optional idempotency key: resending the same request_id to the same job does not queue the message again while it is among the most recent 64 request ids remembered for that job (older ids are evicted); ids over 128 bytes are rejected."
                }
            },
            "required": ["message"],
            "oneOf": [{"required":["job_id"]},{"required":["session_id"]}]
        }))
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let _ = args;
        anyhow::bail!("subagent_message requires typed-parent dispatch")
    }
}

#[async_trait]
impl<State: Send + Sync, Ctx: Send + Sync> ToolDispatch<State, Ctx> for SubAgentMessageTool {
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(self.clone())
    }

    async fn execute(
        &self,
        _state: &State,
        _call_id: tinyagents_harness::ids::CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let job_id = if let Some(session_id) = args
            .get(&self.argument_names.session_id)
            .and_then(Value::as_str)
        {
            let (sessions, key) = self
                .sessions
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("durable sessions are not configured"))?;
            let session = sessions
                .get(session_id)
                .await?
                .filter(|session| session.parent_session == key(parent.instance_id()))
                .ok_or_else(|| anyhow::anyhow!("durable subagent session not found"))?;
            session
                .current_task_id
                .ok_or_else(|| anyhow::anyhow!("durable subagent session has no running task"))?
        } else {
            args.get(&self.argument_names.job_id)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("job_id or session_id must be a string"))?
                .to_owned()
        };
        let message = args
            .get(&self.argument_names.message)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("message must be a string"))?;
        let request_id = match args.get(&self.argument_names.request_id) {
            None | Some(Value::Null) => None,
            Some(value) => {
                let id = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("request_id must be a string when provided"))?;
                Some(id)
            }
        };
        let lane = self.lane.or_else(|| {
            self.lane_field.as_ref().map(|field| {
                if args.get(field).and_then(Value::as_str) == Some("collect") {
                    QueueLane::Collect
                } else {
                    QueueLane::Steer
                }
            })
        });
        let duplicate = self.jobs.send_message_with_lane_and_request_id(
            &job_id,
            parent.instance_id(),
            message,
            request_id,
            lane,
        )?;
        let mut payload = json!({
            "job_id": job_id,
            "status": "message_queued"
        });
        if duplicate {
            payload["duplicate"] = Value::Bool(true);
        }
        Ok(ToolResult::json(payload))
    }
}

/// Registers the standard run-scoped query and message tools in a harness registry.
pub fn register_subagent_job_tools<State: Send + Sync, Ctx: Send + Sync>(
    registry: &mut ToolRegistry<State, Ctx>,
    jobs: SubAgentJobRegistry,
) -> &mut ToolRegistry<State, Ctx> {
    registry
        .register_dispatch(Arc::new(SubAgentJobsTool::new(jobs.clone())))
        .register_dispatch(Arc::new(SubAgentMessageTool::new(jobs)))
}
