//! Asynchronous subagent job registry and host-facing control tools.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::{TaskId, next_seq};
use tinyagents_harness::run_queue::QueueLane;
use tinyagents_harness::steering::{
    SteeringCommand, SteeringCommandKind, SteeringHandle, SteeringPolicy,
};
use tinyagents_tasks::DetachedTaskRegistryError;
use tinyinference_llm::message::Message;
use tokio::sync::watch;

use crate::subagent::{AppliedResult, IncompleteKind};

use super::types::{DriverJobRegistration, JobControl, JobMeta, ParentJobScope};
use super::{
    JobLink, SubAgentJob, SubAgentJobError, SubAgentJobId, SubAgentJobRegistry, SubAgentJobStatus,
};

const LOG_PREFIX: &str = "[subagent-jobs]";

fn registry_failure(error: DetachedTaskRegistryError) -> SubAgentJobError {
    SubAgentJobError::Registry(error.to_string())
}

struct JobExecutionControls {
    cancellation: CancellationToken,
    steering: SteeringHandle,
    run_queue: Option<tinyagents_harness::run_queue::RunQueueHandle>,
}

struct JobWaitGuard {
    jobs: SubAgentJobRegistry,
    waiters: Arc<AtomicUsize>,
}

impl Drop for JobWaitGuard {
    fn drop(&mut self) {
        let previous = self.waiters.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 {
            let _ = self.jobs.prune_terminal_over_cap();
        }
    }
}

/// Settles an inline job if its tool future is dropped (tool timeout, parent
/// stream drop) or unwinds from a panic before the result is recorded.
/// Call [`Self::disarm`] once the result has been written.
pub struct InlineJobGuard {
    jobs: SubAgentJobRegistry,
    id: SubAgentJobId,
    armed: bool,
}

impl InlineJobGuard {
    pub fn new(jobs: SubAgentJobRegistry, id: SubAgentJobId) -> Self {
        Self {
            jobs,
            id,
            armed: true,
        }
    }

    pub fn disarm(&mut self) {
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
        let mut driver_jobs = self
            .driver_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = self.insert_job(
            id.clone(),
            agent,
            owner.to_string(),
            link,
            JobExecutionControls {
                cancellation,
                steering: steering.clone(),
                run_queue,
            },
            &mut driver_jobs,
        ) {
            tracing::error!("{LOG_PREFIX} register.failed job_id={id} error={error}");
        }
        (id, steering)
    }

    fn insert_job(
        &self,
        id: SubAgentJobId,
        agent: &str,
        owner: String,
        link: JobLink,
        execution: JobExecutionControls,
        driver_jobs: &mut std::collections::HashMap<
            crate::subagent::SubagentTaskKey,
            SubAgentJobId,
        >,
    ) -> Result<(), SubAgentJobError> {
        if id.as_str().is_empty() {
            return Err(SubAgentJobError::Registry("empty job id".into()));
        }
        let metadata = JobMeta {
            parent_thread_id: link.parent_thread_id.clone(),
            workspace: link.workspace.clone(),
        };
        let task_id = TaskId::new(id.as_str());
        let job = SubAgentJob {
            id: id.clone(),
            agent: agent.to_owned(),
            status: SubAgentJobStatus::Queued,
            output: None,
            error: None,
            subagent_run_id: link.subagent_run_id,
            worker_thread_id: None,
            extensions: std::collections::BTreeMap::new(),
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
        self.prune_terminal_before_insert(driver_jobs, &mut controls)?;
        self.steering
            .register(task_id.clone(), execution.steering.clone());
        if let Err(error) = self.tasks.register_cooperative_managed(
            task_id.clone(),
            owner,
            metadata,
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
                waiters: Arc::new(AtomicUsize::new(0)),
                terminal_sequence: AtomicU64::new(0),
            },
        );
        Ok(())
    }

    /// Prunes unobserved terminal jobs before the generic registry reaches its
    /// finite soft cap. The shared runtime entry, control sender and driver key
    /// are removed together; live jobs and jobs with current waiters survive.
    fn prune_terminal_before_insert(
        &self,
        driver_jobs: &mut std::collections::HashMap<
            crate::subagent::SubagentTaskKey,
            SubAgentJobId,
        >,
        controls: &mut std::sync::MutexGuard<
            '_,
            std::collections::HashMap<SubAgentJobId, JobControl>,
        >,
    ) -> Result<(), SubAgentJobError> {
        self.prune_terminal_over_cap_locked(driver_jobs, controls)
    }

    /// Keeps unpinned terminal history within the soft cap while allowing
    /// live work and terminal jobs pinned by waiters to exceed it temporarily.
    fn prune_terminal_over_cap(&self) -> Result<(), SubAgentJobError> {
        let mut driver_jobs = self
            .driver_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut controls = self.controls();
        self.prune_terminal_over_cap_locked(&mut driver_jobs, &mut controls)
    }

    fn prune_terminal_over_cap_locked(
        &self,
        driver_jobs: &mut std::collections::HashMap<
            crate::subagent::SubagentTaskKey,
            SubAgentJobId,
        >,
        controls: &mut std::sync::MutexGuard<
            '_,
            std::collections::HashMap<SubAgentJobId, JobControl>,
        >,
    ) -> Result<(), SubAgentJobError> {
        let terminal_count = controls
            .values()
            .filter(|control| {
                control.status.borrow().status.is_terminal()
                    && control.waiters.load(Ordering::Acquire) == 0
            })
            .count();
        let excess = terminal_count.saturating_sub(super::types::SUBAGENT_JOB_SOFT_CAP);
        if excess == 0 {
            return Ok(());
        }

        let mut candidates: Vec<_> = controls
            .iter()
            .filter(|(_, control)| {
                control.status.borrow().status.is_terminal()
                    && control.waiters.load(Ordering::Acquire) == 0
            })
            .map(|(id, control)| {
                (
                    control.terminal_sequence.load(Ordering::Acquire),
                    id.clone(),
                )
            })
            .collect();
        candidates.sort_by_key(|(sequence, _)| *sequence);
        let mut removed = std::collections::HashSet::with_capacity(candidates.len());
        for (_, id) in candidates.into_iter().take(excess) {
            let task_id = TaskId::new(id.as_str());
            if self
                .tasks
                .prune_terminal_trusted(&task_id)
                .map_err(registry_failure)?
                .is_some()
            {
                controls.remove(&id);
                removed.insert(id);
            }
        }
        if !removed.is_empty() {
            driver_jobs.retain(|_, mapped_id| !removed.contains(mapped_id));
        }
        Ok(())
    }

    /// Registers or reuses the public job bridge for one scoped driver task.
    /// Repeated/coalesced requests reuse the same channels; task ids in a
    /// different root/parent/thread scope receive a distinct public job id.
    pub fn register_driver_job(
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
            workspace,
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
            if self.get_owned(id.as_str(), &owner).is_none() {
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
            JobLink {
                parent_thread_id: key.thread_id.clone(),
                workspace,
                ..JobLink::default()
            },
            JobExecutionControls {
                cancellation,
                steering: steering.clone(),
                run_queue: Some(queue.clone()),
            },
            &mut driver_jobs,
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

    pub fn mark_running(&self, id: &SubAgentJobId) {
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
        let settled = {
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
            if control.status.borrow().status.is_terminal() {
                self.stamp_terminal_sequence(control);
            }
            self.steering.deregister(&task_id);
            true
        };
        if settled {
            let _ = self.prune_terminal_over_cap();
        }
        settled
    }

    fn stamp_terminal_sequence(&self, control: &JobControl) {
        if control.terminal_sequence.load(Ordering::Acquire) == 0 {
            let sequence = self.next_terminal_sequence.fetch_add(1, Ordering::Relaxed);
            let _ = control.terminal_sequence.compare_exchange(
                0,
                sequence,
                Ordering::Release,
                Ordering::Relaxed,
            );
        }
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
    pub fn mark_driver_result(
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
                    self.stamp_terminal_sequence(control);
                    self.steering.deregister(&task_id);
                    drop(controls);
                    let _ = self.prune_terminal_over_cap();
                } else if let Ok(outcome) = result {
                    control.status.send_modify(|job| {
                        job.output = Some(outcome.output);
                        job.worker_thread_id = outcome.worker_thread_id;
                        let task_id = outcome.task_id;
                        job.extensions = outcome.extensions;
                        job.extensions
                            .insert("task_id".into(), serde_json::Value::String(task_id));
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
                    job.worker_thread_id = outcome.worker_thread_id;
                    let task_id = outcome.task_id;
                    job.extensions = outcome.extensions;
                    job.extensions
                        .insert("task_id".into(), serde_json::Value::String(task_id));
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

    /// Finds a driver-backed public job by its host-stable task id, while
    /// enforcing the same owner scope as direct job-id lookups.
    pub fn driver_job_for_key(
        &self,
        key: &crate::subagent::SubagentTaskKey,
        owner: &str,
    ) -> Option<SubAgentJobId> {
        let candidates = self
            .driver_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(candidate, _)| *candidate == key)
            .map(|(_, id)| id.clone())
            .collect::<Vec<_>>();
        candidates
            .into_iter()
            .find(|id| self.get_owned(id.as_str(), owner).is_some())
    }

    /// Requests cooperative cancellation for a driver-backed job identified
    /// by its full host lifecycle key and owner scope.
    pub fn cancel_key_owned(
        &self,
        key: &crate::subagent::SubagentTaskKey,
        owner: impl ToString,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        let owner = owner.to_string();
        let id = self
            .driver_job_for_key(key, &owner)
            .ok_or_else(|| SubAgentJobError::NotFound(key.task_id.clone()))?;
        self.cancel_owned(id.as_str(), owner)
    }

    /// Cancels a driver-backed job and immediately tombstones its public
    /// completion record. The router reservation suppresses both an already
    /// pending result and a late result arriving after this call. A foreign
    /// owner is rejected before the router is touched.
    pub fn cancel_key_owned_and_tombstone(
        &self,
        key: &crate::subagent::SubagentTaskKey,
        owner: impl ToString,
        completions: &tinyagents_tasks::CompletionRouter,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        let owner = owner.to_string();
        let id = self
            .driver_job_for_key(key, &owner)
            .ok_or_else(|| SubAgentJobError::NotFound(key.task_id.clone()))?;
        self.cancel_owned_with_change(
            id.as_str(),
            owner,
            Some((completions, key.task_id.as_str())),
        )
        .map(|(job, _)| job)
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
    pub fn cancel_owned(
        &self,
        job_id: &str,
        owner: impl ToString,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        self.cancel_owned_with_change(job_id, owner, None)
            .map(|(job, _)| job)
    }

    fn cancel_owned_with_change(
        &self,
        job_id: &str,
        owner: impl ToString,
        completion_tombstone: Option<(&tinyagents_tasks::CompletionRouter, &str)>,
    ) -> Result<(SubAgentJob, bool), SubAgentJobError> {
        let owner = owner.to_string();
        let id = SubAgentJobId(job_id.to_owned());
        let task_id = TaskId::new(job_id);
        match self.tasks.snapshot(&task_id, &owner) {
            Ok(_) => {}
            Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => {
                return Err(SubAgentJobError::NotFound(job_id.to_owned()));
            }
            Err(error) => return Err(registry_failure(error)),
        }
        let mut controls = self.controls();
        let control = controls
            .get_mut(&id)
            .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
        if control.status.borrow().status.is_terminal()
            && (completion_tombstone.is_none() || !control.cancellation_requested)
        {
            return Err(SubAgentJobError::Terminal {
                job_id: job_id.to_owned(),
                status: control.status.borrow().status,
            });
        }
        if let Some((completions, task_id)) = completion_tombstone {
            completions
                .tombstone(task_id)
                .map_err(|error| SubAgentJobError::Registry(error.to_string()))?;
        }
        if control.cancellation_requested {
            if completion_tombstone.is_some() && !control.status.borrow().status.is_terminal() {
                let task_id = TaskId::new(job_id);
                let _ = self.tasks.release_cancellation_trusted(&task_id);
                control.status.send_modify(|job| {
                    job.status = SubAgentJobStatus::Cancelled;
                    job.error = Some(TinyAgentsError::Cancelled.to_string());
                });
                self.stamp_terminal_sequence(control);
                self.steering.deregister(&task_id);
                let snapshot = control.status.borrow().clone();
                drop(controls);
                let _ = self.prune_terminal_over_cap();
                return Ok((snapshot, true));
            }
            return Ok((control.status.borrow().clone(), false));
        }
        let mut snapshot = match self.tasks.cancel_cooperative(&task_id, &owner) {
            Ok(snapshot) => snapshot.status,
            Err(DetachedTaskRegistryError::AlreadyDone) => {
                return Err(SubAgentJobError::Terminal {
                    job_id: job_id.to_owned(),
                    status: control.status.borrow().status,
                });
            }
            Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => {
                return Err(SubAgentJobError::NotFound(job_id.to_owned()));
            }
            Err(error) => return Err(registry_failure(error)),
        };
        tracing::debug!("{LOG_PREFIX} cancel_owned job_id={job_id}");
        control.cancellation_requested = true;
        if completion_tombstone.is_some()
            || control.status.borrow().status == SubAgentJobStatus::AwaitingInput
        {
            let _ = self.tasks.release_cancellation_trusted(&task_id);
            control.status.send_modify(|job| {
                job.status = SubAgentJobStatus::Cancelled;
                job.error = Some(TinyAgentsError::Cancelled.to_string());
            });
            self.stamp_terminal_sequence(control);
            self.steering.deregister(&task_id);
            let snapshot = control.status.borrow().clone();
            drop(controls);
            let _ = self.prune_terminal_over_cap();
            return Ok((snapshot, true));
        }
        snapshot.error =
            Some("cancellation requested; job will be cancelled when the child unwinds".to_owned());
        Ok((snapshot, true))
    }

    /// Requests cancellation for jobs belonging to the exact parent workspace
    /// and thread. Jobs with incomplete metadata never match this scoped call.
    pub fn cancel_for_thread(&self, scope: &ParentJobScope) -> Result<usize, SubAgentJobError> {
        self.cancel_matching(|metadata| {
            metadata.parent_thread_id.as_deref() == Some(scope.thread_id.as_str())
                && metadata.workspace.as_ref() == Some(&scope.workspace)
        })
    }

    /// Requests cancellation for every live job in one exact workspace.
    /// Jobs with incomplete workspace metadata never match this scoped call.
    pub fn cancel_all_for_workspace(
        &self,
        workspace: &tinytools::WorkspaceDescriptor,
    ) -> Result<usize, SubAgentJobError> {
        self.cancel_matching(|metadata| metadata.workspace.as_ref() == Some(workspace))
    }

    /// Requests cancellation for every live job while retaining each public
    /// job record for the normal settlement path. This is trusted process-wide
    /// supervision; tenant controls should use [`Self::cancel_all_for_workspace`].
    pub fn cancel_all(&self) -> Result<usize, SubAgentJobError> {
        self.cancel_matching(|_| true)
    }

    fn cancel_matching(
        &self,
        matches: impl Fn(&JobMeta) -> bool,
    ) -> Result<usize, SubAgentJobError> {
        let ids = self.controls().keys().cloned().collect::<Vec<_>>();
        let mut requested = 0;
        for id in ids {
            let task_id = TaskId::new(id.as_str());
            let snapshot = match self.tasks.snapshot_trusted(&task_id) {
                Ok(snapshot) if !snapshot.status.status.is_terminal() => snapshot,
                Ok(_) | Err(DetachedTaskRegistryError::Unknown) => continue,
                Err(error) => return Err(registry_failure(error)),
            };
            if !matches(&snapshot.metadata) {
                continue;
            }
            match self.cancel_owned_with_change(id.as_str(), snapshot.owner_id, None) {
                Ok((_, changed)) => requested += usize::from(changed),
                Err(SubAgentJobError::Terminal { .. } | SubAgentJobError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(requested)
    }

    /// Returns a snapshot for `job_id` when it belongs to `owner`.
    pub fn get_owned(&self, job_id: &str, owner: impl ToString) -> Option<SubAgentJob> {
        self.try_get_owned(job_id, owner).ok().flatten()
    }

    /// Returns a snapshot for `job_id` when it belongs to `owner`, preserving
    /// registry failures separately from unknown or foreign identifiers.
    pub fn try_get_owned(
        &self,
        job_id: &str,
        owner: impl ToString,
    ) -> Result<Option<SubAgentJob>, SubAgentJobError> {
        match self
            .tasks
            .snapshot(&TaskId::new(job_id), &owner.to_string())
        {
            Ok(snapshot) => Ok(Some(snapshot.status)),
            Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => {
                Ok(None)
            }
            Err(error) => Err(registry_failure(error)),
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

    /// Returns this run's jobs while preserving detached-registry failures.
    pub fn try_list_owned(
        &self,
        owner: impl ToString,
    ) -> Result<Vec<SubAgentJob>, SubAgentJobError> {
        let snapshots = self
            .tasks
            .snapshots(Some(&owner.to_string()))
            .map_err(registry_failure)?;
        Ok(Self::sorted(Ok(snapshots)))
    }

    pub async fn wait_owned(
        &self,
        job_id: &str,
        owner: impl ToString,
    ) -> Result<SubAgentJob, SubAgentJobError> {
        let owner = owner.to_string();
        let id = SubAgentJobId(job_id.to_owned());
        let task_id = TaskId::new(job_id);
        let (mut status, waiters) = {
            // Pin the control before releasing its lock so cap pruning cannot
            // remove a terminal snapshot between ownership validation and the
            // watch subscription.
            let controls = self.controls();
            match self.tasks.snapshot(&task_id, &owner) {
                Ok(_) => {}
                Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => {
                    return Err(SubAgentJobError::NotFound(job_id.to_owned()));
                }
                Err(error) => return Err(registry_failure(error)),
            }
            let control = controls
                .get(&id)
                .ok_or_else(|| SubAgentJobError::NotFound(job_id.to_owned()))?;
            control.waiters.fetch_add(1, Ordering::AcqRel);
            (control.status.subscribe(), control.waiters.clone())
        };
        let waiter_guard = JobWaitGuard {
            jobs: self.clone(),
            waiters,
        };
        loop {
            let snapshot = status.borrow().clone();
            if snapshot.status.is_terminal() || snapshot.status == SubAgentJobStatus::AwaitingInput
            {
                drop(waiter_guard);
                return Ok(snapshot);
            }
            if status.changed().await.is_err() {
                let snapshot = status.borrow().clone();
                drop(waiter_guard);
                return Ok(snapshot);
            }
        }
    }

    /// Returns every job for trusted host-side supervision.
    ///
    /// Model-visible tools must use the run-scoped dispatch path instead.
    pub fn list(&self) -> Vec<SubAgentJob> {
        Self::sorted(self.tasks.snapshots(None))
    }

    /// Returns live jobs for one exact owner and parent workspace/thread.
    /// Incomplete parent metadata never matches this scoped roster query.
    pub fn list_for_parent_scope(
        &self,
        owner: &str,
        scope: &ParentJobScope,
    ) -> Result<Vec<SubAgentJob>, SubAgentJobError> {
        let snapshots = self
            .tasks
            .snapshots(Some(owner))
            .map_err(registry_failure)?;
        let driver_jobs = self
            .driver_jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut jobs: Vec<SubAgentJob> = snapshots
            .into_iter()
            .filter(|snapshot| {
                snapshot.metadata.workspace.as_ref() == Some(&scope.workspace)
                    && snapshot.metadata.parent_thread_id.as_deref()
                        == Some(scope.thread_id.as_str())
            })
            .map(|snapshot| {
                let mut job = snapshot.status;
                if !job.extensions.contains_key("task_id")
                    && let Some(key) = driver_jobs
                        .iter()
                        .find_map(|(key, id)| (id == &job.id).then_some(key))
                {
                    job.extensions.insert(
                        "task_id".into(),
                        serde_json::Value::String(key.task_id.clone()),
                    );
                }
                job
            })
            .collect();
        jobs.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        Ok(jobs)
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
    pub fn send_message_owned(
        &self,
        job_id: &str,
        owner: impl ToString,
        message: impl Into<String>,
    ) -> Result<(), SubAgentJobError> {
        self.send_message_with_request_id(job_id, owner, message, None)
            .map(|_| ())
    }

    /// Idempotent [`Self::send_message_owned`]: a `request_id` already applied
    /// to this job is acknowledged (`Ok(true)`, "duplicate") without queueing
    /// the message again. Only the last
    /// `RecentRequestIds::DEFAULT_CAPACITY` ids per job are remembered, and a
    /// rejected send (unknown, foreign or terminal job) never consumes its id.
    pub fn send_message_with_request_id(
        &self,
        job_id: &str,
        owner: impl ToString,
        message: impl Into<String>,
        request_id: Option<&str>,
    ) -> Result<bool, SubAgentJobError> {
        self.send_message_with_lane_and_request_id(job_id, owner, message, request_id, None)
    }

    pub fn send_message_with_lane_and_request_id(
        &self,
        job_id: &str,
        owner: impl ToString,
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
        let job = match self.tasks.snapshot(&task_id, &owner) {
            Ok(snapshot) => snapshot.status,
            Err(DetachedTaskRegistryError::Unknown | DetachedTaskRegistryError::NotOwned) => {
                return Err(not_found());
            }
            Err(error) => return Err(registry_failure(error)),
        };
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
