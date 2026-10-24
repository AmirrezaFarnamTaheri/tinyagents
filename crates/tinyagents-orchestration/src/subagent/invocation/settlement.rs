//! Bounded settlement for native driver-job descendants.

use super::jobs::JobWaitGuard;
use super::*;

/// Keeps one owner's retained root subtree available across child completion
/// and bounded settlement. Acquire it before the root run admits children.
#[must_use = "dropping the lease releases retained job history"]
pub struct RootRunDescendantLease {
    jobs: SubAgentJobRegistry,
    id: u64,
    root_run_id: tinyagents_harness::ids::RunId,
    owner: String,
}

impl RootRunDescendantLease {
    /// Waits for every registered descendant to become terminal. The caller
    /// must close direct root admissions before invoking this method. The
    /// lease is consumed and released on success,
    /// timeout, cancellation, or future drop.
    pub async fn settle(
        self,
        deadline: tokio::time::Instant,
        cancellation: &tinyagents_harness::cancel::CancellationToken,
    ) -> std::result::Result<Vec<SubAgentJob>, SubAgentJobError> {
        self.jobs
            .settle_root_run_descendants_inner(
                &self.root_run_id,
                &self.owner,
                deadline,
                cancellation,
            )
            .await
    }
}

impl Drop for RootRunDescendantLease {
    fn drop(&mut self) {
        self.jobs.release_root_descendant_lease(self.id);
    }
}

impl SubAgentJobRegistry {
    /// Starts retaining one owner's root subtree. Acquire this before the root
    /// run starts admitting driver jobs, then call [`RootRunDescendantLease::settle`]
    /// after its direct admissions are closed. A new lease captures currently
    /// retained history and descendants admitted while the lease is active.
    pub fn retain_root_run_descendants(
        &self,
        root_run_id: &tinyagents_harness::ids::RunId,
        owner: impl ToString,
    ) -> std::result::Result<RootRunDescendantLease, SubAgentJobError> {
        let id = self
            .next_root_lease_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let owner = owner.to_string();
        let mut root_leases = self.root_leases();
        let mut controls = self.controls();
        let mut reachable_runs = std::collections::HashSet::new();
        reachable_runs.insert(root_run_id.clone());
        let mut record = super::types::RootLeaseRecord {
            owner: owner.clone(),
            reachable_runs,
            pinned_jobs: std::collections::HashSet::new(),
        };
        Self::extend_root_lease_locked(&mut record, &mut controls);
        root_leases.insert(id, record);
        drop(controls);
        drop(root_leases);
        Ok(RootRunDescendantLease {
            jobs: self.clone(),
            id,
            root_run_id: root_run_id.clone(),
            owner,
        })
    }

    pub(super) fn refresh_root_leases_locked(
        &self,
        root_leases: &mut std::collections::HashMap<u64, super::types::RootLeaseRecord>,
        controls: &mut std::collections::HashMap<SubAgentJobId, super::types::JobControl>,
    ) {
        for lease in root_leases.values_mut() {
            Self::extend_root_lease_locked(lease, controls);
        }
    }

    fn extend_root_lease_locked(
        lease: &mut super::types::RootLeaseRecord,
        controls: &mut std::collections::HashMap<SubAgentJobId, super::types::JobControl>,
    ) {
        let mut pending_runs = lease.reachable_runs.iter().cloned().collect::<Vec<_>>();
        let mut seen_runs = std::collections::HashSet::new();
        while let Some(run_id) = pending_runs.pop() {
            if !seen_runs.insert(run_id.clone()) {
                continue;
            }
            for (id, control) in controls.iter_mut() {
                if control.owner != lease.owner || !control.lineage.parents.contains(&run_id) {
                    continue;
                }
                if lease.pinned_jobs.insert(id.clone()) {
                    control
                        .waiters
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }
                for child_run_id in &control.lineage.children {
                    if lease.reachable_runs.insert(child_run_id.clone()) {
                        pending_runs.push(child_run_id.clone());
                    }
                }
            }
        }
    }

    fn release_root_descendant_lease(&self, id: u64) {
        let mut root_leases = self.root_leases();
        let Some(record) = root_leases.remove(&id) else {
            return;
        };
        let controls = self.controls();
        for job_id in record.pinned_jobs {
            if let Some(control) = controls.get(&job_id) {
                control
                    .waiters
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
        }
        drop(controls);
        drop(root_leases);
        let _ = self.prune_terminal_over_cap();
    }

    /// Waits for every registered driver job descended from `root_run_id` to
    /// reach a terminal state and returns their final snapshots.
    ///
    /// The root run itself is not a registered driver job. Call this only
    /// after that root run has completed and can no longer admit direct child
    /// jobs. A live descendant remains an admission source until it becomes
    /// terminal, so grandchildren registered while settling are included.
    /// Paused (`AwaitingInput`) jobs are not terminal and keep settlement open.
    /// Driver jobs retain native parent-to-attempt edges from live caller and
    /// child contexts, including earlier retries and resumed invocations.
    /// Jobs without native edges and jobs owned by another owner are excluded.
    /// Legacy registrations may expose their durable key's parent but cannot
    /// expose descendants until actual child attempt ids are supplied. The
    /// deadline is absolute and shared by the whole operation. This
    /// compatibility method only sees records still retained when called;
    /// acquire [`Self::retain_root_run_descendants`] before admissions when
    /// completed history must survive bounded-registry pruning.
    pub async fn settle_root_run_descendants(
        &self,
        root_run_id: &tinyagents_harness::ids::RunId,
        owner: impl ToString,
        deadline: tokio::time::Instant,
        cancellation: &tinyagents_harness::cancel::CancellationToken,
    ) -> std::result::Result<Vec<SubAgentJob>, SubAgentJobError> {
        let owner = owner.to_string();
        self.settle_root_run_descendants_inner(root_run_id, &owner, deadline, cancellation)
            .await
    }

    async fn settle_root_run_descendants_inner(
        &self,
        root_run_id: &tinyagents_harness::ids::RunId,
        owner: &str,
        deadline: tokio::time::Instant,
        cancellation: &tinyagents_harness::cancel::CancellationToken,
    ) -> std::result::Result<Vec<SubAgentJob>, SubAgentJobError> {
        let mut changes = self.changes.subscribe();
        let mut pinned = std::collections::HashMap::<SubAgentJobId, JobWaitGuard>::new();
        loop {
            let (jobs, pending) = {
                // Registration and lifecycle transitions take this same lock.
                // Subscribing before the scan while holding it closes the
                // scan/subscribe race; a later change remains visible in the
                // watch receiver even if it precedes the await below.
                let controls = self.controls();
                let snapshots = self
                    .tasks
                    .snapshots(Some(owner))
                    .map_err(|error| SubAgentJobError::Registry(error.to_string()))?;
                let mut reachable_runs = vec![root_run_id.clone()];
                let mut seen_runs = std::collections::HashSet::new();
                let mut reachable_jobs = std::collections::HashSet::new();
                while let Some(run_id) = reachable_runs.pop() {
                    if !seen_runs.insert(run_id.clone()) {
                        continue;
                    }
                    for snapshot in &snapshots {
                        if reachable_jobs.contains(&snapshot.status.id) {
                            continue;
                        }
                        let Some(control) = controls.get(&snapshot.status.id) else {
                            continue;
                        };
                        if !control.lineage.parents.contains(&run_id) {
                            continue;
                        }
                        reachable_jobs.insert(snapshot.status.id.clone());
                        pinned.entry(snapshot.status.id.clone()).or_insert_with(|| {
                            JobWaitGuard::pin(self.clone(), control.waiters.clone())
                        });
                        reachable_runs.extend(control.lineage.children.iter().cloned());
                    }
                }
                let mut jobs: Vec<_> = snapshots
                    .into_iter()
                    .filter(|snapshot| reachable_jobs.contains(&snapshot.status.id))
                    .map(|snapshot| snapshot.status)
                    .collect();
                jobs.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
                let pending = jobs.iter().any(|job| !job.status.is_terminal());
                (jobs, pending)
            };
            if !pending {
                return Ok(jobs);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(SubAgentJobError::SettlementCancelled),
                _ = tokio::time::sleep_until(deadline) => return Err(SubAgentJobError::SettlementTimedOut),
                result = changes.changed() => {
                    if result.is_err() {
                        return Err(SubAgentJobError::Registry("job registry change stream closed".into()));
                    }
                }
            }
        }
    }
}
