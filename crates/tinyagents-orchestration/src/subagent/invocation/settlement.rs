//! Bounded settlement for native driver-job descendants.

use super::*;

impl SubAgentJobRegistry {
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
    /// deadline is absolute and shared by the whole operation.
    pub async fn settle_root_run_descendants(
        &self,
        root_run_id: &tinyagents_harness::ids::RunId,
        owner: impl ToString,
        deadline: tokio::time::Instant,
        cancellation: &tinyagents_harness::cancel::CancellationToken,
    ) -> std::result::Result<Vec<SubAgentJob>, SubAgentJobError> {
        let owner = owner.to_string();
        let mut changes = self.changes.subscribe();
        loop {
            let (jobs, pending) = {
                // Registration and lifecycle transitions take this same lock.
                // Subscribing before the scan while holding it closes the
                // scan/subscribe race; a later change remains visible in the
                // watch receiver even if it precedes the await below.
                let controls = self.controls();
                let snapshots = self
                    .tasks
                    .snapshots(Some(&owner))
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
