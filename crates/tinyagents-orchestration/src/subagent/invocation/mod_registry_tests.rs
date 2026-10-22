//! The job registry is an adapter over `tinyagents_tasks::DetachedTaskRegistry`:
//! these tests pin the shared-registry view and the lifecycle hand-offs.

use super::*;
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::TaskId;

#[tokio::test]
async fn jobs_are_registered_in_the_shared_detached_registry() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, _steering) = jobs.create("worker", 7);
    let task_id = TaskId::new(job_id.as_str());

    let tasks = jobs.tasks();
    assert_eq!(tasks.len().unwrap(), 1);
    let snapshot = tasks.snapshot(&task_id, "7").unwrap();
    assert_eq!(snapshot.status.status, SubAgentJobStatus::Queued);
    assert_eq!(snapshot.status.agent, "worker");
    // Ownership is the detached registry's, so a foreign owner is refused.
    assert!(tasks.snapshot(&task_id, "8").is_err());
}

#[tokio::test]
async fn steering_is_reachable_while_live_and_released_when_settled() {
    let jobs = SubAgentJobRegistry::new();
    let (job_id, _steering) = jobs.create("worker", 1);
    let task_id = TaskId::new(job_id.as_str());
    assert!(jobs.tasks().steering_handle_trusted(&task_id).is_ok());

    jobs.mark_result(&job_id, Err(TinyAgentsError::Cancelled));

    assert!(jobs.tasks().steering_handle_trusted(&task_id).is_err());
    // A settled job stays queryable; the registry never prunes it.
    assert_eq!(
        jobs.get(job_id.as_str()).unwrap().status,
        SubAgentJobStatus::Cancelled
    );
    assert_eq!(jobs.tasks().len().unwrap(), 1);
}

#[tokio::test]
async fn terminal_jobs_survive_many_registrations() {
    // The detached registry sweeps terminal entries at its soft cap; the job
    // registry must never lose a settled job that way.
    let jobs = SubAgentJobRegistry::new();
    let mut ids = Vec::new();
    for _ in 0..2_000 {
        let (id, _s) = jobs.create("worker", 1);
        jobs.mark_result(&id, Err(TinyAgentsError::Cancelled));
        ids.push(id);
    }
    assert!(ids.iter().all(|id| jobs.get(id.as_str()).is_some()));
    assert_eq!(jobs.list().len(), 2_000);
}

#[tokio::test]
async fn control_errors_keep_their_documented_order() {
    let jobs = SubAgentJobRegistry::new();
    let (live, _s) = jobs.create("worker", 1);
    let (done, _s2) = jobs.create("worker", 1);
    jobs.mark_result(&done, Err(TinyAgentsError::Cancelled));
    let not_found = |id: &str| SubAgentJobError::NotFound(id.to_owned());

    // Unknown id and foreign owner are both NotFound, even for a settled job.
    assert_eq!(jobs.cancel_owned("nope", 1).unwrap_err(), not_found("nope"));
    assert_eq!(
        jobs.cancel_owned(live.as_str(), 2).unwrap_err(),
        not_found(live.as_str())
    );
    assert_eq!(
        jobs.cancel_owned(done.as_str(), 2).unwrap_err(),
        not_found(done.as_str())
    );
    assert_eq!(
        jobs.send_message_with_request_id(done.as_str(), 2, "m", None)
            .unwrap_err(),
        not_found(done.as_str())
    );
    // Own settled job is Terminal.
    let terminal = SubAgentJobError::Terminal {
        job_id: done.as_str().to_owned(),
        status: SubAgentJobStatus::Cancelled,
    };
    assert_eq!(
        jobs.cancel_owned(done.as_str(), 1).unwrap_err(),
        terminal.clone()
    );
    assert_eq!(
        jobs.send_message_with_request_id(done.as_str(), 1, "m", None)
            .unwrap_err(),
        terminal
    );
    // An oversized id on a live job is RequestIdTooLong...
    let long = "x".repeat(10_000);
    assert_eq!(
        jobs.send_message_with_request_id(live.as_str(), 1, "m", Some(&long))
            .unwrap_err(),
        SubAgentJobError::RequestIdTooLong
    );
    // ...but a cancelling job reports Cancelling before the id is looked at.
    jobs.cancel_owned(live.as_str(), 1).unwrap();
    let cancelling = SubAgentJobError::Cancelling(live.as_str().to_owned());
    assert_eq!(
        jobs.send_message_with_request_id(live.as_str(), 1, "m", Some(&long))
            .unwrap_err(),
        cancelling
    );
    // Repeated cancels of a cancelling job stay Ok.
    assert!(jobs.cancel_owned(live.as_str(), 1).is_ok());
}

#[tokio::test]
async fn poisoned_detached_registry_errors_remain_typed_across_job_controls() {
    let jobs = SubAgentJobRegistry::new();
    let (id, _) = jobs.create("worker", 7);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::types::PANIC_ON_JOB_META_CLONE.with(|panic| panic.set(true));
        jobs.try_get_owned(id.as_str(), 7)
    }));
    // The intentional panic skips the end of the closure; clear the trigger
    // before any assertions or awaits can reuse this test worker.
    super::types::PANIC_ON_JOB_META_CLONE.with(|panic| panic.set(false));
    assert!(
        panic.is_err(),
        "metadata clone poisons the real job registry lock"
    );

    assert!(matches!(
        jobs.try_get_owned(id.as_str(), 7),
        Err(SubAgentJobError::Registry(_))
    ));
    assert!(matches!(
        jobs.try_list_owned(7),
        Err(SubAgentJobError::Registry(_))
    ));
    assert!(matches!(
        jobs.wait_owned(id.as_str(), 7).await,
        Err(SubAgentJobError::Registry(_))
    ));
    assert!(matches!(
        jobs.cancel_owned(id.as_str(), 7),
        Err(SubAgentJobError::Registry(_))
    ));
    assert!(matches!(
        jobs.send_message_with_request_id(id.as_str(), 7, "message", None),
        Err(SubAgentJobError::Registry(_))
    ));
}

#[tokio::test]
async fn listings_are_sorted_by_job_id() {
    let jobs = SubAgentJobRegistry::new();
    for _ in 0..40 {
        jobs.create("worker", 1);
    }
    let all: Vec<String> = jobs.list().into_iter().map(|j| j.id.0).collect();
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted);
    assert_eq!(all.len(), 40);
}

#[tokio::test]
async fn terminal_jobs_never_return_to_running() {
    let jobs = SubAgentJobRegistry::new();
    let (id, _) = jobs.create("worker", 1);
    jobs.cancel_owned(id.as_str(), 1).expect("cancel");
    jobs.mark_result(&id, Err(TinyAgentsError::Cancelled));
    jobs.mark_running(&id);
    assert_eq!(
        jobs.get(id.as_str()).unwrap().status,
        SubAgentJobStatus::Cancelled
    );
}

#[tokio::test]
async fn settled_jobs_release_their_cancellation_token() {
    let jobs = SubAgentJobRegistry::new();
    let (id, _) = jobs.create("worker", 1);
    let task_id = TaskId::new(id.as_str());
    assert!(jobs.tasks().holds_cancellation(&task_id).unwrap());
    jobs.cancel_owned(id.as_str(), 1).expect("cancel");
    assert!(!jobs.tasks().holds_cancellation(&task_id).unwrap());
    assert!(!jobs.get(id.as_str()).unwrap().status.is_terminal());
    jobs.mark_result(&id, Err(TinyAgentsError::Cancelled));
    assert!(jobs.get(id.as_str()).unwrap().status.is_terminal());

    let (aborted, _) = jobs.create("worker", 1);
    jobs.mark_aborted(&aborted, true);
    assert!(
        !jobs
            .tasks()
            .holds_cancellation(&TaskId::new(aborted.as_str()))
            .unwrap()
    );
}

#[tokio::test]
async fn a_budget_overrun_that_raced_an_owner_cancel_settles_cancelled() {
    let jobs = SubAgentJobRegistry::new();
    let (id, _) = jobs.create("worker", 1);
    jobs.cancel_owned(id.as_str(), 1).expect("cancel");
    jobs.mark_budget_overrun(
        &id,
        crate::subagent::AppliedResult {
            text: "late".into(),
            ..Default::default()
        },
        "over budget".into(),
    );
    let job = jobs.get(id.as_str()).unwrap();
    assert_eq!(job.status, SubAgentJobStatus::Cancelled);
    assert!(job.incomplete_kind.is_none() && job.output.is_none());
}

#[tokio::test]
async fn message_request_ids_are_bounded_per_job() {
    let jobs = SubAgentJobRegistry::new();
    let (id, steering) = jobs.create("worker", 1);
    for index in 0..=tinyagents_harness::steering::RecentRequestIds::DEFAULT_CAPACITY {
        let duplicate = jobs
            .send_message_with_request_id(id.as_str(), 1, "m", Some(&format!("r{index}")))
            .unwrap();
        assert!(!duplicate);
    }
    // r0 ages out of the per-job 64-id window and can be delivered again.
    assert!(
        !jobs
            .send_message_with_request_id(id.as_str(), 1, "m", Some("r0"))
            .unwrap()
    );
    assert_eq!(steering.pending(), 66);
}
