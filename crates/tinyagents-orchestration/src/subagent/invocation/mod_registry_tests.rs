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
async fn owner_cancel_tombstones_a_driver_job_against_late_completion() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_tasks::{
        CompletionRecord, CompletionResult, CompletionRouter, CompletionStatus,
        InMemoryCompletionStore, RecordOutcome,
    };

    let jobs = SubAgentJobRegistry::new();
    let completions = CompletionRouter::new(std::sync::Arc::new(InMemoryCompletionStore::new()));
    let key = crate::subagent::SubagentTaskKey {
        root_run_id: "root".into(),
        parent_run_id: "parent".into(),
        thread_id: Some("thread".into()),
        task_id: "child".into(),
    };
    let token = CancellationToken::new();
    let (id, _, _, registered_token, inserted) = jobs
        .register_driver_job(DriverJobRegistration {
            key: key.clone(),
            agent: "worker".into(),
            owner: "owner-a".into(),
            workspace: Some(tinytools::WorkspaceDescriptor::new("/workspace/a")),
            cancellation: token.clone(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .unwrap();
    assert!(inserted);
    assert!(!registered_token.is_cancelled());

    assert!(
        jobs.cancel_key_owned_and_tombstone(&key, "owner-b")
            .is_err()
    );
    assert!(!token.is_cancelled());
    let late_completion = CompletionRecord::new(
        "child",
        "thread",
        "worker",
        CompletionStatus::Success,
        CompletionResult::text("late output"),
    );
    assert!(matches!(
        completions.record(late_completion.clone()).await.unwrap(),
        RecordOutcome::Recorded { .. }
    ));
    let canceled = jobs
        .cancel_key_owned_and_tombstone(&key, "owner-a")
        .unwrap();
    assert!(token.is_cancelled());
    assert_eq!(canceled.status, SubAgentJobStatus::Cancelled);

    jobs.mark_driver_result(
        &id,
        Ok(crate::subagent::SubagentOutcome::completed(
            "child",
            "late output",
        )),
    );
    let retained = jobs.get_owned(id.as_str(), "owner-a").unwrap();
    assert_eq!(retained.status, SubAgentJobStatus::Cancelled);
    assert_eq!(retained.output, None);
    jobs.mark_driver_result(&id, Err("late failure".into()));
    let retained = jobs.get_owned(id.as_str(), "owner-a").unwrap();
    assert_eq!(retained.status, SubAgentJobStatus::Cancelled);
    assert_eq!(
        retained.error,
        Some(TinyAgentsError::Cancelled.to_string())
    );
    assert!(completions.pending_for("thread").is_empty());
    assert_eq!(
        completions.record(late_completion).await.unwrap(),
        RecordOutcome::Suppressed
    );
}

#[tokio::test]
async fn live_job_metadata_retains_parent_thread_and_workspace() {
    let jobs = SubAgentJobRegistry::new();
    let workspace = tinytools::WorkspaceDescriptor::new("/workspace/project");
    let (job_id, _) = jobs.create_with_cancellation(
        "worker",
        7,
        tinyagents_harness::cancel::CancellationToken::new(),
        JobLink {
            parent_thread_id: Some("thread-7".into()),
            workspace: Some(workspace.clone()),
            ..JobLink::default()
        },
    );
    let snapshot = jobs
        .tasks()
        .snapshot(&TaskId::new(job_id.as_str()), "7")
        .unwrap();
    assert_eq!(
        snapshot.metadata.parent_thread_id.as_deref(),
        Some("thread-7")
    );
    assert_eq!(snapshot.metadata.workspace, Some(workspace));
}

#[tokio::test]
async fn thread_and_global_cancellation_select_jobs_by_retained_metadata() {
    use tinyagents_harness::cancel::CancellationToken;

    let jobs = SubAgentJobRegistry::new();
    let workspace = tinytools::WorkspaceDescriptor::new("/workspace/project");
    let thread_token = CancellationToken::new();
    let other_token = CancellationToken::new();
    let (thread_job, _) = jobs.create_with_cancellation(
        "worker",
        7,
        thread_token.clone(),
        JobLink {
            parent_thread_id: Some("thread-a".into()),
            workspace: Some(workspace.clone()),
            ..JobLink::default()
        },
    );
    let (other_job, _) = jobs.create_with_cancellation(
        "worker",
        7,
        other_token.clone(),
        JobLink {
            parent_thread_id: Some("thread-b".into()),
            workspace: Some(workspace.clone()),
            ..JobLink::default()
        },
    );

    let scope = ParentJobScope::new(workspace, "thread-a");
    assert_eq!(jobs.cancel_for_thread(&scope).unwrap(), 1);
    assert!(thread_token.is_cancelled());
    assert!(!other_token.is_cancelled());
    assert!(jobs.get(thread_job.as_str()).is_some());
    assert!(!jobs.get(thread_job.as_str()).unwrap().status.is_terminal());
    assert_eq!(jobs.cancel_for_thread(&scope).unwrap(), 0);

    assert_eq!(jobs.cancel_all().unwrap(), 1);
    assert!(other_token.is_cancelled());
    assert!(jobs.get(other_job.as_str()).is_some());
}

#[tokio::test]
async fn thread_cancellation_is_scoped_to_workspace() {
    use tinyagents_harness::cancel::CancellationToken;

    let jobs = SubAgentJobRegistry::new();
    let workspace_a = tinytools::WorkspaceDescriptor::new("/workspace/a");
    let workspace_b = tinytools::WorkspaceDescriptor::new("/workspace/b");
    let token_a = CancellationToken::new();
    let token_b = CancellationToken::new();
    for (token, workspace) in [
        (token_a.clone(), workspace_a.clone()),
        (token_b.clone(), workspace_b),
    ] {
        jobs.create_with_cancellation(
            "worker",
            7,
            token,
            JobLink {
                parent_thread_id: Some("same-thread".into()),
                workspace: Some(workspace),
                ..JobLink::default()
            },
        );
    }

    assert_eq!(
        jobs.cancel_for_thread(&ParentJobScope::new(workspace_a, "same-thread"))
            .unwrap(),
        1
    );
    assert!(token_a.is_cancelled());
    assert!(!token_b.is_cancelled());
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

#[tokio::test]
async fn cancel_all_for_workspace_does_not_cross_workspace_boundaries() {
    use tinyagents_harness::cancel::CancellationToken;

    let jobs = SubAgentJobRegistry::new();
    let workspace_a = tinytools::WorkspaceDescriptor::new("/workspace/a");
    let workspace_b = tinytools::WorkspaceDescriptor::new("/workspace/b");
    let token_a = CancellationToken::new();
    let token_b = CancellationToken::new();
    for (token, workspace) in [
        (token_a.clone(), workspace_a.clone()),
        (token_b.clone(), workspace_b),
    ] {
        jobs.create_with_cancellation(
            "worker",
            7,
            token,
            JobLink {
                workspace: Some(workspace),
                ..JobLink::default()
            },
        );
    }

    assert_eq!(jobs.cancel_all_for_workspace(&workspace_a).unwrap(), 1);
    assert!(token_a.is_cancelled());
    assert!(!token_b.is_cancelled());
    assert_eq!(jobs.cancel_all_for_workspace(&workspace_a).unwrap(), 0);
}
