//! The job registry is an adapter over `tinyagents_tasks::DetachedTaskRegistry`:
//! these tests pin the shared-registry view and the lifecycle hand-offs.

use super::*;
use crate::subagent::{SubagentOutcome, SubagentOutcomeKind, SubagentPause, SubagentResume};
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::ids::TaskId;

fn register_driver_job(
    jobs: &SubAgentJobRegistry,
    task_id: impl Into<String>,
) -> (crate::subagent::SubagentTaskKey, SubAgentJobId) {
    use tinyagents_harness::cancel::CancellationToken;

    let key = crate::subagent::SubagentTaskKey {
        root_run_id: "root".into(),
        parent_run_id: "parent".into(),
        thread_id: Some("thread".into()),
        task_id: task_id.into(),
    };
    let (id, _, _, _, inserted) = jobs
        .register_driver_job(DriverJobRegistration {
            key: key.clone(),
            agent: "worker".into(),
            owner: "owner".into(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .unwrap();
    assert!(inserted);
    (key, id)
}

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
async fn root_settlement_includes_late_grandchildren_and_only_owned_root_jobs() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let registration = |task: &str, root: &str, parent: &str, owner: &str| {
        let key = crate::subagent::SubagentTaskKey {
            root_run_id: root.into(),
            parent_run_id: parent.into(),
            thread_id: None,
            task_id: task.into(),
        };
        DriverJobRegistration {
            key,
            agent: task.into(),
            owner: owner.into(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        }
    };
    let register_with_runs = |registration, parent: &str, child: &str| {
        let (id, _, _, _, _) = jobs
            .register_driver_job_with_parent_run(registration, RunId::new(parent))
            .unwrap();
        jobs.set_attempt_run_id(&id, child);
        id
    };

    let child = register_with_runs(
        registration("child", "root", "key-parent", "owner"),
        "root",
        "child-run",
    );
    let unrelated = jobs
        .register_driver_job(registration(
            "unrelated",
            "other-root",
            "other-root",
            "owner",
        ))
        .unwrap()
        .0;
    let foreign = jobs
        .register_driver_job(registration("foreign", "root", "root", "other-owner"))
        .unwrap()
        .0;
    jobs.mark_running(&child);
    jobs.mark_driver_result(&unrelated, Err("unrelated failure".into()));
    jobs.mark_driver_result(&foreign, Err("foreign failure".into()));

    let settling = {
        let jobs = jobs.clone();
        tokio::spawn(async move {
            jobs.settle_root_run_descendants(
                &RunId::new("root"),
                "owner",
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &CancellationToken::new(),
            )
            .await
        })
    };
    tokio::task::yield_now().await;

    // A running descendant can still admit a grandchild. Its terminal
    // transition happens only after that registration, so settlement must
    // observe and wait for both jobs.
    let grandchild = register_with_runs(
        registration("grandchild", "root", "durable-parent", "owner"),
        "child-run",
        "grandchild-run",
    );
    jobs.mark_running(&grandchild);
    jobs.mark_driver_result(&child, Err("child failed".into()));
    assert!(!settling.is_finished());
    jobs.mark_driver_result(&grandchild, Err("grandchild failed".into()));

    let settled = settling.await.unwrap().unwrap();
    let mut settled_ids = settled
        .iter()
        .map(|job| job.id.as_str())
        .collect::<Vec<_>>();
    settled_ids.sort_unstable();
    let mut expected_ids = vec![child.as_str(), grandchild.as_str()];
    expected_ids.sort_unstable();
    assert_eq!(settled_ids, expected_ids);
    assert!(
        settled
            .iter()
            .all(|job| job.status == SubAgentJobStatus::Failed)
    );

    let repeated = jobs
        .settle_root_run_descendants(
            &RunId::new("root"),
            "owner",
            tokio::time::Instant::now(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(repeated, settled);
}

#[tokio::test]
async fn root_settlement_uses_one_deadline_and_propagates_cancellation() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let (child, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: "root".into(),
                    parent_run_id: "legacy-parent".into(),
                    thread_id: None,
                    task_id: "pending".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            RunId::new("root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&child, "child");
    jobs.mark_running(&child);
    let deadline_result = jobs
        .settle_root_run_descendants(
            &RunId::new("root"),
            "owner",
            tokio::time::Instant::now(),
            &CancellationToken::new(),
        )
        .await;
    assert_eq!(deadline_result, Err(SubAgentJobError::SettlementTimedOut));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled_result = jobs
        .settle_root_run_descendants(
            &RunId::new("root"),
            "owner",
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            &cancellation,
        )
        .await;
    assert_eq!(cancelled_result, Err(SubAgentJobError::SettlementCancelled));
}

#[tokio::test]
async fn root_settlement_returns_empty_for_unlinked_jobs_and_empty_roots() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let (legacy, _, _, _, _) = jobs
        .register_driver_job(DriverJobRegistration {
            key: crate::subagent::SubagentTaskKey {
                root_run_id: "root".into(),
                parent_run_id: "legacy-parent".into(),
                thread_id: None,
                task_id: "legacy-unlinked".into(),
            },
            agent: "worker".into(),
            owner: "owner".into(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .unwrap();
    jobs.mark_driver_result(&legacy, Err("legacy terminal".into()));
    assert!(
        jobs.settle_root_run_descendants(
            &RunId::new("root"),
            "owner",
            tokio::time::Instant::now(),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
        jobs.settle_root_run_descendants(
            &RunId::new("empty-root"),
            "owner",
            tokio::time::Instant::now(),
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .is_empty()
    );
}

#[tokio::test(start_paused = true)]
async fn root_settlement_keeps_one_deadline_across_lifecycle_wakes() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let (child, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: "root".into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: "deadline-child".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            RunId::new("root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&child, "deadline-child-attempt");
    jobs.mark_running(&child);
    let cancellation = CancellationToken::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let root_run_id = RunId::new("root");
    let settling = jobs.settle_root_run_descendants(&root_run_id, "owner", deadline, &cancellation);
    tokio::pin!(settling);
    tokio::select! {
        biased;
        result = &mut settling => panic!("live descendant settled unexpectedly: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    jobs.set_attempt_run_id(&child, "deadline-child-another-observed-attempt");
    tokio::select! {
        biased;
        result = &mut settling => panic!("live descendant settled unexpectedly after wake: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::select! {
        biased;
        result = &mut settling => assert_eq!(result, Err(SubAgentJobError::SettlementTimedOut)),
        _ = tokio::task::yield_now() => panic!("settlement extended the caller's absolute deadline"),
    }
}

#[tokio::test]
async fn root_settlement_cancellation_while_blocked_releases_waiter_pins() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let (child, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: "root".into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: "cancel-child".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            RunId::new("root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&child, "cancel-child-attempt");
    jobs.mark_running(&child);
    let cancellation = CancellationToken::new();
    let root_run_id = RunId::new("root");
    let settling = jobs.settle_root_run_descendants(
        &root_run_id,
        "owner",
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        &cancellation,
    );
    tokio::pin!(settling);
    tokio::select! {
        biased;
        result = &mut settling => panic!("live descendant settled unexpectedly: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    cancellation.cancel();
    assert_eq!(settling.await, Err(SubAgentJobError::SettlementCancelled));
    assert_eq!(
        jobs.controls()[&child]
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(
        jobs.get(child.as_str()).unwrap().status,
        SubAgentJobStatus::Running
    );
}

#[tokio::test]
async fn root_settlement_pins_terminal_ancestors_while_descendants_remain_live() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let registration = |task: &str, parent: &str| DriverJobRegistration {
        key: crate::subagent::SubagentTaskKey {
            root_run_id: "settlement-retention-root".into(),
            parent_run_id: parent.into(),
            thread_id: None,
            task_id: task.into(),
        },
        agent: task.into(),
        owner: "owner".into(),
        workspace: None,
        cancellation: CancellationToken::new(),
        steering: None,
        run_queue: None,
        continuing: false,
    };
    let (parent, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("settlement-parent", "unused"),
            RunId::new("settlement-root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&parent, "settlement-parent-attempt");
    jobs.mark_running(&parent);
    let (child, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("settlement-child", "unused"),
            RunId::new("settlement-parent-attempt"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&child, "settlement-child-attempt");
    jobs.mark_running(&child);
    jobs.mark_driver_result(
        &parent,
        Ok(SubagentOutcome::completed(
            parent.as_str(),
            "parent finished",
        )),
    );

    let cancellation = CancellationToken::new();
    let root_run_id = RunId::new("settlement-root");
    let settling = jobs.settle_root_run_descendants(
        &root_run_id,
        "owner",
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        &cancellation,
    );
    tokio::pin!(settling);
    tokio::select! {
        biased;
        result = &mut settling => panic!("live child settled unexpectedly: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }

    for index in 0..=super::types::SUBAGENT_JOB_SOFT_CAP {
        let task = format!("pressure-{index}");
        let (id, _, _, _, _) = jobs
            .register_driver_job_with_parent_run(
                registration(&task, "unused"),
                RunId::new("unrelated-root"),
            )
            .unwrap();
        jobs.set_attempt_run_id(&id, &format!("{task}-attempt"));
        jobs.mark_driver_result(
            &id,
            Ok(SubagentOutcome::completed(
                id.as_str(),
                "unrelated terminal",
            )),
        );
    }

    assert!(
        jobs.get(parent.as_str()).is_some(),
        "the pinned ancestor must survive terminal eviction"
    );
    jobs.mark_driver_result(
        &child,
        Ok(SubagentOutcome::completed(child.as_str(), "child finished")),
    );
    let settled = settling.await.unwrap();
    assert_eq!(settled.len(), 2);
    assert!(settled.iter().any(|job| {
        job.id == parent
            && job.output.as_deref() == Some("parent finished")
            && job.status == SubAgentJobStatus::Completed
    }));
    assert!(settled.iter().any(|job| job.id == child));
}

#[tokio::test]
async fn root_lease_preserves_parent_and_grandchild_before_settlement_starts() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let lease = jobs
        .retain_root_run_descendants(&RunId::new("leased-root"), "owner")
        .unwrap();
    let registration = |task: &str, parent: &str| DriverJobRegistration {
        key: crate::subagent::SubagentTaskKey {
            root_run_id: "leased-root".into(),
            parent_run_id: parent.into(),
            thread_id: None,
            task_id: task.into(),
        },
        agent: task.into(),
        owner: "owner".into(),
        workspace: None,
        cancellation: CancellationToken::new(),
        steering: None,
        run_queue: None,
        continuing: false,
    };
    let (parent, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("leased-parent", "ignored"),
            RunId::new("leased-root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&parent, "leased-parent-attempt");
    jobs.mark_running(&parent);
    let (grandchild, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("leased-grandchild", "ignored"),
            RunId::new("leased-parent-attempt"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&grandchild, "leased-grandchild-attempt");
    jobs.mark_running(&grandchild);
    jobs.mark_driver_result(
        &parent,
        Ok(SubagentOutcome::completed(
            parent.as_str(),
            "retained parent output",
        )),
    );

    for index in 0..=super::types::SUBAGENT_JOB_SOFT_CAP {
        let task = format!("lease-pressure-{index}");
        let (id, _, _, _, _) = jobs
            .register_driver_job_with_parent_run(
                registration(&task, "ignored"),
                RunId::new("unrelated-root"),
            )
            .unwrap();
        jobs.set_attempt_run_id(&id, &format!("{task}-attempt"));
        jobs.mark_driver_result(&id, Ok(SubagentOutcome::completed(id.as_str(), "pressure")));
    }
    assert!(jobs.get(parent.as_str()).is_some());

    let cancellation = CancellationToken::new();
    let settling = lease.settle(
        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        &cancellation,
    );
    tokio::pin!(settling);
    tokio::select! {
        biased;
        result = &mut settling => panic!("live grandchild settled unexpectedly: {result:?}"),
        _ = tokio::task::yield_now() => {},
    }
    jobs.mark_driver_result(
        &grandchild,
        Ok(SubagentOutcome::completed(
            grandchild.as_str(),
            "grandchild output",
        )),
    );
    let settled = settling.await.unwrap();
    assert_eq!(settled.len(), 2);
    assert!(settled.iter().any(|job| {
        job.id == parent && job.output.as_deref() == Some("retained parent output")
    }));
    assert!(settled.iter().any(|job| job.id == grandchild));
}

#[tokio::test]
async fn root_lease_keeps_completed_history_and_overlapping_lease_pins_independent() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let root = RunId::new("history-root");
    let first = jobs.retain_root_run_descendants(&root, "owner").unwrap();
    let second = jobs.retain_root_run_descendants(&root, "owner").unwrap();
    let settling_lease = jobs.retain_root_run_descendants(&root, "owner").unwrap();
    let foreign = jobs
        .retain_root_run_descendants(&root, "foreign-owner")
        .unwrap();
    let (parent, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: "history-root".into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: "history-parent".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            root.clone(),
        )
        .unwrap();
    jobs.set_attempt_run_id(&parent, "history-parent-attempt");
    let (child, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: "history-root".into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: "history-child".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            RunId::new("history-parent-attempt"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&child, "history-child-attempt");
    jobs.mark_driver_result(
        &parent,
        Ok(SubagentOutcome::completed(parent.as_str(), "parent")),
    );
    jobs.mark_driver_result(
        &child,
        Ok(SubagentOutcome::completed(child.as_str(), "child")),
    );
    assert_eq!(
        jobs.controls()[&parent]
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        3
    );
    drop(foreign);
    drop(first);
    assert_eq!(
        jobs.controls()[&parent]
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert!(jobs.get(parent.as_str()).is_some());

    for index in 0..=super::types::SUBAGENT_JOB_SOFT_CAP {
        let task = format!("history-pressure-{index}");
        let (id, _, _, _, _) = jobs
            .register_driver_job_with_parent_run(
                DriverJobRegistration {
                    key: crate::subagent::SubagentTaskKey {
                        root_run_id: "history-root".into(),
                        parent_run_id: "ignored".into(),
                        thread_id: None,
                        task_id: task.clone(),
                    },
                    agent: "worker".into(),
                    owner: "owner".into(),
                    workspace: None,
                    cancellation: CancellationToken::new(),
                    steering: None,
                    run_queue: None,
                    continuing: false,
                },
                RunId::new("pressure-root"),
            )
            .unwrap();
        jobs.set_attempt_run_id(&id, &format!("{task}-attempt"));
        jobs.mark_driver_result(&id, Ok(SubagentOutcome::completed(id.as_str(), "pressure")));
    }
    assert!(jobs.get(parent.as_str()).is_some());
    drop(second);
    assert!(jobs.get(parent.as_str()).is_some());
    let settled = settling_lease
        .settle(
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(settled.len(), 2);
    assert!(
        settled
            .iter()
            .any(|job| job.id == parent && job.output.as_deref() == Some("parent"))
    );
    assert!(
        settled
            .iter()
            .any(|job| job.id == child && job.output.as_deref() == Some("child"))
    );
    assert!(
        jobs.get(parent.as_str()).is_none(),
        "last lease drop releases retention and allows pruning"
    );
}

#[tokio::test]
async fn cancelled_root_lease_releases_history_retention() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let root = RunId::new("cancelled-lease-root");
    let lease = jobs.retain_root_run_descendants(&root, "owner").unwrap();
    let (parent, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: root.as_str().into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: "cancelled-lease-parent".into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            root.clone(),
        )
        .unwrap();
    jobs.set_attempt_run_id(&parent, "cancelled-lease-attempt");
    jobs.mark_running(&parent);
    for index in 0..=super::types::SUBAGENT_JOB_SOFT_CAP {
        let task = format!("cancel-lease-pressure-{index}");
        let (id, _, _, _, _) = jobs
            .register_driver_job_with_parent_run(
                DriverJobRegistration {
                    key: crate::subagent::SubagentTaskKey {
                        root_run_id: root.as_str().into(),
                        parent_run_id: "ignored".into(),
                        thread_id: None,
                        task_id: task.clone(),
                    },
                    agent: "worker".into(),
                    owner: "owner".into(),
                    workspace: None,
                    cancellation: CancellationToken::new(),
                    steering: None,
                    run_queue: None,
                    continuing: false,
                },
                RunId::new("unrelated-root"),
            )
            .unwrap();
        jobs.set_attempt_run_id(&id, &format!("{task}-attempt"));
        jobs.mark_driver_result(&id, Ok(SubagentOutcome::completed(id.as_str(), "pressure")));
    }
    assert!(jobs.get(parent.as_str()).is_some());
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        lease
            .settle(
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &cancellation,
            )
            .await,
        Err(SubAgentJobError::SettlementCancelled)
    );
    jobs.mark_driver_result(
        &parent,
        Ok(SubagentOutcome::completed(
            parent.as_str(),
            "released after cancellation",
        )),
    );
    let task = "cancel-lease-final-pressure";
    let (pressure, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            DriverJobRegistration {
                key: crate::subagent::SubagentTaskKey {
                    root_run_id: root.as_str().into(),
                    parent_run_id: "ignored".into(),
                    thread_id: None,
                    task_id: task.into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: None,
                cancellation: CancellationToken::new(),
                steering: None,
                run_queue: None,
                continuing: false,
            },
            RunId::new("unrelated-root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&pressure, "cancel-lease-final-pressure-attempt");
    jobs.mark_driver_result(
        &pressure,
        Ok(SubagentOutcome::completed(pressure.as_str(), "pressure")),
    );
    assert!(jobs.get(parent.as_str()).is_none());
}

#[tokio::test]
async fn resumed_job_keeps_old_attempt_edges_without_importing_new_root_siblings() {
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::ids::RunId;

    let jobs = SubAgentJobRegistry::new();
    let registration = |task: &str, key_parent: &str| DriverJobRegistration {
        key: crate::subagent::SubagentTaskKey {
            root_run_id: "durable-root".into(),
            parent_run_id: key_parent.into(),
            thread_id: None,
            task_id: task.into(),
        },
        agent: task.into(),
        owner: "owner".into(),
        workspace: None,
        cancellation: CancellationToken::new(),
        steering: None,
        run_queue: None,
        continuing: false,
    };

    let (job, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("resumed", "durable-parent"),
            RunId::new("old-root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&job, "old-attempt");
    jobs.set_attempt_run_id(&job, "old-retry-attempt");

    let (old_grandchild, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("old-grandchild", "durable-parent"),
            RunId::new("old-retry-attempt"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&old_grandchild, "old-grandchild-run");
    jobs.mark_running(&old_grandchild);

    let mut paused = SubagentOutcome::completed(job.as_str(), "waiting");
    paused.status = SubagentOutcomeKind::AwaitingInput(SubagentPause {
        reason: "resume later".into(),
        resume: SubagentResume::default(),
    });
    jobs.mark_driver_result(&old_grandchild, Err("old attempt finished".into()));
    jobs.mark_driver_result(&job, Ok(paused));
    let paused_settlement = {
        let jobs = jobs.clone();
        tokio::spawn(async move {
            jobs.settle_root_run_descendants(
                &RunId::new("old-root"),
                "owner",
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &CancellationToken::new(),
            )
            .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!paused_settlement.is_finished());
    let mut resumed = registration("resumed", "durable-parent");
    resumed.continuing = true;
    let (resumed_id, _, _, _, continued) = jobs
        .register_driver_job_with_parent_run(resumed, RunId::new("new-root"))
        .unwrap();
    assert_eq!(resumed_id, job);
    assert!(continued);
    jobs.set_attempt_run_id(&resumed_id, "new-attempt");

    let (new_grandchild, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("new-grandchild", "durable-parent"),
            RunId::new("new-attempt"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&new_grandchild, "new-grandchild-run");
    jobs.mark_running(&new_grandchild);
    let (new_root_sibling, _, _, _, _) = jobs
        .register_driver_job_with_parent_run(
            registration("new-root-sibling", "new-root"),
            RunId::new("new-root"),
        )
        .unwrap();
    jobs.set_attempt_run_id(&new_root_sibling, "sibling-run");
    jobs.mark_running(&new_root_sibling);

    jobs.mark_driver_result(&job, Err("resumed job finished".into()));
    jobs.mark_driver_result(&new_grandchild, Err("new attempt finished".into()));
    jobs.mark_driver_result(&new_root_sibling, Err("sibling finished".into()));
    paused_settlement.await.unwrap().unwrap();

    let settle = |root: &'static str| {
        let jobs = jobs.clone();
        async move {
            jobs.settle_root_run_descendants(
                &RunId::new(root),
                "owner",
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                &CancellationToken::new(),
            )
            .await
            .unwrap()
        }
    };
    let old_root_jobs = settle("old-root").await;
    assert_eq!(old_root_jobs.len(), 3);
    assert!(old_root_jobs.iter().any(|entry| entry.id == old_grandchild));
    assert!(old_root_jobs.iter().any(|entry| entry.id == new_grandchild));
    assert!(
        !old_root_jobs
            .iter()
            .any(|entry| entry.id == new_root_sibling)
    );

    let new_root_jobs = settle("new-root").await;
    assert_eq!(new_root_jobs.len(), 4);
    assert!(new_root_jobs.iter().any(|entry| entry.id == job));
    assert!(new_root_jobs.iter().any(|entry| entry.id == old_grandchild));
    assert!(new_root_jobs.iter().any(|entry| entry.id == new_grandchild));
    assert!(
        new_root_jobs
            .iter()
            .any(|entry| entry.id == new_root_sibling)
    );
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
        jobs.cancel_key_owned_and_tombstone(&key, "owner-b", &completions)
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
        .cancel_key_owned_and_tombstone(&key, "owner-a", &completions)
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
    assert_eq!(retained.error, Some(TinyAgentsError::Cancelled.to_string()));
    assert_eq!(
        completions.record(late_completion).await.unwrap(),
        RecordOutcome::Suppressed
    );
    assert!(completions.pending_for("thread").is_empty());
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
async fn settled_driver_jobs_have_bounded_retention_across_all_registry_maps() {
    let jobs = SubAgentJobRegistry::new();
    let total = 2_000;
    for index in 0..total {
        let (_, id) = register_driver_job(&jobs, format!("child-{index}"));
        jobs.mark_driver_result(&id, Err("finished".into()));
    }

    let retained = jobs.tasks().len().unwrap();
    assert!(retained < total, "settled tasks retained: {retained}");
    assert_eq!(jobs.controls.lock().unwrap().len(), retained);
    assert_eq!(jobs.driver_jobs.lock().unwrap().len(), retained);
    assert_eq!(jobs.list().len(), retained);
}

#[tokio::test]
async fn recent_terminal_driver_jobs_replay_until_oldest_cache_eviction() {
    use tinyagents_harness::cancel::CancellationToken;

    let jobs = SubAgentJobRegistry::new();
    let (old_key, old_id) = register_driver_job(&jobs, "old-cache-entry");
    jobs.mark_running(&old_id);
    jobs.mark_driver_result(
        &old_id,
        Ok(crate::subagent::SubagentOutcome::completed(
            "old",
            "old result",
        )),
    );

    let (recent_key, recent_id) = register_driver_job(&jobs, "recent-cache-entry");
    jobs.mark_running(&recent_id);
    jobs.mark_driver_result(
        &recent_id,
        Ok(crate::subagent::SubagentOutcome::completed(
            "recent",
            "replayable result",
        )),
    );
    let waited = jobs
        .wait_owned(recent_id.as_str(), "owner")
        .await
        .expect("terminal result");
    assert_eq!(waited.output.as_deref(), Some("replayable result"));

    let (replayed_id, _, _, _, inserted) = jobs
        .register_driver_job(DriverJobRegistration {
            key: recent_key.clone(),
            agent: "worker".into(),
            owner: "owner".into(),
            workspace: None,
            cancellation: CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: false,
        })
        .expect("reuse retained terminal driver job");
    assert_eq!(replayed_id, recent_id);
    assert!(!inserted);

    for index in 0..127 {
        let (_, id) = register_driver_job(&jobs, format!("cache-filler-{index}"));
        jobs.mark_driver_result(
            &id,
            Ok(crate::subagent::SubagentOutcome::completed(
                "filler", "done",
            )),
        );
    }

    assert!(jobs.get(old_id.as_str()).is_none());
    assert_eq!(
        jobs.get(recent_id.as_str()).unwrap().output.as_deref(),
        Some("replayable result")
    );
    assert_eq!(
        jobs.driver_job_for_key(&recent_key, "owner"),
        Some(recent_id)
    );
    assert!(jobs.driver_job_for_key(&old_key, "owner").is_none());
    assert_eq!(jobs.tasks().len().unwrap(), 128);
    assert_eq!(jobs.controls.lock().unwrap().len(), 128);
    assert_eq!(jobs.driver_jobs.lock().unwrap().len(), 128);
}

#[tokio::test]
async fn settling_many_prestarted_jobs_bounds_terminal_retention() {
    let jobs = SubAgentJobRegistry::new();
    let total = 256;
    let ids: Vec<_> = (0..total)
        .map(|index| register_driver_job(&jobs, format!("prestarted-{index}")).1)
        .collect();
    for id in &ids {
        jobs.mark_running(id);
    }
    for id in &ids {
        jobs.mark_driver_result(
            id,
            Ok(crate::subagent::SubagentOutcome::completed(
                "prestarted",
                "done",
            )),
        );
    }

    let retained = jobs.tasks().len().unwrap();
    assert!(
        retained <= 128,
        "terminal tasks retained after a settlement batch: {retained}"
    );
    assert_eq!(jobs.controls.lock().unwrap().len(), retained);
    assert_eq!(jobs.driver_jobs.lock().unwrap().len(), retained);
    assert_eq!(jobs.list().len(), retained);
}

#[tokio::test]
async fn dropping_terminal_waiters_releases_retention_pins() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let jobs = SubAgentJobRegistry::new();
    let ids: Vec<_> = (0..132)
        .map(|index| register_driver_job(&jobs, format!("pinned-{index}")).1)
        .collect();
    for id in &ids {
        jobs.mark_running(id);
    }

    let mut waiters: Vec<_> = ids
        .iter()
        .map(|id| Box::pin(jobs.wait_owned(id.as_str(), "owner")))
        .collect();
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for waiter in &mut waiters {
        assert!(matches!(waiter.as_mut().poll(&mut context), Poll::Pending));
    }

    for id in &ids {
        jobs.mark_driver_result(
            id,
            Ok(crate::subagent::SubagentOutcome::completed(
                "pinned", "done",
            )),
        );
    }
    assert_eq!(jobs.tasks().len().unwrap(), ids.len());

    drop(waiters);
    let retained = jobs.tasks().len().unwrap();
    assert!(
        retained <= 128,
        "terminal tasks retained after waiter drops: {retained}"
    );
    assert_eq!(jobs.controls.lock().unwrap().len(), retained);
    assert_eq!(jobs.driver_jobs.lock().unwrap().len(), retained);
}

#[tokio::test]
async fn direct_cancellation_keeps_terminal_retention_bounded() {
    let jobs = SubAgentJobRegistry::new();
    let ids: Vec<_> = (0..256)
        .map(|index| register_driver_job(&jobs, format!("cancel-{index}")).1)
        .collect();
    for id in &ids {
        let mut awaiting = crate::subagent::SubagentOutcome::completed("cancel", "paused");
        awaiting.status =
            crate::subagent::SubagentOutcomeKind::AwaitingInput(crate::subagent::SubagentPause {
                reason: "clarification".into(),
                resume: crate::subagent::SubagentResume::default(),
            });
        jobs.mark_driver_result(id, Ok(awaiting));
    }
    for id in &ids {
        jobs.cancel_owned(id.as_str(), "owner").expect("cancel");
    }

    let retained = jobs.tasks().len().unwrap();
    assert!(retained <= 128, "cancelled tasks retained: {retained}");
    assert_eq!(jobs.controls.lock().unwrap().len(), retained);
    assert_eq!(jobs.driver_jobs.lock().unwrap().len(), retained);
}

#[tokio::test]
async fn active_and_awaiting_jobs_survive_terminal_retention_pressure() {
    let jobs = SubAgentJobRegistry::new();
    let (active_key, active_id) = register_driver_job(&jobs, "active");
    jobs.mark_running(&active_id);
    let (awaiting_key, awaiting_id) = register_driver_job(&jobs, "awaiting");
    let mut awaiting = crate::subagent::SubagentOutcome::completed("awaiting", "paused");
    awaiting.status =
        crate::subagent::SubagentOutcomeKind::AwaitingInput(crate::subagent::SubagentPause {
            reason: "clarification".into(),
            resume: crate::subagent::SubagentResume::default(),
        });
    jobs.mark_driver_result(&awaiting_id, Ok(awaiting));

    for index in 0..2_000 {
        let (_, id) = register_driver_job(&jobs, format!("settled-{index}"));
        jobs.mark_driver_result(&id, Err("finished".into()));
    }

    assert!(jobs.tasks().len().unwrap() < 2_002);
    assert!(jobs.get(active_id.as_str()).is_some());
    assert_eq!(
        jobs.get(awaiting_id.as_str()).unwrap().status,
        SubAgentJobStatus::AwaitingInput
    );
    assert_eq!(
        jobs.driver_job_for_key(&active_key, "owner"),
        Some(active_id)
    );
    assert_eq!(
        jobs.driver_job_for_key(&awaiting_key, "owner"),
        Some(awaiting_id.clone())
    );

    let (resumed_id, _, _, _, should_run) = jobs
        .register_driver_job(DriverJobRegistration {
            key: awaiting_key,
            agent: "worker".into(),
            owner: "owner".into(),
            workspace: None,
            cancellation: tinyagents_harness::cancel::CancellationToken::new(),
            steering: None,
            run_queue: None,
            continuing: true,
        })
        .unwrap();
    assert!(should_run);
    assert_eq!(resumed_id, awaiting_id);
    assert_eq!(
        jobs.get(resumed_id.as_str()).unwrap().status,
        SubAgentJobStatus::Running
    );
}

#[tokio::test]
async fn waiter_keeps_terminal_snapshot_when_pruning_races_its_observation() {
    use std::future::Future;

    let jobs = SubAgentJobRegistry::new();
    let (key, id) = register_driver_job(&jobs, "awaited");
    jobs.mark_running(&id);

    let waiter_jobs = jobs.clone();
    let waiter_id = id.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let wait = waiter_jobs.wait_owned(waiter_id.as_str(), "owner");
        tokio::pin!(wait);
        let mut started_tx = Some(started_tx);
        std::future::poll_fn(|cx| {
            let result = wait.as_mut().poll(cx);
            if result.is_pending()
                && let Some(started_tx) = started_tx.take()
            {
                let _ = started_tx.send(());
            }
            result
        })
        .await
    });
    started_rx.await.unwrap();

    jobs.mark_driver_result(
        &id,
        Ok(crate::subagent::SubagentOutcome::completed(
            "awaited",
            "preserved output",
        )),
    );
    for index in 0..2_000 {
        let (_, pressure_id) = register_driver_job(&jobs, format!("pressure-{index}"));
        jobs.mark_driver_result(&pressure_id, Err("finished".into()));
    }

    assert_eq!(jobs.driver_job_for_key(&key, "owner"), Some(id.clone()));
    let observed = waiter.await.unwrap().unwrap();
    assert_eq!(observed.id, id);
    assert_eq!(observed.status, SubAgentJobStatus::Completed);
    assert_eq!(observed.output.as_deref(), Some("preserved output"));
    assert!(jobs.driver_job_for_key(&key, "owner").is_none());
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
