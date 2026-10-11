use super::*;
use crate::run_ledger::{port::RunLedgerStore, *};
use chrono::Duration;
use serde_json::json;
use std::sync::{Arc, Barrier};

/// A running workflow with no claimed driver.
fn workflow(id: &str, status: WorkflowRunStatus) -> WorkflowRunUpsert {
    WorkflowRunUpsert {
        id: id.into(),
        definition_id: "definition".into(),
        parent_thread_id: None,
        input: json!({}),
        phase_states: json!({}),
        child_run_ids: vec![],
        status,
        summary: None,
        started_at: None,
        completed_at: None,
    }
}

/// Seed one run through an object-safe provider handle.
fn seed_agent(ledger: &dyn RunLedgerStore) {
    ledger
        .upsert_agent_run(AgentRunUpsert {
            id: "agent".into(),
            kind: AgentRunKind::Subagent,
            parent_run_id: None,
            parent_thread_id: None,
            agent_id: None,
            status: AgentRunStatus::Running,
            prompt_ref: None,
            worker_thread_id: None,
            checkpoint_path: None,
            checkpoint: None,
            summary: None,
            error: None,
            metadata: json!({}),
            started_at: None,
            completed_at: None,
        })
        .unwrap();
}

/// Failed event insertion rolls back its status update; successful CAS records both once.
#[test]
fn adapter_status_and_event_commit_or_rollback_together() {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    seed_agent(ledger.as_ref());
    crate::store::with_connection(dir.path(), |conn| {
        conn.execute_batch("CREATE TRIGGER reject_run_event BEFORE INSERT ON run_events BEGIN SELECT RAISE(ABORT, 'injected event failure'); END;")?;
        Ok(())
    }).unwrap();
    let event = RunEventAppend {
        run_id: "agent".into(),
        event_type: "stop".into(),
        payload: json!({}),
    };
    assert!(
        ledger
            .transition_agent_run_status_from(
                "agent",
                AgentRunStatus::Running,
                AgentRunStatus::Interrupted,
                None,
                None,
                Some(event.clone())
            )
            .is_err()
    );
    assert_eq!(
        ledger.get_agent_run("agent").unwrap().unwrap().status,
        AgentRunStatus::Running
    );
    assert!(
        ledger
            .list_recent_run_events(&RunEventListRequest {
                run_id: "agent".into(),
                after_sequence: None,
                limit: None
            })
            .unwrap()
            .events
            .is_empty()
    );
    crate::store::with_connection(dir.path(), |conn| {
        conn.execute_batch("DROP TRIGGER reject_run_event")?;
        Ok(())
    })
    .unwrap();
    assert!(matches!(
        ledger
            .transition_agent_run_status_from(
                "agent",
                AgentRunStatus::Running,
                AgentRunStatus::Interrupted,
                None,
                None,
                Some(event.clone())
            )
            .unwrap(),
        RunTransition::Applied(_)
    ));
    assert!(matches!(
        ledger
            .transition_agent_run_status_from(
                "agent",
                AgentRunStatus::Running,
                AgentRunStatus::Interrupted,
                None,
                None,
                Some(event)
            )
            .unwrap(),
        RunTransition::StatusChanged(_)
    ));
    let events = ledger
        .list_recent_run_events(&RunEventListRequest {
            run_id: "agent".into(),
            after_sequence: None,
            limit: None,
        })
        .unwrap();
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].sequence, 1);
}

/// Concurrent drivers acquire one lease; lifecycle changes fence the previous owner.
#[test]
fn adapter_workflow_claims_and_lifecycle_reject_stale_owners() {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    ledger
        .upsert_workflow_run(workflow("workflow", WorkflowRunStatus::Running))
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let workers: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|owner| {
            let ledger = ledger.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                ledger
                    .try_claim_workflow_run("workflow", owner, Duration::minutes(5))
                    .unwrap()
            })
        })
        .collect();
    let claims: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        claims
            .iter()
            .filter(|claim| matches!(claim, WorkflowLeaseClaim::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        claims
            .iter()
            .filter(|claim| matches!(claim, WorkflowLeaseClaim::Busy(_)))
            .count(),
        1
    );
    let acquired = claims
        .into_iter()
        .find_map(|claim| match claim {
            WorkflowLeaseClaim::Acquired(row) => Some(row),
            _ => None,
        })
        .unwrap();
    let owner = acquired.lease_owner.as_deref().unwrap();
    assert!(
        !ledger
            .renew_workflow_run_lease("workflow", "stranger", Duration::minutes(5))
            .unwrap()
    );
    assert!(
        ledger
            .renew_workflow_run_lease("workflow", owner, Duration::minutes(5))
            .unwrap()
    );
    assert!(
        ledger
            .compare_and_swap_workflow_run(
                workflow("workflow", WorkflowRunStatus::Running),
                acquired.revision,
                "stranger",
                Duration::minutes(5)
            )
            .unwrap()
            .is_none()
    );
    let changed = ledger
        .compare_and_swap_workflow_run(
            workflow("workflow", WorkflowRunStatus::Running),
            acquired.revision,
            owner,
            Duration::minutes(5),
        )
        .unwrap()
        .unwrap();
    assert!(
        ledger
            .compare_and_swap_workflow_run_lifecycle(
                workflow("workflow", WorkflowRunStatus::Interrupted),
                acquired.revision
            )
            .unwrap()
            .is_none()
    );
    let stopped = ledger
        .compare_and_swap_workflow_run_lifecycle(
            workflow("workflow", WorkflowRunStatus::Interrupted),
            changed.revision,
        )
        .unwrap()
        .unwrap();
    assert!(stopped.lease_owner.is_none());
    assert!(
        !ledger
            .renew_workflow_run_lease("workflow", owner, Duration::minutes(5))
            .unwrap()
    );
    let replacement = ledger
        .try_claim_workflow_run("workflow", "replacement", Duration::minutes(5))
        .unwrap();
    assert!(matches!(replacement, WorkflowLeaseClaim::Acquired(_)));
    assert!(
        ledger
            .compare_and_swap_workflow_run(
                workflow("workflow", WorkflowRunStatus::Failed),
                changed.revision,
                owner,
                Duration::minutes(5)
            )
            .unwrap()
            .is_none()
    );
}

/// Expired ownership is fenced without relying on scheduler timing or sleeps.
#[test]
fn adapter_expired_claim_takeover_rejects_old_driver() {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    ledger
        .upsert_workflow_run(workflow("workflow", WorkflowRunStatus::Running))
        .unwrap();
    let old = match ledger
        .try_claim_workflow_run("workflow", "old", Duration::minutes(-5))
        .unwrap()
    {
        WorkflowLeaseClaim::Acquired(row) => row,
        other => panic!("unexpected claim: {other:?}"),
    };
    assert!(
        !ledger
            .renew_workflow_run_lease("workflow", "old", Duration::minutes(5))
            .unwrap()
    );
    assert!(matches!(
        ledger
            .try_claim_workflow_run("workflow", "new", Duration::minutes(5))
            .unwrap(),
        WorkflowLeaseClaim::Acquired(_)
    ));
    assert!(
        ledger
            .compare_and_swap_workflow_run(
                workflow("workflow", WorkflowRunStatus::Failed),
                old.revision,
                "old",
                Duration::minutes(5)
            )
            .unwrap()
            .is_none()
    );
}
