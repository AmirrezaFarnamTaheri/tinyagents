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

/// Team claims retain dependency gates and reject a released claimant after another member takes over.
#[test]
fn adapter_team_claim_and_completion_preserve_dependency_and_owner_gates() {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    ledger
        .upsert_agent_team(AgentTeamUpsert {
            id: "team".into(),
            parent_thread_id: None,
            lead_agent_id: "lead".into(),
            status: AgentTeamStatus::Active,
            summary: None,
            created_at: None,
            closed_at: None,
        })
        .unwrap();
    for (id, depends_on) in [("first", vec![]), ("second", vec!["first".into()])] {
        ledger
            .upsert_agent_team_task(AgentTeamTaskUpsert {
                id: id.into(),
                team_id: "team".into(),
                title: id.into(),
                objective: None,
                status: AgentTeamTaskStatus::Todo,
                owner_member_id: None,
                depends_on,
                gate_status: None,
                gate_reason: None,
                evidence: vec![],
                source_run_id: None,
                order_index: 0,
                created_at: None,
            })
            .unwrap();
    }
    assert!(matches!(
        ledger
            .claim_agent_team_task("team", "second", "old", "token-old")
            .unwrap(),
        ClaimOutcome::Blocked { .. }
    ));
    assert!(matches!(
        ledger
            .claim_agent_team_task("team", "first", "old", "token-old")
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert!(matches!(
        ledger
            .claim_agent_team_task("team", "first", "new", "token-new")
            .unwrap(),
        ClaimOutcome::AlreadyClaimed
    ));
    assert!(matches!(
        ledger
            .complete_agent_team_task("team", "first", "old", &[], true)
            .unwrap(),
        CompletionOutcome::GateFailed { .. }
    ));
    assert!(ledger.release_agent_team_task("team", "first").unwrap());
    assert!(matches!(
        ledger
            .claim_agent_team_task("team", "first", "new", "token-new")
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert!(matches!(
        ledger
            .complete_agent_team_task("team", "first", "old", &["proof".into()], true)
            .unwrap(),
        CompletionOutcome::NotClaimed
    ));
    assert!(matches!(
        ledger
            .complete_agent_team_task("team", "first", "new", &["proof".into()], true)
            .unwrap(),
        CompletionOutcome::Completed(_)
    ));
    assert!(matches!(
        ledger
            .claim_agent_team_task("team", "second", "new", "token-second")
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert_eq!(ledger.list_agent_team_tasks("team").unwrap().len(), 2);
}

/// Entry trees and effects round-trip across adapter recreation without crossing bound workspaces.
#[test]
fn adapter_tree_effects_and_telemetry_preserve_classic_storage() {
    use crate::entry_tree::{CustomEntry, EntryKind};
    let dir = tempfile::tempdir().unwrap();
    let other_dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    let other: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(other_dir.path()));
    let kind = EntryKind::Custom(CustomEntry {
        kind: "note".into(),
        payload: json!({"text":"record"}),
        display: None,
    });
    let first = ledger
        .entry_append_to_head("session", kind.clone())
        .unwrap();
    let second = ledger.entry_append("session", Some(&first), kind).unwrap();
    assert_eq!(
        ledger
            .entry_get("session", &first)
            .unwrap()
            .unwrap()
            .ordinal,
        0
    );
    assert_eq!(
        ledger
            .entry_get("session", &second)
            .unwrap()
            .unwrap()
            .ordinal,
        1
    );
    let entries = ledger.entry_ancestor_chain("session", &second).unwrap();
    ledger.entry_import_legacy("session", &entries).unwrap();
    assert_eq!(
        ledger.entry_ancestor_chain("session", &second).unwrap(),
        entries
    );
    ledger
        .record_tool_started(ToolEffectStart {
            run_id: "run".into(),
            call_id: "call".into(),
            tool: "write".into(),
            idempotency_key: Some("key".into()),
            effect_summary: None,
        })
        .unwrap();
    let effects = ledger.list_unresolved_tool_effects("run").unwrap();
    let tail = vec![DanglingToolCall {
        run_id: "run".into(),
        call_id: "call".into(),
        tool: "write".into(),
    }];
    assert_eq!(
        ledger.overall_recovery(&ledger.classify_recovery(&tail, &effects)),
        RecoveryClass::NeedsVerification
    );
    ledger
        .mark_tool_effect_interrupted("run", "call")
        .unwrap()
        .unwrap();
    let first_telemetry = ledger
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: "run".into(),
            input_tokens: Some(42),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first_telemetry.input_tokens, 42);
    let merged = ledger
        .upsert_run_telemetry(RunTelemetryUpsert {
            run_id: "run".into(),
            model: Some("model".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(merged.input_tokens, 42);
    drop(ledger);
    let reopened: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    assert_eq!(reopened.entry_head("session").unwrap(), Some(second));
    assert!(other.entry_head("session").unwrap().is_none());
    assert!(
        other
            .list_unresolved_tool_effects("run")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened
            .prune_run_telemetry_before(chrono::Utc::now() + Duration::minutes(1))
            .unwrap(),
        1
    );
    assert!(
        reopened.entry_get("session", &first).unwrap().is_some(),
        "retention must not implicitly prune entry trees"
    );
}

/// Command-center control routes the same status/event transaction through an object handle.
#[test]
fn adapter_control_and_retention_preserve_existing_tables_and_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Arc<dyn RunLedgerStore> = Arc::new(SqliteRunLedger::new(dir.path()));
    seed_agent(ledger.as_ref());
    let stopped = ledger
        .apply_control(
            "agent",
            command_center::ControlVerb::Stop,
            None,
            Some("stop"),
            &|_| None,
        )
        .unwrap();
    assert_eq!(stopped.status, AgentRunStatus::Interrupted.as_str());
    assert_eq!(ledger.list_agent_work(None, &|_| None).unwrap().total, 1);
    let request = RunEventListRequest {
        run_id: "agent".into(),
        after_sequence: None,
        limit: None,
    };
    assert_eq!(ledger.list_recent_run_events(&request).unwrap().count, 1);
    let report = ledger
        .apply_retention(chrono::Utc::now() + Duration::minutes(1))
        .unwrap();
    assert_eq!(report.run_events, 1);
    assert!(
        ledger.get_agent_run("agent").unwrap().is_some(),
        "classic retention does not delete agent run rows"
    );
}
