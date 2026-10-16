use super::*;

use crate::teams::NewMember;
use tempfile::TempDir;
use tinyagents_session::run_ledger::{AgentTeamMemberStatus, AgentTeamTaskStatus};

fn service(dir: &TempDir) -> TeamService<SessionTeamLedger> {
    TeamService::new(
        SessionTeamLedger::new(Arc::new(crate::test_support::ledger_in(dir.path()))).unwrap(),
    )
}

struct FixedExecutor(Result<Vec<String>, &'static str>);

#[async_trait]
impl TeamMemberExecutor for FixedExecutor {
    async fn execute(&self, _: &AgentTeamMember, _: &AgentTeamTask) -> Result<Vec<String>> {
        self.0.clone().map_err(anyhow::Error::msg)
    }
}

struct RecordingExecutor(Arc<std::sync::Mutex<Option<AgentTeamMember>>>);

#[async_trait]
impl TeamMemberExecutor for RecordingExecutor {
    async fn execute(&self, member: &AgentTeamMember, _: &AgentTeamTask) -> Result<Vec<String>> {
        *self.0.lock().unwrap() = Some(member.clone());
        Ok(vec!["done".into()])
    }
}

struct HangingExecutor(Arc<tokio::sync::Notify>);

#[async_trait]
impl TeamMemberExecutor for HangingExecutor {
    async fn execute(&self, _: &AgentTeamMember, _: &AgentTeamTask) -> Result<Vec<String>> {
        self.0.notify_one();
        std::future::pending().await
    }
}

struct PausedExecutor {
    entered: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl TeamMemberExecutor for PausedExecutor {
    async fn execute(&self, _: &AgentTeamMember, _: &AgentTeamTask) -> Result<Vec<String>> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(vec!["stale completion".into()])
    }
}

#[tokio::test]
async fn start_member_runs_and_completes_claimed_task() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service
        .create_team(
            "lead",
            None,
            None,
            &[NewMember {
                name: "alice".into(),
                agent_id: None,
            }],
        )
        .unwrap();
    let member = &team.members[0];
    let task = service
        .assign_task(&team.team.id, "write", None, Some(&member.id), &[])
        .unwrap();

    let executed_member = Arc::new(std::sync::Mutex::new(None));
    let outcome = service
        .start_member(
            &team.team.id,
            &task.id,
            &member.id,
            &RecordingExecutor(executed_member.clone()),
            true,
        )
        .await
        .unwrap();

    assert!(matches!(outcome, CompletionOutcome::Completed(_)));
    let executed_member = executed_member.lock().unwrap().clone().unwrap();
    assert_eq!(executed_member.member_status, AgentTeamMemberStatus::Active);
    assert_eq!(
        executed_member.current_task_id.as_deref(),
        Some(task.id.as_str())
    );
    assert!(executed_member.worker_thread_id.is_some());
    assert!(executed_member.run_id.is_some());
    assert_eq!(
        service.ledger().list_tasks(&team.team.id).unwrap()[0].status,
        AgentTeamTaskStatus::Done
    );
    assert_eq!(
        service.ledger().list_members(&team.team.id).unwrap()[0].member_status,
        AgentTeamMemberStatus::Idle
    );
}

#[tokio::test]
async fn start_member_releases_task_and_idles_member_on_executor_failure() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service
        .create_team(
            "lead",
            None,
            None,
            &[NewMember {
                name: "alice".into(),
                agent_id: None,
            }],
        )
        .unwrap();
    let member = &team.members[0];
    let task = service
        .assign_task(&team.team.id, "write", None, Some(&member.id), &[])
        .unwrap();

    let error = service
        .start_member(
            &team.team.id,
            &task.id,
            &member.id,
            &FixedExecutor(Err("worker stopped")),
            true,
        )
        .await
        .unwrap_err();

    assert!(format!("{error:#}").contains("worker stopped"));
    assert_eq!(
        service.ledger().list_tasks(&team.team.id).unwrap()[0].status,
        AgentTeamTaskStatus::Todo
    );
    assert_eq!(
        service.ledger().list_members(&team.team.id).unwrap()[0].member_status,
        AgentTeamMemberStatus::Idle
    );
}

#[tokio::test]
async fn cancelling_start_member_releases_task_and_idles_member() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service
        .create_team(
            "lead",
            None,
            None,
            &[NewMember {
                name: "alice".into(),
                agent_id: None,
            }],
        )
        .unwrap();
    let member = &team.members[0];
    let task = service
        .assign_task(&team.team.id, "write", None, Some(&member.id), &[])
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let executor = Arc::new(HangingExecutor(entered.clone()));
    let worker_service = service.clone();
    let team_id = team.team.id.clone();
    let task_id = task.id.clone();
    let member_id = member.id.clone();
    let worker = tokio::spawn(async move {
        worker_service
            .start_member(&team_id, &task_id, &member_id, executor.as_ref(), true)
            .await
    });
    entered.notified().await;
    worker.abort();
    let _ = worker.await;

    assert_eq!(
        service.ledger().list_tasks(&team.team.id).unwrap()[0].status,
        AgentTeamTaskStatus::Todo
    );
    assert_eq!(
        service.ledger().list_members(&team.team.id).unwrap()[0].member_status,
        AgentTeamMemberStatus::Idle
    );
}

#[tokio::test]
async fn stale_executor_cannot_complete_successor_claim_for_same_member() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service
        .create_team(
            "lead",
            None,
            None,
            &[NewMember {
                name: "alice".into(),
                agent_id: None,
            }],
        )
        .unwrap();
    let member = &team.members[0];
    let task = service
        .assign_task(&team.team.id, "write", None, Some(&member.id), &[])
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let executor = Arc::new(PausedExecutor {
        entered: entered.clone(),
        resume: resume.clone(),
    });
    let worker_service = service.clone();
    let (team_id, task_id, member_id) = (team.team.id.clone(), task.id.clone(), member.id.clone());
    let worker = tokio::spawn(async move {
        worker_service
            .start_member(&team_id, &task_id, &member_id, executor.as_ref(), false)
            .await
    });
    entered.notified().await;

    service.shutdown_member(&team.team.id, &member.id).unwrap();
    let ClaimOutcome::Claimed(_) = service
        .ledger()
        .claim_task(&team.team.id, &task.id, &member.id, "successor-token")
        .unwrap()
    else {
        panic!("successor should reclaim the released task");
    };
    service
        .ledger()
        .mark_member_running(
            &team.team.id,
            &member.id,
            &task.id,
            "successor-thread",
            "successor-run",
        )
        .unwrap()
        .unwrap();

    resume.notify_one();
    assert!(matches!(
        worker.await.unwrap().unwrap(),
        CompletionOutcome::NotClaimed
    ));
    let current_task = service
        .ledger()
        .list_tasks(&team.team.id)
        .unwrap()
        .remove(0);
    assert_eq!(current_task.status, AgentTeamTaskStatus::InProgress);
    assert_eq!(current_task.claim_token.as_deref(), Some("successor-token"));
    assert!(current_task.evidence.is_empty());
    let current_member = service
        .ledger()
        .list_members(&team.team.id)
        .unwrap()
        .remove(0);
    assert_eq!(current_member.member_status, AgentTeamMemberStatus::Active);
    assert_eq!(current_member.run_id.as_deref(), Some("successor-run"));
}

#[test]
fn stale_claim_cleanup_cannot_release_a_successor_claim() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service
        .create_team(
            "lead",
            None,
            None,
            &[NewMember {
                name: "alice".into(),
                agent_id: None,
            }],
        )
        .unwrap();
    let member = &team.members[0];
    let task = service
        .assign_task(&team.team.id, "write", None, Some(&member.id), &[])
        .unwrap();
    let ClaimOutcome::Claimed(first_claim) = service
        .claim_task(&team.team.id, &task.id, &member.id, "first-generation")
        .unwrap()
    else {
        panic!("first generation should claim the task");
    };
    assert!(
        service
            .ledger()
            .release_task(&team.team.id, &task.id, &member.id, "first-generation")
            .unwrap()
    );
    let ClaimOutcome::Claimed(second_claim) = service
        .claim_task(&team.team.id, &task.id, &member.id, "successor-generation")
        .unwrap()
    else {
        panic!("successor should claim the released task");
    };

    assert!(
        !service
            .ledger()
            .release_task(&team.team.id, &task.id, &member.id, "first-generation")
            .unwrap()
    );
    let current = service
        .ledger()
        .list_tasks(&team.team.id)
        .unwrap()
        .remove(0);
    assert_eq!(current.claim_token.as_deref(), Some("successor-generation"));
    assert_eq!(current.claim_token, second_claim.claim_token);
    assert_ne!(first_claim.claim_token, second_claim.claim_token);
}
