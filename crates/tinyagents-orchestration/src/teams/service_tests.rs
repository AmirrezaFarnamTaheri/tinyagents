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
    assert_eq!(executed_member.current_task_id.as_deref(), Some(task.id.as_str()));
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
