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

#[tokio::test]
async fn start_member_runs_and_completes_claimed_task() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service.create_team(
        "lead",
        None,
        None,
        &[NewMember { name: "alice".into(), agent_id: None }],
    ).unwrap();
    let member = &team.members[0];
    let task = service.assign_task(&team.team.id, "write", None, Some(&member.id), &[]).unwrap();

    let outcome = service.start_member(
        &team.team.id,
        &task.id,
        &member.id,
        &FixedExecutor(Ok(vec!["done".into()])),
        true,
    ).await.unwrap();

    assert!(matches!(outcome, CompletionOutcome::Completed(_)));
    assert_eq!(service.ledger().list_tasks(&team.team.id).unwrap()[0].status, AgentTeamTaskStatus::Done);
    assert_eq!(service.ledger().list_members(&team.team.id).unwrap()[0].member_status, AgentTeamMemberStatus::Idle);
}

#[tokio::test]
async fn start_member_releases_task_and_idles_member_on_executor_failure() {
    let dir = TempDir::new().unwrap();
    let service = service(&dir);
    let team = service.create_team(
        "lead",
        None,
        None,
        &[NewMember { name: "alice".into(), agent_id: None }],
    ).unwrap();
    let member = &team.members[0];
    let task = service.assign_task(&team.team.id, "write", None, Some(&member.id), &[]).unwrap();

    let error = service.start_member(
        &team.team.id,
        &task.id,
        &member.id,
        &FixedExecutor(Err("worker stopped")),
        true,
    ).await.unwrap_err();

    assert!(error.to_string().contains("worker stopped"));
    assert_eq!(service.ledger().list_tasks(&team.team.id).unwrap()[0].status, AgentTeamTaskStatus::Todo);
    assert_eq!(service.ledger().list_members(&team.team.id).unwrap()[0].member_status, AgentTeamMemberStatus::Idle);
}
