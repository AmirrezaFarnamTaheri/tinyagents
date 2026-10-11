//! Async control consumer of the bound ledger port; no workspace resolution.
use super::control::{ControlError, ControlVerb, plan_transition};
use super::view::project_row;
use super::AgentWorkRow;
use crate::run_ledger::{LedgerResult, RunEventAppend, RunLedgerStore, RunTransition};
use chrono::Utc;
use serde_json::json;

pub(crate) async fn apply_control(
    ledger: &dyn RunLedgerStore, run_id: &str, verb: ControlVerb,
    message: Option<&str>, reason: Option<&str>, display_name: &(dyn Fn(&str) -> Option<String> + Sync),
) -> LedgerResult<AgentWorkRow> {
    let message = message.map(str::trim).filter(|s| !s.is_empty());
    let reason = reason.map(str::trim).filter(|s| !s.is_empty());
    if verb.requires_message() && message.is_none() { return Err(ControlError::MessageRequired(verb.as_str()).into()); }
    let mut run = ledger.get_agent_run(run_id).await?.ok_or_else(|| ControlError::RunNotFound(run_id.into()))?;
    for _ in 0..3 {
        let plan = plan_transition(run.status, verb)?;
        let (error, completed) = match verb {
            ControlVerb::Stop => (reason.map(str::to_owned), Some(Utc::now())),
            ControlVerb::Retry | ControlVerb::Continue => (None, None),
            ControlVerb::FollowUp => (run.error.clone(), run.completed_at),
        };
        let event = RunEventAppend { run_id: run_id.into(), event_type: plan.event_type.into(),
            payload: json!({"verb":verb.as_str(),"fromStatus":run.status.as_str(),"toStatus":plan.target_status.as_str(),"message":message,"reason":reason}) };
        match ledger.transition_agent_run_status_from(run_id, run.status, plan.target_status, error.as_deref(), completed, Some(event)).await? {
            RunTransition::Applied(updated) => return Ok(project_row(updated, display_name)),
            RunTransition::NotFound => return Err(ControlError::RunNotFound(run_id.into()).into()),
            RunTransition::StatusChanged(current) => run = current,
        }
    }
    Err(ControlError::InvalidTransition { verb: verb.as_str(), status: run.status.as_str() }.into())
}
