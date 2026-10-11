//! Native agent records. Merge rules match the classic ledger without workspace lookup.
use super::*;
use super::storage::AGENTS;

impl DriverRunLedger {
    pub(super) async fn agent(&self, id: &str) -> Result<Option<AgentRun>> {
        let Some(row) = self.read::<AgentRun>(AGENTS, id).await? else { return Ok(None); };
        Ok(Some(self.agent_telemetry(row.doc).await?))
    }

    pub(super) async fn agent_telemetry(&self, mut row: AgentRun) -> Result<AgentRun> {
        row.telemetry = self.read::<RunTelemetry>(TELEMETRY, &row.id).await?.map(|t| t.doc);
        Ok(row)
    }

    pub(super) async fn upsert_agent(&self, upsert: AgentRunUpsert) -> Result<AgentRun> {
        let now = Utc::now();
        let row = self.change(AGENTS, &upsert.id, |previous: Option<AgentRun>| {
            let first = previous.is_none();
            let mut row = previous.unwrap_or(AgentRun {
                id: upsert.id.clone(), kind: upsert.kind, parent_run_id: None, parent_thread_id: None,
                agent_id: None, status: upsert.status, prompt_ref: None, worker_thread_id: None,
                checkpoint_path: None, checkpoint: None, summary: None, error: None,
                metadata: upsert.metadata.clone(), telemetry: None,
                started_at: upsert.started_at.unwrap_or(now), updated_at: now, completed_at: None,
            });
            if !(row.kind == AgentRunKind::WorkerThread && upsert.kind == AgentRunKind::Subagent) {
                row.kind = upsert.kind;
            }
            row.parent_run_id = upsert.parent_run_id.clone().or(row.parent_run_id);
            row.parent_thread_id = upsert.parent_thread_id.clone().or(row.parent_thread_id);
            row.agent_id = upsert.agent_id.clone().or(row.agent_id);
            row.status = upsert.status;
            row.prompt_ref = upsert.prompt_ref.clone().or(row.prompt_ref);
            row.worker_thread_id = upsert.worker_thread_id.clone().or(row.worker_thread_id);
            row.checkpoint_path = upsert.checkpoint_path.clone().or(row.checkpoint_path);
            row.checkpoint = upsert.checkpoint.clone().or(row.checkpoint);
            row.summary = upsert.summary.clone().or(row.summary);
            row.error = upsert.error.clone().or(row.error);
            if first || upsert.metadata != serde_json::json!({}) { row.metadata = upsert.metadata.clone(); }
            row.completed_at = upsert.completed_at.or(row.completed_at);
            row.updated_at = now;
            Ok((Some(row.clone()), row))
        }).await?;
        self.agent_telemetry(row).await
    }
}
