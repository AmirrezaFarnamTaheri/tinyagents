//! Mechanical document encoding for the classic per-call tool-effect ledger.
use super::super::tool_effects::ToolEffectStatus;
use super::*;
use storage::EFFECTS;

impl DriverRunLedger {
    pub(super) async fn effect_start(&self, start: ToolEffectStart) -> Result<ToolEffectRow> {
        let now = now();
        let id = effect_id(&start.run_id, &start.call_id)?;
        Ok(self
            .merge(EFFECTS, &id, |_: Option<ToolEffectRow>| ToolEffectRow {
                run_id: start.run_id.clone(),
                call_id: start.call_id.clone(),
                tool: start.tool.clone(),
                status: ToolEffectStatus::Started,
                idempotency_key: start.idempotency_key.clone(),
                effect_summary: start.effect_summary.clone(),
                started_at: now,
                settled_at: None,
            })
            .await?)
    }

    pub(super) async fn effect_settle(
        &self,
        settle: ToolEffectSettle,
    ) -> Result<Option<ToolEffectRow>> {
        let now = now();
        let id = effect_id(&settle.run_id, &settle.call_id)?;
        Ok(Some(
            self.merge(EFFECTS, &id, |previous: Option<ToolEffectRow>| {
                let mut record = previous.unwrap_or(ToolEffectRow {
                    run_id: settle.run_id.clone(),
                    call_id: settle.call_id.clone(),
                    tool: String::new(),
                    status: settle.status,
                    idempotency_key: None,
                    effect_summary: None,
                    started_at: now,
                    settled_at: None,
                });
                record.status = settle.status;
                record.effect_summary = settle.effect_summary.clone().or(record.effect_summary);
                record.settled_at = Some(now);
                record
            })
            .await?)
    }

    pub(super) async fn effect_unresolved(&self, run_id: &str) -> Result<Vec<ToolEffectRow>> {
        Ok(self
            .records(
                EFFECTS,
                Query::filter(Filter::eq("run_id", run_id).and(Filter::eq("status", "started")))
                    .sort(Sort::asc("started_ms")),
            )
            .await?)
    }
}
