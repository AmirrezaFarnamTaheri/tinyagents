//! Mechanical document encoding for the classic per-call tool-effect ledger.
use super::super::tool_effects::ToolEffectStatus;
use super::*;
use serde::{Deserialize, Serialize};
use storage::EFFECTS;

/// Store status strings so unknown persisted values retain the classic conservative parse fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EffectRecord {
    run_id: String,
    call_id: String,
    tool: String,
    status: String,
    idempotency_key: Option<String>,
    effect_summary: Option<String>,
    started_at: DateTime<Utc>,
    settled_at: Option<DateTime<Utc>>,
}

impl EffectRecord {
    fn into_row(self) -> ToolEffectRow {
        ToolEffectRow {
            run_id: self.run_id,
            call_id: self.call_id,
            tool: self.tool,
            status: ToolEffectStatus::parse(&self.status),
            idempotency_key: self.idempotency_key,
            effect_summary: self.effect_summary,
            started_at: self.started_at,
            settled_at: self.settled_at,
        }
    }
}

impl DriverRunLedger {
    pub(super) async fn effect_start(&self, start: ToolEffectStart) -> Result<ToolEffectRow> {
        let now = Utc::now();
        let id = storage::key(&[&start.run_id, &start.call_id]);
        Ok(self
            .merge(EFFECTS, &id, |_: Option<EffectRecord>| EffectRecord {
                run_id: start.run_id.clone(),
                call_id: start.call_id.clone(),
                tool: start.tool.clone(),
                status: ToolEffectStatus::Started.as_str().into(),
                idempotency_key: start.idempotency_key.clone(),
                effect_summary: start.effect_summary.clone(),
                started_at: now,
                settled_at: None,
            })
            .await?
            .into_row())
    }

    pub(super) async fn effect_settle(
        &self,
        settle: ToolEffectSettle,
    ) -> Result<Option<ToolEffectRow>> {
        let now = Utc::now();
        let id = storage::key(&[&settle.run_id, &settle.call_id]);
        Ok(Some(
            self.merge(EFFECTS, &id, |previous: Option<EffectRecord>| {
                let mut record = previous.unwrap_or(EffectRecord {
                    run_id: settle.run_id.clone(),
                    call_id: settle.call_id.clone(),
                    tool: String::new(),
                    status: settle.status.as_str().into(),
                    idempotency_key: None,
                    effect_summary: None,
                    started_at: now,
                    settled_at: None,
                });
                record.status = settle.status.as_str().into();
                record.effect_summary = settle.effect_summary.clone().or(record.effect_summary);
                record.settled_at = Some(now);
                record
            })
            .await?
            .into_row(),
        ))
    }

    pub(super) async fn effect_unresolved(&self, run_id: &str) -> Result<Vec<ToolEffectRow>> {
        let records: Vec<EffectRecord> = self
            .records(
                EFFECTS,
                Query::filter(
                    Filter::eq("record.runId", run_id).and(Filter::eq("record.status", "started")),
                )
                .sort(Sort::asc("startedSql")),
            )
            .await?;
        Ok(records.into_iter().map(EffectRecord::into_row).collect())
    }
}
