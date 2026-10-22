//! Host-owned migration writes into the already-bound ledger scope.

use super::events::EventHead;
use super::storage::{
    AGENTS, EFFECTS, EVENTS, LEDGER_OWNER, MEMBERS, TASKS, TEAMS, TELEMETRY, WORKFLOWS, encode,
    key, put_op, workflow_lease_key,
};
use super::*;
use crate::run_ledger::import::{
    AgentFamilyImport, AgentFamilyImportReport, ImportCounts, LegacyLedgerRows,
    LegacyLedgerRowsReport,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tinyagents_storage_kit::primitives::LEASES;
use tinyagents_storage_kit::{
    DocumentStoreExt, ErrorKind, Key, MIGRATIONS_COLLECTION, Precondition, WriteOp,
};

const AGENT_IMPORT_PREFIX: &str = "agent_import";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentImportReceipt {
    owner: String,
    step: String,
    from: u32,
    to: u32,
    cursor: Option<String>,
    written: u64,
    done: bool,
    fingerprint: String,
    report: AgentFamilyImportReport,
}

fn agent_receipt_id(operation_id: &str) -> Result<String> {
    Ok(Key::of([
        LEDGER_OWNER,
        &format!("{AGENT_IMPORT_PREFIX}/{operation_id}"),
    ])?
    .as_str()
    .to_owned())
}

impl DriverRunLedger {
    async fn import_put_if_absent(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
    ) -> Result<bool> {
        match self
            .scoped
            .documents()
            .put(collection, id, doc, Precondition::Absent)
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn agent_import_receipt(&self, operation_id: &str) -> Result<Option<AgentImportReceipt>> {
        Ok(self
            .scoped
            .documents()
            .get_as::<AgentImportReceipt>(MIGRATIONS_COLLECTION, &agent_receipt_id(operation_id)?)
            .await?
            .map(|row| row.doc))
    }

    pub(super) async fn import_agent_family_rows(
        &self,
        import: AgentFamilyImport,
    ) -> Result<AgentFamilyImportReport> {
        self.declare().await?;
        if import.operation_id.trim().is_empty() || import.source_fingerprint.trim().is_empty() {
            return Err(StorageError::invalid_input(
                "agent-family import requires operation id and source fingerprint",
            )
            .into());
        }
        if let Some(receipt) = self.agent_import_receipt(&import.operation_id).await? {
            return if receipt.fingerprint == import.source_fingerprint {
                Ok(receipt.report)
            } else {
                Err(StorageError::conflict("agent import operation source changed").into())
            };
        }

        let report = AgentFamilyImportReport {
            agent_runs: ImportCounts::new(import.runs.len(), import.runs.len()),
            events: ImportCounts::new(import.events.len(), import.events.len()),
            telemetry: ImportCounts::new(import.telemetry.len(), import.telemetry.len()),
        };
        let receipt = AgentImportReceipt {
            owner: LEDGER_OWNER.into(),
            step: format!("{AGENT_IMPORT_PREFIX}/{}", import.operation_id),
            from: 0,
            to: 0,
            cursor: Some(import.source_fingerprint.clone()),
            written: (import.runs.len() + import.events.len() + import.telemetry.len()) as u64,
            done: true,
            fingerprint: import.source_fingerprint.clone(),
            report,
        };
        let receipt_doc = serde_json::to_value(&receipt).map_err(StorageError::from)?;
        let mut writes = Vec::with_capacity(
            import.runs.len() + import.events.len() + import.telemetry.len() + 1,
        );
        for run in &import.runs {
            let mut run = run.clone();
            run.telemetry = None;
            writes.push(put_op(
                AGENTS,
                key(&[&run.id])?,
                &run,
                Precondition::Absent,
            )?);
        }
        let mut heads = std::collections::BTreeMap::<String, u64>::new();
        for event in &import.events {
            if event.sequence == 0 || event.sequence > i64::MAX as u64 {
                return Err(
                    StorageError::invalid_input("legacy event sequence is out of range").into(),
                );
            }
            let next = event.sequence + 1;
            heads
                .entry(event.run_id.clone())
                .and_modify(|value| *value = (*value).max(next))
                .or_insert(next);
            writes.push(put_op(
                EVENTS,
                key(&[&event.run_id, &event.sequence.to_string()])?,
                event,
                Precondition::Absent,
            )?);
        }
        for row in &import.telemetry {
            writes.push(put_op(
                TELEMETRY,
                key(&[&row.run_id])?,
                row,
                Precondition::Absent,
            )?);
        }
        for (run_id, next) in heads {
            writes.push(put_op(
                super::storage::EVENT_HEADS,
                key(&[&run_id])?,
                &EventHead {
                    next: (next <= i64::MAX as u64).then_some(next),
                },
                Precondition::Absent,
            )?);
        }
        writes.push(WriteOp::Put {
            collection: MIGRATIONS_COLLECTION.into(),
            id: agent_receipt_id(&import.operation_id)?,
            doc: receipt_doc,
            precondition: Precondition::Absent,
        });
        match self
            .scoped
            .documents()
            .atomic_batch_checked(Vec::new(), writes)
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::Conflict => {
                if let Some(receipt) = self.agent_import_receipt(&import.operation_id).await? {
                    return if receipt.fingerprint == import.source_fingerprint {
                        Ok(receipt.report)
                    } else {
                        Err(StorageError::conflict("agent import operation source changed").into())
                    };
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        }

        Ok(report)
    }

    pub(super) async fn import_legacy_rows_inner(
        &self,
        import: LegacyLedgerRows,
    ) -> Result<LegacyLedgerRowsReport> {
        self.declare().await?;
        let mut report = LegacyLedgerRowsReport::default();
        let mut imported = 0;
        for run in &import.workflow_runs {
            imported += usize::from(
                self.import_put_if_absent(WORKFLOWS, &key(&[&run.id])?, encode(run)?)
                    .await?,
            );
            if let (Some(holder), Some(expires)) = (&run.lease_owner, run.lease_expires_at) {
                let id = key(&[LEDGER_OWNER, &workflow_lease_key(&run.id)?])?;
                let body = json!({
                    "holder": holder,
                    "epoch": 1,
                    "expires_ms": expires.timestamp_millis(),
                });
                self.import_put_if_absent(LEASES, &id, body).await?;
            }
        }
        report.workflow_runs = ImportCounts::new(import.workflow_runs.len(), imported);

        let mut imported = 0;
        for team in &import.teams {
            imported += usize::from(
                self.import_put_if_absent(TEAMS, &key(&[&team.id])?, encode(team)?)
                    .await?,
            );
        }
        report.teams = ImportCounts::new(import.teams.len(), imported);

        let mut imported = 0;
        for member in &import.members {
            imported += usize::from(
                self.import_put_if_absent(MEMBERS, &key(&[&member.id])?, encode(member)?)
                    .await?,
            );
        }
        report.members = ImportCounts::new(import.members.len(), imported);

        let mut imported = 0;
        for task in &import.tasks {
            imported += usize::from(
                self.import_put_if_absent(TASKS, &key(&[&task.id])?, encode(task)?)
                    .await?,
            );
        }
        report.tasks = ImportCounts::new(import.tasks.len(), imported);

        let mut imported = 0;
        for effect in &import.tool_effects {
            imported += usize::from(
                self.import_put_if_absent(
                    EFFECTS,
                    &key(&[&effect.run_id, &effect.call_id])?,
                    encode(effect)?,
                )
                .await?,
            );
        }
        report.tool_effects = ImportCounts::new(import.tool_effects.len(), imported);
        Ok(report)
    }
}
