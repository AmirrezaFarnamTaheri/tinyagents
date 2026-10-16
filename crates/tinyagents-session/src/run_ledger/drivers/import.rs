//! Agent-family import with an operation receipt.
//!
//! [`DriverRunLedger::import_agent_family`] imports runs, events and telemetry
//! of a quiescent classic source in **one atomic batch** together with a
//! receipt keyed by the caller's operation id, so a retry of a committed
//! operation is a no-op and a changed source cannot reuse the id. For the whole
//! database, only-if-absent and without a receipt, see
//! [`DriverRunLedger::import_legacy_database`].
use super::*;
use crate::legacy::{LegacySourceSnapshot, QuiescentSource, rows, source};
use tinystoragedrivers_core::{ErrorKind, MIGRATIONS_COLLECTION, Progress};

/// The `tsd.migrations` step (owner `ledger`) that records the agent-family
/// import `operation_id`.
pub(super) fn import_step(operation_id: &str) -> String {
    format!("agent_import/{operation_id}")
}

/// The id of an import's receipt in `tsd.migrations`: `Key::of([owner, step])`,
/// the id [`tinystoragedrivers_core::Migrations::mark_done`] gives an importer.
pub(super) fn import_receipt_id(operation_id: &str) -> std::result::Result<String, StorageError> {
    super::storage::key(&[LEDGER_OWNER, &import_step(operation_id)])
}

impl DriverRunLedger {
    /// Inspect an admitted source without creating, migrating or modifying any source file.
    pub fn preflight_source(&self, source: &QuiescentSource) -> Result<LegacySourceSnapshot> {
        source::open_source(source, self.scope())
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;

/// Counts committed together with an agent-family import receipt.
///
/// The receipt is the importer's `tsd.migrations` record
/// (`Migrations::done("ledger", "agent_import/<operation id>")`), written in the
/// same atomic batch as the rows. Its `cursor` carries the source fingerprint,
/// so an operation id cannot be reused for a changed source, and its `written`
/// the total row count; the per-family counts of a replayed operation are
/// recomputed from the (unchanged) source.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentImportReport {
    /// Agent run rows committed by this operation.
    pub agent_runs: usize,
    /// Event receipts committed with their original ordinals.
    pub events: usize,
    /// Run telemetry rows committed by this operation.
    pub telemetry: usize,
}

impl DriverRunLedger {
    /// Import the agent family from a trusted quiescent classic source.
    ///
    /// # Errors
    /// Source authority, unsupported schema, or native transaction refusal.
    pub async fn import_agent_family(
        &self,
        source: &QuiescentSource,
        operation_id: &str,
    ) -> Result<AgentImportReport> {
        tinystoragedrivers_core::ReadCheck {
            collection: MIGRATIONS_COLLECTION.into(),
            id: import_receipt_id(operation_id)?,
            expected: tinystoragedrivers_core::ExpectedDocument::Absent,
        }
        .validate()?;
        let loaded = self.load_agent_import(source)?;
        self.commit_agent_import(loaded, operation_id).await
    }
}

struct LoadedAgentImport {
    fingerprint: String,
    runs: Vec<AgentRun>,
    events: Vec<RunEvent>,
    telemetry: Vec<RunTelemetry>,
}

impl DriverRunLedger {
    fn load_agent_import(&self, source: &QuiescentSource) -> Result<LoadedAgentImport> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let snapshot = self.preflight_source(source)?;
        // This operation imports only the agent family from its current known layout.
        // Historical discovery never fabricates absent columns or rewrites rows.
        for table in ["agent_runs", "run_events", "run_telemetry"] {
            if !snapshot.has_table(table) {
                return Err(StorageError::invalid_input(
                    "agent import requires its declared source tables",
                )
                .into());
            }
        }
        let runs = rows::agent_runs(&snapshot)?;
        let events = rows::run_events(&snapshot)?;
        let telemetry = rows::telemetry(&snapshot)?;
        for row in &runs {
            super::storage::encode(row)?;
        }
        for row in &events {
            super::storage::encode(row)?;
        }
        for row in &telemetry {
            super::storage::encode(row)?;
        }
        // A content digest identifies a receipt; only SourceOwner establishes tenancy.
        let mut file = std::fs::File::open(&source.path)
            .map_err(|error| StorageError::backend("read source digest").with_source(error))?;
        let mut digest = Sha256::new();
        let mut buffer = [0; 65536];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| StorageError::backend("read source digest").with_source(error))?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(LoadedAgentImport {
            fingerprint: hex::encode(digest.finalize()),
            runs,
            events,
            telemetry,
        })
    }

    /// The recorded fingerprint of a committed import, if `operation_id` has one.
    pub(super) async fn import_receipt(&self, operation_id: &str) -> Result<Option<Progress>> {
        let found = self
            .scoped
            .documents()
            .get(MIGRATIONS_COLLECTION, &import_receipt_id(operation_id)?)
            .await?;
        found
            .map(|found| serde_json::from_value(found.doc).map_err(StorageError::from))
            .transpose()
            .map_err(Into::into)
    }

    async fn commit_agent_import(
        &self,
        loaded: LoadedAgentImport,
        operation_id: &str,
    ) -> Result<AgentImportReport> {
        use super::storage::{AGENTS, EVENT_HEADS, EVENTS, TELEMETRY, key, put_op};
        use tinystoragedrivers_core::{Precondition, WriteOp};
        self.declare().await?;
        let report = AgentImportReport {
            agent_runs: loaded.runs.len(),
            events: loaded.events.len(),
            telemetry: loaded.telemetry.len(),
        };
        if let Some(receipt) = self.import_receipt(operation_id).await? {
            if receipt.cursor.as_deref() != Some(loaded.fingerprint.as_str()) {
                return Err(StorageError::conflict("import operation source changed").into());
            }
            return Ok(report);
        }
        let mut writes = Vec::new();
        for mut run in loaded.runs {
            run.telemetry = None;
            writes.push(put_op(
                AGENTS,
                key(&[&run.id])?,
                &run,
                Precondition::Absent,
            )?);
        }
        for telemetry in loaded.telemetry {
            writes.push(put_op(
                TELEMETRY,
                key(&[&telemetry.run_id])?,
                &telemetry,
                Precondition::Absent,
            )?);
        }
        let mut heads = std::collections::BTreeMap::<String, u64>::new();
        for event in loaded.events {
            if event.sequence == 0 || event.sequence > i64::MAX as u64 {
                return Err(StorageError::serialization(
                    "import event sequence exceeds classic range",
                )
                .into());
            }
            let next = event.sequence + 1;
            heads
                .entry(event.run_id.clone())
                .and_modify(|current| *current = (*current).max(next))
                .or_insert(next);
            writes.push(put_op(
                EVENTS,
                key(&[&event.run_id, &event.sequence.to_string()])?,
                &event,
                Precondition::Absent,
            )?);
        }
        for (run_id, next) in heads {
            writes.push(put_op(
                EVENT_HEADS,
                key(&[&run_id])?,
                &super::events::EventHead {
                    next: (next <= i64::MAX as u64).then_some(next),
                },
                Precondition::Absent,
            )?);
        }
        let receipt = Progress {
            owner: LEDGER_OWNER.into(),
            step: import_step(operation_id),
            from: 0,
            to: 0,
            cursor: Some(loaded.fingerprint.clone()),
            written: (report.agent_runs + report.events + report.telemetry) as u64,
            done: true,
        };
        writes.push(WriteOp::Put {
            collection: MIGRATIONS_COLLECTION.into(),
            id: import_receipt_id(operation_id)?,
            doc: serde_json::to_value(&receipt).map_err(StorageError::from)?,
            precondition: Precondition::Absent,
        });
        match self
            .scoped
            .documents()
            .atomic_batch_checked(vec![], writes)
            .await
        {
            Ok(_) => Ok(report),
            Err(error) if error.kind() == ErrorKind::Conflict => {
                if let Some(receipt) = self.import_receipt(operation_id).await?
                    && receipt.cursor.as_deref() == Some(loaded.fingerprint.as_str())
                {
                    return Ok(report);
                }
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }
}
