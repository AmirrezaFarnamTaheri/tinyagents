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
use tinystoragedrivers_core::ErrorKind;

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
            collection: super::storage::IMPORTS.into(),
            id: operation_id.into(),
            expected: tinystoragedrivers_core::ExpectedDocument::Absent,
        }
        .validate()?;
        let loaded = self.load_agent_import(source)?;
        self.commit_agent_import(loaded, operation_id).await
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ImportReceipt {
    fingerprint: String,
    report: AgentImportReport,
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

    async fn commit_agent_import(
        &self,
        loaded: LoadedAgentImport,
        operation_id: &str,
    ) -> Result<AgentImportReport> {
        use super::storage::{AGENTS, EVENT_HEADS, EVENTS, IMPORTS, TELEMETRY, encode, key};
        use tinystoragedrivers_core::{Precondition, WriteOp};
        self.declare().await?;
        if let Some(receipt) = self.read::<ImportReceipt>(IMPORTS, operation_id).await? {
            if receipt.doc.fingerprint != loaded.fingerprint {
                return Err(StorageError::conflict("import operation source changed").into());
            }
            return Ok(receipt.doc.report);
        }
        let report = AgentImportReport {
            agent_runs: loaded.runs.len(),
            events: loaded.events.len(),
            telemetry: loaded.telemetry.len(),
        };
        let put = |collection: &str, id: String, doc| WriteOp::Put {
            collection: collection.into(),
            id,
            doc,
            precondition: Precondition::Absent,
        };
        let mut writes = Vec::new();
        for mut run in loaded.runs {
            run.telemetry = None;
            writes.push(put(AGENTS, key(&[&run.id]), encode(&run)?));
        }
        for telemetry in loaded.telemetry {
            writes.push(put(
                TELEMETRY,
                key(&[&telemetry.run_id]),
                encode(&telemetry)?,
            ));
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
            writes.push(put(
                EVENTS,
                key(&[&event.run_id, &event.sequence.to_string()]),
                encode(&event)?,
            ));
        }
        for (run_id, next) in heads {
            writes.push(put(
                EVENT_HEADS,
                key(&[&run_id]),
                encode(&super::events::EventHead {
                    next: (next <= i64::MAX as u64).then_some(next),
                })?,
            ));
        }
        writes.push(put(
            IMPORTS,
            key(&[operation_id]),
            encode(&ImportReceipt {
                fingerprint: loaded.fingerprint.clone(),
                report: report.clone(),
            })?,
        ));
        match self
            .scoped
            .documents()
            .atomic_batch_checked(vec![], writes)
            .await
        {
            Ok(_) => Ok(report),
            Err(error) if error.kind() == ErrorKind::Conflict => {
                if let Some(receipt) = self.read::<ImportReceipt>(IMPORTS, operation_id).await?
                    && receipt.doc.fingerprint == loaded.fingerprint
                {
                    return Ok(receipt.doc.report);
                }
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }
}
