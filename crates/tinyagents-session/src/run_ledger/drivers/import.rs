//! Source admission and inspection before any target-side import writes.
use super::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::{Path, PathBuf};
use tinystoragedrivers_core::ErrorKind;

/// The tenant identity the host has independently established for a classic source.
///
/// This assertion must come from trusted host configuration, never model arguments,
/// source rows, or a path digest. The library cannot authenticate a local file owner.
#[derive(Debug, Clone)]
pub struct SourceOwner {
    scope: Scope,
}
impl SourceOwner {
    /// Assert a source owner after the trusted host has validated its mapping.
    pub fn asserted_by_trusted_host(scope: Scope) -> Self {
        Self { scope }
    }
}

/// A checkpointed source whose writers the host has stopped for the whole inspection/import.
#[derive(Debug, Clone)]
pub struct QuiescentSource {
    path: PathBuf,
    owner: SourceOwner,
}
impl QuiescentSource {
    /// Assert exclusive quiescence; the importer never checkpoints or repairs this source.
    pub fn asserted_by_trusted_host(path: impl Into<PathBuf>, owner: SourceOwner) -> Self {
        Self {
            path: path.into(),
            owner,
        }
    }
    /// The host-selected SQLite source path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A read-only source snapshot retained for later schema-aware record discovery.
#[derive(Debug)]
pub struct LegacySourceSnapshot {
    pub schema_version: i64,
    pub tables: Vec<String>,
    connection: Connection,
}

impl DriverRunLedger {
    /// Inspect an admitted source without creating, migrating or modifying any source file.
    pub fn preflight_source(&self, source: &QuiescentSource) -> Result<LegacySourceSnapshot> {
        if &source.owner.scope != self.scope() {
            return Err(StorageError::invalid_input(
                "legacy ledger source owner differs from target scope",
            )
            .into());
        }
        let path = std::fs::canonicalize(&source.path).map_err(|error| {
            StorageError::new(
                if error.kind() == std::io::ErrorKind::NotFound {
                    ErrorKind::NotFound
                } else {
                    ErrorKind::Backend
                },
                "resolve legacy ledger source",
            )
            .with_source(error)
        })?;
        if !path.is_file() {
            return Err(StorageError::invalid_input("legacy ledger source is not a file").into());
        }
        // immutable=1 suppresses even SQLite shared-memory bookkeeping. It is safe
        // only under the host quiescence assertion and with no recovery sidecars.
        for suffix in ["-wal", "-journal"] {
            let mut sidecar = path.as_os_str().to_os_string();
            sidecar.push(suffix);
            match std::fs::metadata(PathBuf::from(sidecar)) {
                Ok(_) => return Err(super::super::LedgerError::SourceRequiresSnapshot { path }),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(StorageError::new(
                        ErrorKind::Backend,
                        "inspect legacy ledger recovery sidecar",
                    )
                    .with_source(error)
                    .into());
                }
            }
        }
        let path_text = path.to_str().ok_or_else(|| {
            StorageError::invalid_input("legacy ledger source path must be UTF-8")
        })?;
        let mut uri = String::from("file:");
        for byte in path_text.bytes() {
            if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
                uri.push(char::from(byte));
            } else {
                use std::fmt::Write;
                write!(&mut uri, "%{byte:02X}").expect("String formatting cannot fail");
            }
        }
        uri.push_str("?mode=ro&immutable=1");
        let connection = Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(source_sql_error)?;
        connection
            .execute_batch("BEGIN")
            .map_err(source_sql_error)?;
        let tables: Vec<String> = {
            let mut statement = connection
                .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
                .map_err(source_sql_error)?;
            statement
                .query_map([], |row| row.get(0))
                .map_err(source_sql_error)?
                .collect::<std::result::Result<_, _>>()
                .map_err(source_sql_error)?
        };
        let schema_version = if tables.iter().any(|table| table == "schema_version") {
            connection
                .query_row(
                    "SELECT version FROM schema_version WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(source_sql_error)?
                .unwrap_or(-1)
        } else {
            -1
        };
        if !(-1..=7).contains(&schema_version) {
            return Err(StorageError::invalid_input(
                "legacy ledger schema version is newer than supported discovery",
            )
            .into());
        }
        Ok(LegacySourceSnapshot {
            schema_version,
            tables,
            connection,
        })
    }
}

impl LegacySourceSnapshot {
    /// Discover source column names without invoking any classic schema migration.
    pub fn columns(&self, table: &str) -> Result<Vec<String>> {
        if !self.tables.iter().any(|known| known == table) {
            return Err(StorageError::not_found("legacy ledger source table is absent").into());
        }
        let mut statement = self
            .connection
            .prepare("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
            .map_err(source_sql_error)?;
        Ok(statement
            .query_map([table], |row| row.get(0))
            .map_err(source_sql_error)?
            .collect::<std::result::Result<_, _>>()
            .map_err(source_sql_error)?)
    }
}

fn source_sql_error(error: rusqlite::Error) -> StorageError {
    StorageError::new(ErrorKind::Backend, "inspect immutable legacy ledger source")
        .with_source(error)
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;

/// Counts committed together with an agent-family import receipt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AgentImportReport {
    pub agent_runs: usize,
    pub events: usize,
    pub telemetry: usize,
}

impl DriverRunLedger {
    /// Import the agent family from a trusted quiescent classic source.
    ///
    /// # Errors
    /// Source authority, unsupported schema, or native transaction refusal.
    pub async fn import_agent_family(&self, source: &QuiescentSource, operation_id: &str) -> Result<AgentImportReport> {
        tinystoragedrivers_core::document::validate_id(operation_id)?;
        let loaded = self.load_agent_import(source)?;
        self.commit_agent_import(loaded, operation_id).await
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ImportReceipt { fingerprint: String, report: AgentImportReport }
struct LoadedAgentImport {
    fingerprint: String,
    runs: Vec<AgentRun>, events: Vec<RunEvent>, telemetry: Vec<RunTelemetry>,
}

impl DriverRunLedger {
    fn load_agent_import(&self, source: &QuiescentSource) -> Result<LoadedAgentImport> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let snapshot = self.preflight_source(source)?;
        // This slice imports only the agent family from its current known layout.
        // Historical discovery never fabricates absent columns or rewrites rows.
        for table in ["agent_runs", "run_events", "run_telemetry"] {
            if !snapshot.tables.iter().any(|name| name == table) {
                return Err(StorageError::invalid_input("agent import requires its declared source tables").into());
            }
        }
        let runs = {
            let mut statement = snapshot.connection.prepare("SELECT id,kind,parent_run_id,parent_thread_id,agent_id,status,prompt_ref,worker_thread_id,checkpoint_path,checkpoint_json,summary,error,metadata_json,started_at,updated_at,completed_at FROM agent_runs ORDER BY id").map_err(source_sql_error)?;
            statement.query_map([], |row| {
                let run = super::super::ops::map_agent_run_row(&snapshot.connection, row)?;
                if row.get::<_, String>(1)? != run.kind.as_str() || row.get::<_, String>(5)? != run.status.as_str() {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                for index in [9,12] {
                    if let Some(raw) = row.get::<_, Option<String>>(index)? {
                        let _: serde_json::Value = serde_json::from_str(&raw).map_err(|error|
                            rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(error)))?;
                    }
                }
                Ok(run)
            }).map_err(source_sql_error)?.collect::<std::result::Result<Vec<_>, _>>().map_err(source_sql_error)?
        };
        let events = {
            let mut statement = snapshot.connection.prepare("SELECT run_id,sequence,event_type,payload_json,timestamp FROM run_events ORDER BY run_id,sequence").map_err(source_sql_error)?;
            statement.query_map([], |row| {
                if row.get::<_, i64>(1)? <= 0 { return Err(rusqlite::Error::InvalidQuery); }
                let raw: String = row.get(3)?;
                let _: serde_json::Value = serde_json::from_str(&raw).map_err(|error|
                    rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error)))?;
                super::super::ops::map_run_event_row(row)
            }).map_err(source_sql_error)?.collect::<std::result::Result<Vec<_>, _>>().map_err(source_sql_error)?
        };
        let telemetry = {
            let mut statement = snapshot.connection.prepare("SELECT run_id,input_tokens,output_tokens,cached_input_tokens,cost_usd,elapsed_ms,tool_count,model,provider,error,updated_at FROM run_telemetry ORDER BY run_id").map_err(source_sql_error)?;
            statement.query_map([], |row| {
                for index in [1,2,3,5,6] {
                    if row.get::<_, Option<i64>>(index)?.is_some_and(|number| number < 0) { return Err(rusqlite::Error::InvalidQuery); }
                }
                super::super::ops::map_run_telemetry_row(row)
            }).map_err(source_sql_error)?.collect::<std::result::Result<Vec<_>, _>>().map_err(source_sql_error)?
        };
        // A content digest identifies a receipt; only SourceOwner establishes tenancy.
        let mut file = std::fs::File::open(&source.path).map_err(|error| StorageError::backend("read source digest").with_source(error))?;
        let mut digest = Sha256::new(); let mut buffer = [0; 65536];
        loop {
            let read = file.read(&mut buffer).map_err(|error| StorageError::backend("read source digest").with_source(error))?;
            if read == 0 { break; } digest.update(&buffer[..read]);
        }
        Ok(LoadedAgentImport { fingerprint: hex::encode(digest.finalize()), runs, events, telemetry })
    }

    async fn commit_agent_import(&self, loaded: LoadedAgentImport, operation_id: &str) -> Result<AgentImportReport> {
        use super::storage::{AGENTS, TELEMETRY, EVENTS, EVENT_HEADS, IMPORTS, encode, key};
        use tinystoragedrivers_core::{Precondition, WriteOp};
        self.declare().await?;
        if let Some(receipt) = self.read::<ImportReceipt>(IMPORTS, operation_id).await? {
            if receipt.doc.fingerprint != loaded.fingerprint { return Err(StorageError::conflict("import operation source changed").into()); }
            return Ok(receipt.doc.report);
        }
        let report = AgentImportReport { agent_runs: loaded.runs.len(), events: loaded.events.len(), telemetry: loaded.telemetry.len() };
        let put = |collection: &str, id: String, doc| WriteOp::Put { collection: collection.into(), id, doc, precondition: Precondition::Absent };
        let mut writes = Vec::new();
        for mut run in loaded.runs { run.telemetry = None; writes.push(put(AGENTS, key(&[&run.id]), encode(&run)?)); }
        for telemetry in loaded.telemetry { writes.push(put(TELEMETRY, key(&[&telemetry.run_id]), encode(&telemetry)?)); }
        let mut heads = std::collections::BTreeMap::<String,u64>::new();
        for event in loaded.events {
            let next = event.sequence.checked_add(1).filter(|sequence| *sequence <= i64::MAX as u64)
                .ok_or_else(|| StorageError::serialization("import event sequence exceeds classic range"))?;
            heads.entry(event.run_id.clone()).and_modify(|current| *current = (*current).max(next)).or_insert(next);
            writes.push(put(EVENTS, key(&[&event.run_id, &event.sequence.to_string()]), encode(&event)?));
        }
        for (run_id, next) in heads { writes.push(put(EVENT_HEADS, key(&[&run_id]), encode(&super::events::EventHead { next })?)); }
        writes.push(put(IMPORTS, key(&[operation_id]), encode(&ImportReceipt { fingerprint: loaded.fingerprint.clone(), report: report.clone() })?));
        match self.scoped.documents().atomic_batch_checked(vec![], writes).await {
            Ok(_) => Ok(report),
            Err(error) if error.kind() == ErrorKind::Conflict => {
                if let Some(receipt) = self.read::<ImportReceipt>(IMPORTS, operation_id).await? {
                    if receipt.doc.fingerprint == loaded.fingerprint { return Ok(receipt.doc.report); }
                }
                Err(error.into())
            }
            Err(error) => Err(error.into()),
        }
    }
}
