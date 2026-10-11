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
