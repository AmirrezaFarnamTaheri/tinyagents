//! Storage fixtures backed by the published storage ports and adapters.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use tinyagents_session::port::{AgentStorageFactory, ConditionedAgentStorageFactory};
use tinyagents_storage_kit::{DocumentStore, Scope, ScopedStorage, StreamStore, WriteCondition};
use tinystoragedrivers_api::{Storage, StorageBackend};
use tinystoragedrivers_document::sqlite::{SqliteDocuments, SqliteStreams};
use tinystoragedrivers_testkit::MemoryBackend;

/// A fresh in-memory scope using the real testkit backend.
pub fn memory() -> ScopedStorage {
    let scope = Scope::new("integration-memory").unwrap();
    let backend = MemoryBackend::new();
    ScopedStorage::new(backend.for_scope(&scope).unwrap(), scope)
}

/// The SQLite database `name` in `dir`, opened afresh on every call, so two
/// calls with the same arguments are two handles on one database. The other
/// four ports remain explicit testkit ports because these integration cases
/// exercise durable document/stream state only.
pub fn sqlite(dir: &Path, name: &str) -> ScopedStorage {
    let scope = Scope::new("integration-sqlite").unwrap();
    sqlite_storage(dir, name, &scope, None)
}

fn sqlite_storage(
    dir: &Path,
    name: &str,
    scope: &Scope,
    condition: Option<&WriteCondition>,
) -> ScopedStorage {
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(dir.join(name)).unwrap();
    let documents = match condition {
        Some(condition) => Arc::new(SqliteDocuments::conditioned(
            connection.clone(),
            scope.clone(),
            condition.clone(),
        )) as Arc<dyn DocumentStore>,
        None => Arc::new(SqliteDocuments::new(connection.clone(), scope.clone()))
            as Arc<dyn DocumentStore>,
    };
    let streams = Arc::new(SqliteStreams::new(connection, scope.clone())) as Arc<dyn StreamStore>;
    let memory = MemoryBackend::new().for_scope(&scope).unwrap();
    let storage = Storage::new(
        documents,
        streams,
        memory.kv().clone(),
        memory.blobs().clone(),
        memory.queues().clone(),
        memory.secrets().clone(),
    );
    ScopedStorage::new(storage, scope.clone())
}

/// The document port of `storage`.
pub fn docs(storage: &ScopedStorage) -> Arc<dyn DocumentStore> {
    Arc::clone(storage.documents())
}

/// A test-host binding to one already-open storage scope.
pub fn fixed_factory(storage: ScopedStorage) -> AgentStorageFactory {
    Arc::new(move |_| Ok(storage.clone()))
}

/// Host factories over one SQLite scope, including the native conditioned
/// opener required for durable transcript-generation writes.
pub fn conditioned_sqlite_factories(
    dir: &Path,
    name: &str,
) -> (AgentStorageFactory, ConditionedAgentStorageFactory) {
    let directory = dir.to_path_buf();
    let name = name.to_owned();
    let scope = Scope::new("integration-conditioned-sqlite").unwrap();
    let plain = {
        let directory = directory.clone();
        let name = name.clone();
        let scope = scope.clone();
        Arc::new(move |_: &str| Ok(sqlite_storage(&directory, &name, &scope, None)))
            as AgentStorageFactory
    };
    let conditioned = Arc::new(move |_: &str, condition: &WriteCondition| {
        Ok(sqlite_storage(&directory, &name, &scope, Some(condition)))
    }) as ConditionedAgentStorageFactory;
    (plain, conditioned)
}
