//! Storage fixtures: the bundled drivers the persistence tests run against.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use tinyagents_graph::checkpoint::DriverCheckpointer;
use tinystoragedrivers_core::{
    DocumentStore, MemoryStorage, Scope, ScopedStorage, StorageBackend,
};
use tinystoragedrivers_sqlite::SqliteStorage;

/// A fresh in-memory scope.
pub fn memory() -> ScopedStorage {
    MemoryStorage::new().for_scope(&Scope::local()).unwrap()
}

/// The SQLite database file `name` in `dir`, opened afresh on every call, so
/// two calls with the same arguments are two handles on one database (and a
/// dropped one reopened models a restart).
pub fn sqlite(dir: &Path, name: &str) -> ScopedStorage {
    SqliteStorage::open(dir.join(name))
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap()
}

/// The document port of `storage`.
pub fn docs(storage: &ScopedStorage) -> Arc<dyn DocumentStore> {
    Arc::clone(storage.documents())
}

/// A durable checkpointer over `checkpoints.sqlite` in `dir`.
pub fn checkpointer<State>(dir: &Path) -> DriverCheckpointer<State> {
    DriverCheckpointer::new(docs(&sqlite(dir, "checkpoints.sqlite")))
}
