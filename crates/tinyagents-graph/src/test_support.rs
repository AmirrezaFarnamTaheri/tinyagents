//! Shared fixtures for in-crate tests that need a checkpointer which survives
//! being dropped and reopened (a simulated process restart).

use std::path::Path;
use std::sync::Arc;

use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_sqlite::SqliteStorage;

use crate::checkpoint::DriverCheckpointer;

/// A [`DriverCheckpointer`] over the SQLite database file at `path`.
///
/// Every call opens the file afresh, so two calls with the same path behave
/// like two executors (or one executor before and after a restart) sharing a
/// database.
pub(crate) fn sqlite_checkpointer<State>(path: &Path) -> DriverCheckpointer<State> {
    let storage = SqliteStorage::open(path).expect("open sqlite checkpoint database");
    let scoped = storage
        .for_scope(&Scope::local())
        .expect("local scope on a single-operator database");
    DriverCheckpointer::new(Arc::clone(scoped.documents()))
}

/// [`sqlite_checkpointer`] on a fixed file inside `dir`, so callers can pass
/// the same temp directory they used to hand a directory-based checkpointer.
pub(crate) fn dir_checkpointer<State>(dir: &Path) -> DriverCheckpointer<State> {
    sqlite_checkpointer(&dir.join("checkpoints.sqlite"))
}
