//! Fixtures for in-crate tests that need a durable run ledger.

use std::path::Path;

use tinyagents_session::run_ledger::DriverRunLedger;
use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_sqlite::SqliteStorage;

/// A run ledger over the SQLite file `ledger.sqlite` in `dir`.
///
/// Every call opens the file afresh, so two calls with the same directory are
/// two independent handles (two executors) on one database, and a ledger
/// dropped and reopened models a process restart.
pub(crate) fn ledger_in(dir: &Path) -> DriverRunLedger {
    let storage = SqliteStorage::open(dir.join("ledger.sqlite")).expect("open ledger database");
    DriverRunLedger::new(storage.for_scope(&Scope::local()).expect("local scope"))
}
