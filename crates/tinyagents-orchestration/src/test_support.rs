//! Shared testkit fixtures for the orchestration drivers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tinyagents_session::run_ledger::DriverRunLedger;
use tinyagents_storage_kit::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend;

/// Opens a run ledger on a testkit backend shared by calls using the same dir.
pub(crate) fn ledger_in(dir: &Path) -> DriverRunLedger {
    static BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<MemoryBackend>>>> = OnceLock::new();
    let mut backends = BACKENDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("test backend registry lock poisoned");
    let backend = Arc::clone(
        backends
            .entry(dir.join("ledger"))
            .or_insert_with(|| Arc::new(MemoryBackend::new())),
    );
    let scoped = backend
        .for_scope(&Scope::new("local").unwrap())
        .expect("local scope");
    DriverRunLedger::new(scoped)
}
