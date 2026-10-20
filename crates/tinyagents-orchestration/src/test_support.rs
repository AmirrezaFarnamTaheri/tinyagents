//! Shared testkit fixtures for the orchestration drivers.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tinyagents_session::run_ledger::DriverRunLedger;
use tinyagents_storage_kit::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend;

fn backends() -> &'static Mutex<HashMap<PathBuf, Arc<MemoryBackend>>> {
    static BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<MemoryBackend>>>> = OnceLock::new();
    BACKENDS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns a testkit backend shared by calls using the same path key.
pub(crate) fn memory_backend_at(path: impl AsRef<Path>) -> Arc<MemoryBackend> {
    let mut backends = backends()
        .lock()
        .expect("test backend registry lock poisoned");
    let backend = Arc::clone(
        backends
            .entry(path.as_ref().to_owned())
            .or_insert_with(|| Arc::new(MemoryBackend::new())),
    );
    backend
}

/// Opens a run ledger on a testkit backend shared by calls using the same dir.
pub(crate) fn ledger_in(dir: &Path) -> DriverRunLedger {
    let backend = memory_backend_at(dir.join("ledger"));
    let scoped = backend
        .for_scope(&Scope::new("local").unwrap())
        .expect("local scope");
    DriverRunLedger::new(scoped)
}
