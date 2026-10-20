//! Shared testkit fixtures for storage-port conformance and reopen tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tinyagents_storage_kit::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend;

use crate::checkpoint::DriverCheckpointer;

/// A testkit backend shared by opens using the same fixture path.
pub(crate) fn memory_backend(path: &Path) -> Arc<MemoryBackend> {
    static BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<MemoryBackend>>>> = OnceLock::new();
    let mut backends = BACKENDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("test backend registry lock poisoned");
    let backend = Arc::clone(
        backends
            .entry(path.to_owned())
            .or_insert_with(|| Arc::new(MemoryBackend::new())),
    );
    backend
}

/// A checkpointer over a shared testkit backend keyed by `path`.
pub(crate) fn memory_checkpointer<State>(path: &Path) -> DriverCheckpointer<State> {
    let scoped = memory_backend(path)
        .for_scope(&Scope::new("local").unwrap())
        .expect("local scope on a single-operator database");
    DriverCheckpointer::new(Arc::clone(scoped.documents()))
}

/// [`memory_checkpointer`] on a fixed key inside `dir`.
pub(crate) fn dir_checkpointer<State>(dir: &Path) -> DriverCheckpointer<State> {
    memory_checkpointer(&dir.join("checkpoints.sqlite"))
}
