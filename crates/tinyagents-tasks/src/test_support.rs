//! Shared testkit storage backends for driver reopen tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tinystoragedrivers_testkit::MemoryBackend;

/// Returns the same in-memory test backend for the same test path key.
pub(crate) fn memory_backend(path: impl AsRef<Path>) -> Arc<MemoryBackend> {
    static BACKENDS: OnceLock<Mutex<HashMap<PathBuf, Arc<MemoryBackend>>>> = OnceLock::new();
    let mut backends = BACKENDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("test backend registry lock poisoned");
    Arc::clone(
        backends
            .entry(path.as_ref().to_owned())
            .or_insert_with(|| Arc::new(MemoryBackend::new())),
    )
}
