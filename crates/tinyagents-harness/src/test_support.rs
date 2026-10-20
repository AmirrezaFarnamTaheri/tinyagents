//! Shared test fixtures for storage ports.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tinystoragedrivers_testkit::MemoryBackend;

/// Returns a testkit backend shared by every open using the same fixture path.
///
/// The path is only a test identity: storage remains in memory. Keeping the
/// backend alive across opens lets driver tests model dropping and reopening a
/// handle without depending on a storage engine in consumer dev-dependencies.
pub(crate) fn shared_memory_backend(path: impl AsRef<Path>) -> Arc<MemoryBackend> {
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
