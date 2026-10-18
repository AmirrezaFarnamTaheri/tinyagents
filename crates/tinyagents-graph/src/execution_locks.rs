//! Private per-thread lock state for the graph's executor and delegation runner.
//!
//! Reusable lock maps live in `tinyagents-harness-tools`; these two graph
//! entry points retain only the private serialization state they need until
//! the graph runtime is retired.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};

use tokio::sync::Mutex;

#[derive(Default)]
pub(super) struct RunLockMap {
    locks: StdMutex<HashMap<String, Weak<Mutex<()>>>>,
}

impl RunLockMap {
    pub(super) fn lock_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(|_| panic!("graph run lock map poisoned"));
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.to_owned(), Arc::downgrade(&lock));
        if locks.len() >= 16 {
            locks.retain(|_, weak| weak.strong_count() > 0);
        }
        lock
    }
}
