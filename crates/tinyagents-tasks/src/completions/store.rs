//! The completion store trait and its in-memory implementation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tinyagents_harness::error::{Result, TinyAgentsError};

use super::types::{CompletionRecord, CompletionState};

fn store_err(what: &str, err: impl std::fmt::Display) -> TinyAgentsError {
    TinyAgentsError::Graph(format!("completion store: {what}: {err}"))
}

/// Durable bookkeeping behind [`CompletionRouter`](super::CompletionRouter).
///
/// A store holds the latest [`CompletionRecord`] per task id. It is a plain
/// persistence seam: the router serialises its read-modify-write sequences, so
/// an implementation only has to make `put` durable before it returns.
/// Synchronous on purpose, like [`TaskStore`](crate::TaskStore).
pub trait CompletionStore: Send + Sync {
    /// The latest record for `task_id`.
    fn get(&self, task_id: &str) -> Option<CompletionRecord>;

    /// Upserts a record. Durable stores persist it before returning.
    fn put(&self, record: &CompletionRecord) -> Result<()>;

    /// Latest records, for one parent or (`None`) for all. Unordered.
    fn list(&self, parent_key: Option<&str>) -> Vec<CompletionRecord>;

    /// Fallible form used when copying state during a backing-store migration.
    /// Legacy implementations remain source-compatible; stores that can fail
    /// while enumerating should override this and return the actual error.
    fn try_list(&self, parent_key: Option<&str>) -> Result<Vec<CompletionRecord>> {
        Ok(self.list(parent_key))
    }

    /// Drops settled records (anything but `Pending`) last updated more than
    /// `retain` ago, returning how many went. Dropping a settled record also
    /// drops its dedupe, so pick a window longer than any child can outlive its
    /// parent's interest. The default keeps everything.
    fn compact(&self, retain: Duration) -> Result<usize> {
        let _ = retain;
        Ok(0)
    }
}

fn expired(record: &CompletionRecord, now: SystemTime, retain: Duration) -> bool {
    record.state != CompletionState::Pending
        && now
            .duration_since(record.updated_at)
            .is_ok_and(|age| age > retain)
}

/// Thread-safe in-memory [`CompletionStore`].
#[derive(Clone, Debug, Default)]
pub struct InMemoryCompletionStore {
    inner: Arc<Mutex<HashMap<String, CompletionRecord>>>,
}

impl InMemoryCompletionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, CompletionRecord>>> {
        self.inner.lock().map_err(|_| store_err("lock", "poisoned"))
    }
}

impl CompletionStore for InMemoryCompletionStore {
    fn get(&self, task_id: &str) -> Option<CompletionRecord> {
        self.inner.lock().ok()?.get(task_id).cloned()
    }

    fn put(&self, record: &CompletionRecord) -> Result<()> {
        self.lock()?.insert(record.task_id.clone(), record.clone());
        Ok(())
    }

    fn list(&self, parent_key: Option<&str>) -> Vec<CompletionRecord> {
        let Ok(guard) = self.inner.lock() else {
            return Vec::new();
        };
        guard
            .values()
            .filter(|r| parent_key.is_none_or(|p| r.parent_key == p))
            .cloned()
            .collect()
    }

    fn compact(&self, retain: Duration) -> Result<usize> {
        let now = SystemTime::now();
        let mut guard = self.lock()?;
        let before = guard.len();
        guard.retain(|_, r| !expired(r, now, retain));
        Ok(before - guard.len())
    }
}
