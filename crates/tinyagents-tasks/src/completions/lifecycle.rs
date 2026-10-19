//! Process-local parent lifecycle authority shared across router cache entries.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use tinyagents_harness::error::{Result, TinyAgentsError};

const LOCAL_GATE_WAIT: Duration = Duration::from_secs(65);

/// A lifecycle snapshot retained for one parent in one trusted pool scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ParentLifecycleStatus {
    Open(u64),
    Stopped(u64),
    Deleted(u64),
}

#[derive(Default)]
struct ParentLifecycleEntry {
    held: bool,
    status: Option<ParentLifecycleStatus>,
}

/// Scope-owned state that outlives cached routers and their backing stores.
pub(super) struct ParentLifecycle {
    identity: String,
    entries: Mutex<HashMap<String, ParentLifecycleEntry>>,
    changed: Condvar,
}

impl ParentLifecycle {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            identity: uuid::Uuid::new_v4().to_string(),
            entries: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
        })
    }

    pub(super) fn identity(&self) -> &str {
        &self.identity
    }

    pub(super) fn acquire(self: &Arc<Self>, parent_key: &str) -> Result<ParentLifecycleGuard> {
        let started = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| TinyAgentsError::Graph("parent lifecycle lock poisoned".into()))?;
        loop {
            let entry = entries.entry(parent_key.to_owned()).or_default();
            if !entry.held {
                entry.held = true;
                return Ok(ParentLifecycleGuard {
                    lifecycle: Arc::clone(self),
                    parent_key: parent_key.to_owned(),
                });
            }
            let remaining = LOCAL_GATE_WAIT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(TinyAgentsError::Graph(format!(
                    "completion router: timed out waiting for local parent admission gate `{parent_key}`"
                )));
            }
            let (next, timeout) = self
                .changed
                .wait_timeout(entries, remaining)
                .map_err(|_| TinyAgentsError::Graph("parent lifecycle lock poisoned".into()))?;
            entries = next;
            if timeout.timed_out()
                && entries
                    .get(parent_key)
                    .is_some_and(|entry| entry.held)
            {
                return Err(TinyAgentsError::Graph(format!(
                    "completion router: timed out waiting for local parent admission gate `{parent_key}`"
                )));
            }
        }
    }

    pub(super) fn status(&self, parent_key: &str) -> Result<Option<ParentLifecycleStatus>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| TinyAgentsError::Graph("parent lifecycle lock poisoned".into()))?;
        Ok(entries.get(parent_key).and_then(|entry| entry.status))
    }

    pub(super) fn set_status(
        &self,
        guard: &ParentLifecycleGuard,
        status: ParentLifecycleStatus,
    ) -> Result<()> {
        if !std::ptr::eq(self, Arc::as_ptr(&guard.lifecycle)) {
            return Err(TinyAgentsError::Graph(
                "completion router: parent lifecycle guard scope mismatch".into(),
            ));
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| TinyAgentsError::Graph("parent lifecycle lock poisoned".into()))?;
        let entry = entries.entry(guard.parent_key.clone()).or_default();
        if !entry.held {
            return Err(TinyAgentsError::Graph(
                "completion router: parent lifecycle guard is no longer held".into(),
            ));
        }
        entry.status = Some(status);
        Ok(())
    }
}

pub(super) struct ParentLifecycleGuard {
    lifecycle: Arc<ParentLifecycle>,
    parent_key: String,
}

impl Drop for ParentLifecycleGuard {
    fn drop(&mut self) {
        let Ok(mut entries) = self.lifecycle.entries.lock() else {
            return;
        };
        if let Some(entry) = entries.get_mut(&self.parent_key) {
            entry.held = false;
        }
        self.lifecycle.changed.notify_all();
    }
}
