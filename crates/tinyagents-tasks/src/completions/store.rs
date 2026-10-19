//! The completion store trait and its in-memory implementation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tinyagents_harness::error::{Result, TinyAgentsError};

use super::types::{CompletionClaim, CompletionRecord, CompletionState, NotifyMode};

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

    /// Atomically leases matching pending completions until `visible_after`.
    /// Implementations shared across processes must use backend CAS/claim
    /// semantics; the default fails closed rather than emulating a local lock.
    fn claim_pending(
        &self,
        parent_key: &str,
        mode: CompletionClaimMode,
        max: usize,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Vec<CompletionRecord>> {
        let _ = (parent_key, mode, max, token, visible_after);
        Err(store_err("claim", "store does not support atomic delivery claims"))
    }

    /// Atomically claims one pending record for a live push. The default fails
    /// closed; a process-local lock is not a substitute for backend ownership.
    fn claim_task(
        &self,
        task_id: &str,
        parent_key: &str,
        mode: CompletionClaimMode,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Option<CompletionRecord>> {
        let _ = (task_id, parent_key, mode, token, visible_after);
        Err(store_err("claim task", "store does not support atomic delivery claims"))
    }

    /// Resolves a claim only while its token still owns the record. `None`
    /// means the claim expired, was reclaimed, or the record was withdrawn.
    fn resolve_claim(
        &self,
        task_id: &str,
        token: &str,
        resolution: ClaimResolution,
    ) -> Result<Option<CompletionRecord>> {
        let _ = (task_id, token, resolution);
        Err(store_err("resolve claim", "store does not support atomic delivery claims"))
    }

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

/// Which pending completion lane a delivery worker may lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionClaimMode {
    /// Follow-up or explicit collect delivery.
    Followup,
    /// Completions held for the parent's next turn.
    HoldForNextTurn,
    /// Pull-only completions.
    Pull,
}

impl CompletionClaimMode {
    pub(crate) fn accepts(self, mode: NotifyMode) -> bool {
        match self {
            Self::Followup => matches!(mode, NotifyMode::Followup | NotifyMode::Collect),
            Self::HoldForNextTurn => mode == NotifyMode::HoldForNextTurn,
            Self::Pull => mode == NotifyMode::Off,
        }
    }
}

/// How a currently owned delivery claim is settled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimResolution {
    /// The parent accepted the completion.
    Delivered,
    /// The delivery failed; the claim is released or the record gives up.
    Failed { max_attempts: u32 },
    /// The delivery was abandoned without counting another attempt.
    Release,
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

    fn claim_pending(
        &self,
        parent_key: &str,
        mode: CompletionClaimMode,
        max: usize,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Vec<CompletionRecord>> {
        let now = SystemTime::now();
        let mut records = self.lock()?;
        let mut candidates: Vec<_> = records
            .values()
            .filter(|record| {
                record.parent_key == parent_key
                    && record.state == CompletionState::Pending
                    && mode.accepts(record.notify_mode)
                    && record
                        .claim
                        .as_ref()
                        .is_none_or(|claim| claim.visible_after <= now)
            })
            .cloned()
            .collect();
        candidates.sort_by(|a, b| {
            a.finished_at
                .cmp(&b.finished_at)
                .then_with(|| a.task_id.cmp(&b.task_id))
        });
        candidates.truncate(max);
        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(record) = records.get_mut(&candidate.task_id) else { continue };
            record.attempts = record.attempts.saturating_add(1);
            record.updated_at = now;
            record.claim = Some(CompletionClaim {
                token: format!("{token}:{}", record.task_id),
                visible_after,
            });
            claimed.push(record.clone());
        }
        Ok(claimed)
    }

    fn claim_task(
        &self,
        task_id: &str,
        parent_key: &str,
        mode: CompletionClaimMode,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Option<CompletionRecord>> {
        let now = SystemTime::now();
        let mut records = self.lock()?;
        let Some(record) = records.get_mut(task_id) else { return Ok(None) };
        if record.parent_key != parent_key
            || record.state != CompletionState::Pending
            || !mode.accepts(record.notify_mode)
            || record.claim.as_ref().is_some_and(|claim| claim.visible_after > now)
        {
            return Ok(None);
        }
        record.attempts = record.attempts.saturating_add(1);
        record.updated_at = now;
        record.claim = Some(CompletionClaim {
            token: format!("{token}:{task_id}"),
            visible_after,
        });
        Ok(Some(record.clone()))
    }

    fn resolve_claim(
        &self,
        task_id: &str,
        token: &str,
        resolution: ClaimResolution,
    ) -> Result<Option<CompletionRecord>> {
        let mut records = self.lock()?;
        let Some(record) = records.get_mut(task_id) else {
            return Ok(None);
        };
        if record.state != CompletionState::Pending
            || record.claim.as_ref().is_none_or(|claim| {
                claim.token != token || claim.visible_after <= SystemTime::now()
            })
        {
            return Ok(None);
        }
        record.updated_at = SystemTime::now();
        record.claim = None;
        match resolution {
            ClaimResolution::Delivered => record.state = CompletionState::Delivered,
            ClaimResolution::Failed { max_attempts } if record.attempts >= max_attempts => {
                record.state = CompletionState::GaveUp;
            }
            ClaimResolution::Failed { .. } | ClaimResolution::Release => {}
        }
        Ok(Some(record.clone()))
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
