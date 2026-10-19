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
/// A store holds the latest [`CompletionRecord`] per task id. `put` persists
/// unconditional record changes; delivery ownership uses `claim_pending`,
/// `claim_task`, and `resolve_claim`, which implementations shared by routers
/// must perform atomically. Synchronous on purpose, like
/// [`TaskStore`](crate::TaskStore).
pub trait CompletionStore: Send + Sync {
    /// Acquires the cross-router parent admission gate. Shared durable stores
    /// must implement this with a backend lease; the default fails closed.
    fn acquire_parent_gate(
        &self,
        parent_key: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<ParentGateClaim>> {
        let _ = (parent_key, holder, ttl);
        Err(store_err(
            "parent gate",
            "store does not support durable leases",
        ))
    }

    /// Renews the exact holder and epoch returned by `acquire_parent_gate`.
    fn renew_parent_gate(
        &self,
        parent_key: &str,
        claim: &ParentGateClaim,
        ttl: Duration,
    ) -> Result<bool> {
        let _ = (parent_key, claim, ttl);
        Err(store_err(
            "parent gate",
            "store does not support durable leases",
        ))
    }

    /// Releases only the holder and epoch returned by `acquire_parent_gate`.
    fn release_parent_gate(&self, parent_key: &str, claim: &ParentGateClaim) -> Result<bool> {
        let _ = (parent_key, claim);
        Err(store_err(
            "parent gate",
            "store does not support durable leases",
        ))
    }

    /// The latest record for `task_id`.
    fn get(&self, task_id: &str) -> Option<CompletionRecord>;

    /// Fallible read used to classify a conflicting conditional create.
    fn try_get(&self, task_id: &str) -> Result<Option<CompletionRecord>> {
        Ok(self.get(task_id))
    }

    /// Upserts a record. Durable stores persist it before returning.
    fn put(&self, record: &CompletionRecord) -> Result<()>;

    /// Upserts a record only through the supplied current parent tenure.
    /// Shared durable stores must fence the write atomically; the default
    /// fails closed rather than accepting an unguarded write.
    fn put_fenced(
        &self,
        record: &CompletionRecord,
        parent_key: &str,
        gate: &ParentGateClaim,
    ) -> Result<()> {
        let _ = (record, parent_key, gate);
        Err(store_err(
            "fenced write",
            "store does not support epoch-fenced writes",
        ))
    }

    /// Creates a record only if its key is absent. Shared stores must make the
    /// absence check and write atomic; unsupported stores fail closed.
    fn insert_if_absent(&self, record: &CompletionRecord, gate: &ParentGateClaim) -> Result<bool> {
        let _ = (record, gate);
        Err(store_err(
            "conditional create",
            "store does not support atomic create",
        ))
    }

    /// Atomically leases matching pending completions until `visible_after`.
    /// Implementations shared across processes must use backend CAS/claim
    /// semantics; the default fails closed rather than emulating a local lock.
    fn claim_pending(
        &self,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        mode: CompletionClaimMode,
        max: usize,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Vec<CompletionRecord>> {
        let _ = (
            parent_key,
            parent_epoch,
            gate,
            mode,
            max,
            token,
            visible_after,
        );
        Err(store_err(
            "claim",
            "store does not support atomic delivery claims",
        ))
    }

    /// Atomically claims one pending record for a live push. The default fails
    /// closed; a process-local lock is not a substitute for backend ownership.
    fn claim_task(
        &self,
        task_id: &str,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        mode: CompletionClaimMode,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Option<CompletionRecord>> {
        let _ = (
            task_id,
            parent_key,
            parent_epoch,
            gate,
            mode,
            token,
            visible_after,
        );
        Err(store_err(
            "claim task",
            "store does not support atomic delivery claims",
        ))
    }

    /// Resolves a claim only while its token still owns the record. `None`
    /// means the claim expired, was reclaimed, or the record was withdrawn.
    fn resolve_claim(
        &self,
        task_id: &str,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        token: &str,
        resolution: ClaimResolution,
    ) -> Result<Option<CompletionRecord>> {
        let _ = (task_id, parent_key, parent_epoch, gate, token, resolution);
        Err(store_err(
            "resolve claim",
            "store does not support atomic delivery claims",
        ))
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

/// Fencing identity for one held parent admission lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentGateClaim {
    /// Unique holder identity for this lease tenure.
    pub holder: String,
    /// Backend epoch that fences any earlier tenure.
    pub epoch: u64,
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
    parent_gates: Arc<Mutex<HashMap<String, MemoryParentGate>>>,
    parent_gate_epochs: Arc<Mutex<HashMap<String, u64>>>,
}

#[derive(Clone, Debug)]
struct MemoryParentGate {
    claim: ParentGateClaim,
    expires_at: SystemTime,
}

impl InMemoryCompletionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, CompletionRecord>>> {
        self.inner.lock().map_err(|_| store_err("lock", "poisoned"))
    }

    fn with_current_gate<T>(
        &self,
        parent_key: &str,
        claim: &ParentGateClaim,
        operation: impl FnOnce(&mut HashMap<String, CompletionRecord>) -> Result<T>,
    ) -> Result<T> {
        let gates = self
            .parent_gates
            .lock()
            .map_err(|_| store_err("parent gate", "lock poisoned"))?;
        let now = SystemTime::now();
        if !gates
            .get(parent_key)
            .is_some_and(|gate| gate.claim == *claim && gate.expires_at > now)
        {
            return Err(store_err("parent gate", "claim is stale or expired"));
        }
        let mut records = self.lock()?;
        if !gates
            .get(parent_key)
            .is_some_and(|gate| gate.claim == *claim && gate.expires_at > SystemTime::now())
        {
            return Err(store_err("parent gate", "claim expired before mutation"));
        }
        operation(&mut records)
    }
}

impl CompletionStore for InMemoryCompletionStore {
    fn acquire_parent_gate(
        &self,
        parent_key: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<ParentGateClaim>> {
        let now = SystemTime::now();
        let expires_at = now
            .checked_add(ttl)
            .ok_or_else(|| store_err("parent gate", "ttl out of range"))?;
        let mut gates = self
            .parent_gates
            .lock()
            .map_err(|_| store_err("parent gate", "lock poisoned"))?;
        if let Some(existing) = gates.get(parent_key) {
            if existing.expires_at > now {
                return if existing.claim.holder == holder {
                    Ok(Some(existing.claim.clone()))
                } else {
                    Ok(None)
                };
            }
        }
        let mut epochs = self
            .parent_gate_epochs
            .lock()
            .map_err(|_| store_err("parent gate epoch", "lock poisoned"))?;
        let epoch = epochs
            .get(parent_key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| store_err("parent gate epoch", "epoch overflow"))?;
        epochs.insert(parent_key.to_owned(), epoch);
        let claim = ParentGateClaim {
            holder: holder.to_owned(),
            epoch,
        };
        gates.insert(
            parent_key.to_owned(),
            MemoryParentGate {
                claim: claim.clone(),
                expires_at,
            },
        );
        Ok(Some(claim))
    }

    fn renew_parent_gate(
        &self,
        parent_key: &str,
        claim: &ParentGateClaim,
        ttl: Duration,
    ) -> Result<bool> {
        let now = SystemTime::now();
        let expires_at = now
            .checked_add(ttl)
            .ok_or_else(|| store_err("parent gate", "ttl out of range"))?;
        let mut gates = self
            .parent_gates
            .lock()
            .map_err(|_| store_err("parent gate", "lock poisoned"))?;
        let Some(gate) = gates.get_mut(parent_key) else {
            return Ok(false);
        };
        if gate.claim != *claim || gate.expires_at <= now {
            return Ok(false);
        }
        gate.expires_at = expires_at;
        Ok(true)
    }

    fn release_parent_gate(&self, parent_key: &str, claim: &ParentGateClaim) -> Result<bool> {
        let mut gates = self
            .parent_gates
            .lock()
            .map_err(|_| store_err("parent gate", "lock poisoned"))?;
        if gates
            .get(parent_key)
            .is_some_and(|gate| gate.claim == *claim && gate.expires_at > SystemTime::now())
        {
            gates.remove(parent_key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn get(&self, task_id: &str) -> Option<CompletionRecord> {
        self.inner.lock().ok()?.get(task_id).cloned()
    }

    fn put(&self, record: &CompletionRecord) -> Result<()> {
        let mut records = self.lock()?;
        let mut record = record.clone();
        if record.parent_epoch == 0 && !record.parent_key.is_empty() {
            let marker_id = format!("\u{1}parent-cancelled:{}", record.parent_key);
            if let Some(marker) = records
                .get(&marker_id)
                .filter(|m| m.state == CompletionState::Delivered)
            {
                record.parent_epoch = marker.parent_epoch;
            }
        }
        records.insert(record.task_id.clone(), record);
        Ok(())
    }

    fn put_fenced(
        &self,
        record: &CompletionRecord,
        parent_key: &str,
        gate: &ParentGateClaim,
    ) -> Result<()> {
        self.with_current_gate(parent_key, gate, |records| {
            let mut record = record.clone();
            if record.parent_epoch == 0 && !record.parent_key.is_empty() {
                let marker_id = format!("\u{1}parent-cancelled:{}", record.parent_key);
                if let Some(marker) = records
                    .get(&marker_id)
                    .filter(|m| m.state == CompletionState::Delivered)
                {
                    record.parent_epoch = marker.parent_epoch;
                }
            }
            records.insert(record.task_id.clone(), record);
            Ok(())
        })
    }

    fn insert_if_absent(&self, record: &CompletionRecord, gate: &ParentGateClaim) -> Result<bool> {
        self.with_current_gate(&record.parent_key, gate, |records| {
            if records.contains_key(&record.task_id) {
                return Ok(false);
            }
            records.insert(record.task_id.clone(), record.clone());
            Ok(true)
        })
    }

    fn claim_pending(
        &self,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        mode: CompletionClaimMode,
        max: usize,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Vec<CompletionRecord>> {
        self.with_current_gate(parent_key, gate, |records| {
            let now = SystemTime::now();
            let mut candidates: Vec<_> = records
                .values()
                .filter(|record| {
                    record.parent_key == parent_key
                        && record.parent_epoch == parent_epoch
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
                let Some(record) = records.get_mut(&candidate.task_id) else {
                    continue;
                };
                record.attempts = record.attempts.saturating_add(1);
                record.updated_at = now;
                record.claim = Some(CompletionClaim {
                    token: format!("{token}:{}", record.task_id),
                    visible_after,
                });
                claimed.push(record.clone());
            }
            Ok(claimed)
        })
    }

    fn claim_task(
        &self,
        task_id: &str,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        mode: CompletionClaimMode,
        token: &str,
        visible_after: SystemTime,
    ) -> Result<Option<CompletionRecord>> {
        self.with_current_gate(parent_key, gate, |records| {
            let now = SystemTime::now();
            let Some(record) = records.get_mut(task_id) else {
                return Ok(None);
            };
            if record.parent_key != parent_key
                || record.parent_epoch != parent_epoch
                || record.state != CompletionState::Pending
                || !mode.accepts(record.notify_mode)
                || record
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.visible_after > now)
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
        })
    }

    fn resolve_claim(
        &self,
        task_id: &str,
        parent_key: &str,
        parent_epoch: u64,
        gate: &ParentGateClaim,
        token: &str,
        resolution: ClaimResolution,
    ) -> Result<Option<CompletionRecord>> {
        self.with_current_gate(parent_key, gate, |records| {
            let Some(record) = records.get_mut(task_id) else {
                return Ok(None);
            };
            if record.parent_key != parent_key
                || record.parent_epoch != parent_epoch
                || record.state != CompletionState::Pending
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
        })
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
        guard.retain(|task_id, r| {
            task_id.starts_with("\u{1}parent-cancelled:") || !expired(r, now, retain)
        });
        Ok(before - guard.len())
    }
}
