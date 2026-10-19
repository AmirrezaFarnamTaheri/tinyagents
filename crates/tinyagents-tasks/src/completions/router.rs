//! The completion router.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tinyagents_harness::error::{Result, TinyAgentsError};
use tinyagents_harness::run_queue::{QueueLane, RunQueueHandle};

use super::format::{CompletionFormatter, NeutralCompletionFormatter};
use super::store::{ClaimResolution, CompletionClaimMode, CompletionStore, ParentGateClaim};
use super::types::{CompletionReceipt, CompletionRecord, CompletionState, NotifyMode};

const LOG_PREFIX: &str = "[completion-router]";
const CLAIM_VISIBILITY: Duration = Duration::from_secs(30);
const PARENT_GATE_TTL: Duration = Duration::from_secs(60);
const PARENT_GATE_WAIT: Duration = Duration::from_secs(65);

/// Delivery attempts before a record gives up, unless overridden.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// What [`CompletionRouter::record`] did with a completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    /// Stored. `lane` is set when it was pushed onto a live parent's queue; the
    /// record is then pending and leased until the host calls
    /// [`CompletionRouter::mark_delivered`].
    Recorded {
        /// The lane the completion was pushed onto, if any.
        lane: Option<QueueLane>,
        /// The receipt authorizing settlement of a live push.
        receipt: Option<CompletionReceipt>,
    },
    /// A record for this task id already exists; nothing changed.
    Duplicate,
    /// Dropped: the task was tombstoned, or its parent was cancelled.
    Suppressed,
    /// The task id is already recorded for a *different* parent. Completion ids
    /// are the dedupe key and must be unique across parents (the host's
    /// `sub-<uuid>` ids are); a host that reuses ids across roots must make them
    /// unique before recording. Nothing was stored.
    IdCollision,
}

/// What [`CompletionRouter::tombstone`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TombstoneOutcome {
    /// A pending completion was withdrawn.
    Suppressed,
    /// Nothing was recorded yet; a completion arriving later will be dropped.
    Reserved,
    /// The record was already delivered, given up, or tombstoned.
    AlreadyFinal,
}

/// Result of an identity-guarded backing-store migration.
#[derive(Clone)]
pub enum CompletionStoreReplaceOutcome {
    /// The replacement was copied and installed on this router.
    Replaced(Arc<dyn CompletionStore>),
    /// The expected store was stale; this is the router's current store.
    Current(Arc<dyn CompletionStore>),
}

struct RouterState {
    /// Store operations and in-process leases share this lock so a migration
    /// cannot detach a claim from the state that owns its acknowledgement.
    store: Arc<dyn CompletionStore>,
    /// Task ids handed out by a claim and not yet resolved. Process-local on
    /// purpose: after a restart nobody holds a lease, so everything still
    /// pending is claimable again.
    leased: HashSet<String>,
    /// Durable claim tokens owned by this router, used to reject stale settles.
    active_receipts: HashMap<String, CompletionReceipt>,
    /// Live parents and their steering queues.
    parents: HashMap<String, RunQueueHandle>,
}

/// Durable, deduplicating hand-off of finished children to their parents.
///
/// See the `completions` module docs for the division of labour with the host.
/// Router locks protect local bookkeeping; shared stores coordinate delivery
/// ownership through atomic claims, so independent routers may safely share a
/// backend that implements those claims.
pub struct CompletionRouter {
    formatter: Arc<dyn CompletionFormatter>,
    max_attempts: u32,
    state: Mutex<RouterState>,
}

struct ParentGateGuard {
    store: Arc<dyn CompletionStore>,
    parent_key: String,
    claim: ParentGateClaim,
}

/// Short-lived durable authority held while a queued completion crosses into
/// its parent turn. Dropping it releases the parent lifecycle gate.
pub struct ParentAdmissionPermit {
    _guard: ParentGateGuard,
}

impl std::fmt::Debug for ParentAdmissionPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ParentAdmissionPermit").finish_non_exhaustive()
    }
}

impl ParentGateGuard {
    fn acquire(store: Arc<dyn CompletionStore>, parent_key: &str) -> Result<Self> {
        let holder = uuid::Uuid::new_v4().to_string();
        let started = std::time::Instant::now();
        loop {
            if let Some(claim) = store.acquire_parent_gate(parent_key, &holder, PARENT_GATE_TTL)? {
                return Ok(Self {
                    store,
                    parent_key: parent_key.to_owned(),
                    claim,
                });
            }
            if started.elapsed() >= PARENT_GATE_WAIT {
                return Err(TinyAgentsError::Graph(format!(
                    "completion router: timed out waiting for parent admission gate `{parent_key}`"
                )));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn renew(&self) -> Result<bool> {
        self.store
            .renew_parent_gate(&self.parent_key, &self.claim, PARENT_GATE_TTL)
    }

    fn claim(&self) -> &ParentGateClaim {
        &self.claim
    }
}

impl Drop for ParentGateGuard {
    fn drop(&mut self) {
        if let Err(error) = self
            .store
            .release_parent_gate(&self.parent_key, &self.claim)
        {
            tracing::warn!(parent_key = %self.parent_key, error = %error, "{LOG_PREFIX} parent gate release failed");
        }
    }
}

impl CompletionRouter {
    /// A router over `store` with the neutral formatter and
    /// [`DEFAULT_MAX_ATTEMPTS`].
    pub fn new(store: Arc<dyn CompletionStore>) -> Self {
        Self {
            formatter: Arc::new(NeutralCompletionFormatter),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            state: Mutex::new(RouterState {
                store,
                leased: HashSet::new(),
                active_receipts: HashMap::new(),
                parents: HashMap::new(),
            }),
        }
    }

    /// Returns the persistence store backing this router.
    pub fn store(&self) -> Arc<dyn CompletionStore> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .store
            .clone()
    }

    /// Atomically migrates this router to `replacement`, preserving its
    /// in-flight leases, parent queues, formatter, and attempt policy. The
    /// current store is enumerated and copied before publication; on any
    /// source-read or target-write error the old store remains canonical. The
    /// replacement must be private to this router while the copy is made.
    pub fn replace_store_if_current(
        &self,
        expected: &Arc<dyn CompletionStore>,
        replacement: Arc<dyn CompletionStore>,
    ) -> Result<CompletionStoreReplaceOutcome> {
        let mut state = self.lock()?;
        if !Arc::ptr_eq(&state.store, expected) {
            return Ok(CompletionStoreReplaceOutcome::Current(Arc::clone(
                &state.store,
            )));
        }
        if Arc::ptr_eq(&state.store, &replacement) {
            return Ok(CompletionStoreReplaceOutcome::Current(Arc::clone(
                &state.store,
            )));
        }
        let records = state.store.try_list(None)?;
        for record in records {
            let parent_key = if record.parent_key.is_empty() {
                Self::parent_from_marker_id(&record.task_id)
                    .unwrap_or_else(|| record.task_id.clone())
            } else {
                record.parent_key.clone()
            };
            let gate = ParentGateGuard::acquire(Arc::clone(&replacement), &parent_key)?;
            replacement.put_fenced(&record, &parent_key, gate.claim())?;
        }
        state.store = Arc::clone(&replacement);
        Ok(CompletionStoreReplaceOutcome::Replaced(replacement))
    }

    /// Replaces the formatter (the host's own wording).
    pub fn with_formatter(mut self, formatter: Arc<dyn CompletionFormatter>) -> Self {
        self.formatter = formatter;
        self
    }

    /// Sets how many failed attempts a record survives. At least one.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// The formatter in use.
    pub fn formatter(&self) -> &Arc<dyn CompletionFormatter> {
        &self.formatter
    }

    /// The attempt limit.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    fn lock(&self) -> Result<MutexGuard<'_, RouterState>> {
        self.state
            .lock()
            .map_err(|_| TinyAgentsError::Graph("completion router lock poisoned".into()))
    }

    /// Marks `parent_key` live: [`NotifyMode::Followup`] and
    /// [`NotifyMode::Collect`] completions for it are pushed onto `queue`.
    pub fn attach_parent(&self, parent_key: impl Into<String>, queue: RunQueueHandle) {
        let parent_key = parent_key.into();
        tracing::debug!(parent_key = %parent_key, "{LOG_PREFIX} parent attached");
        if let Ok(mut state) = self.lock() {
            state.parents.insert(parent_key, queue);
        }
    }

    /// Marks `parent_key` no longer live. Later completions stay pending.
    ///
    /// The parent's queue is presumed gone, so leases on its in-flight pushes
    /// are released and those records can be claimed again. Acknowledge what the
    /// parent already received with [`Self::mark_delivered`] first.
    pub fn detach_parent(&self, parent_key: &str) {
        tracing::debug!(parent_key = %parent_key, "{LOG_PREFIX} parent detached");
        if let Ok(mut state) = self.lock() {
            state.parents.remove(parent_key);
            self.release_parent_locked(&mut state, parent_key);
        }
    }

    fn release_parent_locked(&self, state: &mut RouterState, parent_key: &str) {
        let Ok(gate) = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key) else {
            return;
        };
        for record in state.store.list(Some(parent_key)) {
            state.leased.remove(&record.task_id);
            if let Some(receipt) = state.active_receipts.remove(&record.task_id) {
                let _ = state.store.resolve_claim(
                    &record.task_id,
                    parent_key,
                    receipt.parent_epoch,
                    gate.claim(),
                    &receipt.token,
                    ClaimResolution::Release,
                );
            }
        }
    }

    /// Releases the lease on `task_ids` without counting a failure, so they can
    /// be claimed again. Use it when a claim or push was abandoned (a dropped
    /// delivery future, a cleared queue). Attempts already counted stay counted.
    pub fn release(&self, receipts: &[CompletionReceipt]) {
        if let Ok(mut state) = self.lock() {
            for receipt in receipts {
                if state.active_receipts.get(&receipt.task_id) == Some(receipt) {
                    state.leased.remove(&receipt.task_id);
                    state.active_receipts.remove(&receipt.task_id);
                }
                let Ok(gate) =
                    ParentGateGuard::acquire(Arc::clone(&state.store), &receipt.parent_key)
                else {
                    continue;
                };
                let _ = state.store.resolve_claim(
                    &receipt.task_id,
                    &receipt.parent_key,
                    receipt.parent_epoch,
                    gate.claim(),
                    &receipt.token,
                    ClaimResolution::Release,
                );
            }
        }
    }

    /// Records pushed onto `parent_key`'s live queue (or claimed) and not yet
    /// acknowledged, oldest first.
    pub fn in_flight_for(&self, parent_key: &str) -> Vec<CompletionRecord> {
        let Ok(state) = self.lock() else {
            return Vec::new();
        };
        let mut records: Vec<_> = state
            .store
            .list(Some(parent_key))
            .into_iter()
            .filter(|r| r.state == CompletionState::Pending && state.leased.contains(&r.task_id))
            .collect();
        sort_oldest_first(&mut records);
        records
    }

    fn classify_existing(
        existing: &CompletionRecord,
        incoming: &CompletionRecord,
    ) -> RecordOutcome {
        if existing.state == CompletionState::Tombstoned {
            tracing::debug!(task_id = %incoming.task_id, "{LOG_PREFIX} dropped: tombstoned");
            RecordOutcome::Suppressed
        } else if !existing.parent_key.is_empty() && existing.parent_key != incoming.parent_key {
            tracing::warn!(task_id = %incoming.task_id, "{LOG_PREFIX} task id already recorded for another parent");
            RecordOutcome::IdCollision
        } else {
            tracing::debug!(task_id = %incoming.task_id, state = existing.state.as_str(), "{LOG_PREFIX} duplicate completion ignored");
            RecordOutcome::Duplicate
        }
    }

    /// Records a finished child, idempotent per task id.
    ///
    /// A new record is stored `Pending`. If its [`NotifyMode`] is `Followup` or
    /// `Collect` and the parent is attached, it is also pushed onto that lane,
    /// counted as attempt one and leased. The queue is in memory and has no
    /// acknowledgement, so the record settles only when the host calls
    /// [`Self::mark_delivered`] after the parent has the message; if the queue
    /// is lost first ([`Self::detach_parent`], [`Self::release`], or a restart)
    /// the record is claimable again. Delivery is at-least-once.
    pub async fn record(&self, mut record: CompletionRecord) -> Result<RecordOutcome> {
        let (push, gate) = {
            let mut state = self.lock()?;
            let gate = ParentGateGuard::acquire(Arc::clone(&state.store), &record.parent_key)?;
            let Some(parent_epoch) =
                Self::admit_child_locked(&state, &record.parent_key, gate.claim())?
            else {
                tracing::debug!(
                    task_id = %record.task_id,
                    parent_key = %record.parent_key,
                    "{LOG_PREFIX} dropped: parent is closed"
                );
                return Ok(RecordOutcome::Suppressed);
            };
            if let Some(existing) = state.store.get(&record.task_id) {
                return Ok(Self::classify_existing(&existing, &record));
            }
            record.state = CompletionState::Pending;
            record.parent_epoch = parent_epoch;
            record.attempts = 0;
            record.updated_at = SystemTime::now();
            if !state.store.insert_if_absent(&record, gate.claim())? {
                let existing = state.store.try_get(&record.task_id)?.ok_or_else(|| {
                    TinyAgentsError::Graph(format!(
                        "completion store: conditional create conflicted but `{}` was not readable",
                        record.task_id
                    ))
                })?;
                return Ok(Self::classify_existing(&existing, &record));
            }
            tracing::debug!(
                task_id = %record.task_id,
                parent_key = %record.parent_key,
                status = record.status.as_str(),
                notify_mode = record.notify_mode.as_str(),
                "{LOG_PREFIX} recorded"
            );
            let lane = match record.notify_mode {
                NotifyMode::Followup => Some(QueueLane::Followup),
                NotifyMode::Collect => Some(QueueLane::Collect),
                NotifyMode::HoldForNextTurn | NotifyMode::Off => None,
            };
            // A live push is a delivery attempt, not a delivery: the record
            // stays pending and leased until the host acknowledges it with
            // `mark_delivered`, so a crash, a cleared queue or a detached parent
            // never loses it (it is redelivered through `claim_pending`).
            let push = match lane.and_then(|lane| {
                state
                    .parents
                    .get(&record.parent_key)
                    .map(|queue| (lane, queue.clone()))
            }) {
                Some((lane, queue)) => {
                    let token = uuid::Uuid::new_v4().to_string();
                    let visible_after = SystemTime::now() + CLAIM_VISIBILITY;
                    let Some(claimed) = state.store.claim_task(
                        &record.task_id,
                        &record.parent_key,
                        record.parent_epoch,
                        gate.claim(),
                        CompletionClaimMode::Followup,
                        &token,
                        visible_after,
                    )?
                    else {
                        return Ok(RecordOutcome::Recorded {
                            lane: None,
                            receipt: None,
                        });
                    };
                    record = claimed;
                    let receipt = record.receipt().expect("claimed record has a receipt");
                    state.leased.insert(record.task_id.clone());
                    state
                        .active_receipts
                        .insert(record.task_id.clone(), receipt.clone());
                    Some((lane, queue, receipt))
                }
                None => None,
            };
            (push, gate)
        };
        let Some((lane, queue, push_receipt)) = push else {
            return Ok(RecordOutcome::Recorded {
                lane: None,
                receipt: None,
            });
        };
        if !gate.renew()? {
            self.release(std::slice::from_ref(&push_receipt));
            return Err(TinyAgentsError::Graph(format!(
                "completion router: parent admission gate lost for `{}`",
                record.parent_key
            )));
        }
        let message = self.formatter.to_message(std::slice::from_ref(&record));
        queue.push(lane, message).await;
        tracing::debug!(
            task_id = %record.task_id,
            parent_key = %record.parent_key,
            lane = lane.as_str(),
            "{LOG_PREFIX} pushed to live parent"
        );
        Ok(RecordOutcome::Recorded {
            lane: Some(lane),
            receipt: Some(push_receipt),
        })
    }

    /// [`Self::record`], retried up to `attempts` times with a short backoff
    /// when the store fails. Safe because `record` is idempotent per task id
    /// (a retry after a write that actually landed reports `Duplicate`).
    pub async fn record_with_retries(
        &self,
        record: CompletionRecord,
        attempts: u32,
    ) -> Result<RecordOutcome> {
        let attempts = attempts.max(1);
        let mut attempt = 1;
        loop {
            match self.record(record.clone()).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) if attempt >= attempts => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        task_id = %record.task_id,
                        attempt,
                        error = %error,
                        "{LOG_PREFIX} record failed, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Withdraws a child's completion because the parent collected it itself
    /// (waited on it, or read its result). Suppresses routing from now on; a
    /// message already pushed onto a live queue is not retracted. A completion that has not arrived
    /// yet is dropped when it does.
    pub fn tombstone(&self, task_id: &str) -> Result<TombstoneOutcome> {
        let mut state = self.lock()?;
        let outcome = match state.store.get(task_id) {
            Some(mut existing) if existing.state == CompletionState::Pending => {
                let gate_key = existing.parent_key.clone();
                let gate = ParentGateGuard::acquire(Arc::clone(&state.store), &gate_key)?;
                existing.state = CompletionState::Tombstoned;
                existing.updated_at = SystemTime::now();
                state.store.put_fenced(&existing, &gate_key, gate.claim())?;
                state.leased.remove(task_id);
                TombstoneOutcome::Suppressed
            }
            Some(_) => TombstoneOutcome::AlreadyFinal,
            None => {
                let gate_key = format!("completion-task:{task_id}");
                let gate = ParentGateGuard::acquire(Arc::clone(&state.store), &gate_key)?;
                state.store.put_fenced(
                    &CompletionRecord::tombstone_stub(task_id),
                    &gate_key,
                    gate.claim(),
                )?;
                TombstoneOutcome::Reserved
            }
        };
        tracing::debug!(task_id = %task_id, outcome = ?outcome, "{LOG_PREFIX} tombstone");
        Ok(outcome)
    }

    /// Drops every pending completion for `parent_key` and everything that
    /// finishes for it later, until [`Self::resume_parent`]. For a deleted or
    /// stopped parent. A message already pushed onto the parent's queue is not
    /// retracted; queued generations must be rejected when the parent turn is
    /// admitted. The flag is durable and survives completion compaction and
    /// process restarts. Returns how many pending records were withdrawn.
    pub fn cancel_parent(&self, parent_key: &str) -> Result<usize> {
        self.close_parent(parent_key, CompletionState::Tombstoned)
    }

    /// Permanently closes a deleted parent thread. The durable marker is
    /// retained through completion compaction so late children remain dropped
    /// after a restart.
    pub fn delete_parent(&self, parent_key: &str) -> Result<usize> {
        self.close_parent(parent_key, CompletionState::ParentDeleted)
    }

    fn close_parent(&self, parent_key: &str, marker_state: CompletionState) -> Result<usize> {
        let mut state = self.lock()?;
        let gate = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key)?;
        Self::set_parent_marker(&state, parent_key, marker_state, gate.claim())?;
        state.parents.remove(parent_key);
        let mut withdrawn = 0;
        for mut record in state.store.list(Some(parent_key)) {
            if record.state == CompletionState::Pending {
                record.state = CompletionState::Tombstoned;
                record.updated_at = SystemTime::now();
                state.store.put_fenced(&record, parent_key, gate.claim())?;
                state.leased.remove(&record.task_id);
                withdrawn += 1;
            }
        }
        tracing::debug!(parent_key = %parent_key, withdrawn, "{LOG_PREFIX} parent cancelled");
        Ok(withdrawn)
    }

    /// Reopens a parent stopped with [`Self::cancel_parent`]. Calling this on
    /// an already open parent is idempotent. A parent closed with
    /// [`Self::delete_parent`] cannot be reopened.
    pub fn resume_parent(&self, parent_key: &str) {
        let result = self.lock().and_then(|state| {
            let gate = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key)?;
            let marker_id = Self::parent_marker_id(parent_key);
            match state.store.try_get(&marker_id)? {
                Some(marker) if marker.state == CompletionState::Delivered => Ok(()),
                Some(marker) if marker.state == CompletionState::ParentDeleted => {
                    Err(TinyAgentsError::Graph(format!(
                        "completion router: deleted parent {parent_key:?} cannot be resumed"
                    )))
                }
                _ => Self::set_parent_marker(
                    &state,
                    parent_key,
                    CompletionState::Delivered,
                    gate.claim(),
                ),
            }
        });
        if let Err(error) = result {
            tracing::warn!(parent_key = %parent_key, error = %error, "{LOG_PREFIX} could not resume parent");
        }
    }

    /// The cancelled-parent flag lives in the store as a marker record, so it
    /// survives a restart with the rest of the log. `Tombstoned` means
    /// cancelled; `Delivered` means resumed.
    fn parent_marker_id(parent_key: &str) -> String {
        format!("\u{1}parent-cancelled:{parent_key}")
    }

    fn parent_from_marker_id(task_id: &str) -> Option<String> {
        task_id
            .strip_prefix("\u{1}parent-cancelled:")
            .map(str::to_owned)
    }

    /// Opens a parent on first sight, or reports whether its durable lifecycle
    /// marker currently admits child completions. Closed parents are never
    /// reopened implicitly.
    pub fn admit_child(&self, parent_key: &str) -> Result<bool> {
        let state = self.lock()?;
        let gate = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key)?;
        Ok(Self::admit_child_locked(&state, parent_key, gate.claim())?.is_some())
    }

    /// Whether a generation carried by a queued completion still belongs to
    /// the currently open parent lifecycle. Hosts use this at turn admission
    /// to discard queue entries that raced with stop, delete, or reopen.
    pub fn parent_generation_admitted(&self, parent_key: &str, parent_epoch: u64) -> Result<bool> {
        let state = self.lock()?;
        let marker = state.store.try_get(&Self::parent_marker_id(parent_key))?;
        Ok(marker.is_some_and(|marker| {
            marker.state == CompletionState::Delivered && marker.parent_epoch == parent_epoch
        }))
    }

    /// Acquires the durable parent gate and verifies that `parent_epoch` is
    /// still open. The returned permit orders this queue-to-turn handoff
    /// against cancellation, deletion, and resume on every router sharing the
    /// backing store.
    pub fn acquire_parent_generation(
        &self,
        parent_key: &str,
        parent_epoch: u64,
    ) -> Result<Option<ParentAdmissionPermit>> {
        let state = self.lock()?;
        let gate = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key)?;
        let current = state.store.try_get(&Self::parent_marker_id(parent_key))?;
        let admitted = current.is_some_and(|marker| {
            marker.state == CompletionState::Delivered && marker.parent_epoch == parent_epoch
        });
        Ok(admitted.then_some(ParentAdmissionPermit { _guard: gate }))
    }

    fn admit_child_locked(
        router: &RouterState,
        parent_key: &str,
        gate: &ParentGateClaim,
    ) -> Result<Option<u64>> {
        let marker_id = Self::parent_marker_id(parent_key);
        match router.store.try_get(&marker_id)? {
            Some(marker) if marker.state == CompletionState::Delivered => {
                Ok(Some(marker.parent_epoch))
            }
            Some(_) => Ok(None),
            None => {
                // Records written before lifecycle markers existed carry
                // generation zero. First sight admits those durable records
                // into the new open generation under the same parent gate.
                for mut legacy in router.store.try_list(Some(parent_key))? {
                    if legacy.parent_epoch == 0 {
                        legacy.parent_epoch = 1;
                        router.store.put_fenced(&legacy, parent_key, gate)?;
                    }
                }
                Self::set_parent_marker(router, parent_key, CompletionState::Delivered, gate)?;
                let marker = router.store.try_get(&marker_id)?.ok_or_else(|| {
                    TinyAgentsError::Graph(
                        "completion router: parent marker disappeared after admission".into(),
                    )
                })?;
                Ok(Some(marker.parent_epoch))
            }
        }
    }

    fn set_parent_marker(
        router: &RouterState,
        parent_key: &str,
        state: CompletionState,
        gate: &ParentGateClaim,
    ) -> Result<()> {
        let mut marker = CompletionRecord::tombstone_stub(&Self::parent_marker_id(parent_key));
        marker.parent_key = parent_key.to_owned();
        marker.state = state;
        marker.parent_epoch = router
            .store
            .try_get(&marker.task_id)?
            .map_or(1, |current| current.parent_epoch.saturating_add(1));
        router.store.put_fenced(&marker, parent_key, gate)
    }

    /// Every undelivered completion for `parent_key`, oldest first, in every
    /// [`NotifyMode`] and whether or not a claim currently holds it. Read-only:
    /// this is what surfaces after a restart.
    pub fn pending_for(&self, parent_key: &str) -> Vec<CompletionRecord> {
        let Ok(state) = self.lock() else {
            return Vec::new();
        };
        let mut pending: Vec<_> = state
            .store
            .list(Some(parent_key))
            .into_iter()
            .filter(|r| r.state == CompletionState::Pending)
            .collect();
        sort_oldest_first(&mut pending);
        pending
    }

    /// Claims up to `max` `Followup`/`Collect` completions for the idle-time
    /// batch, oldest first, counting an attempt on each. A claimed record is
    /// leased until [`Self::mark_delivered`] or [`Self::mark_failed`].
    pub fn claim_pending(&self, parent_key: &str, max: usize) -> Result<Vec<CompletionRecord>> {
        self.claim(parent_key, max, CompletionClaimMode::Followup)
    }

    /// Claims the `HoldForNextTurn` completions for the turn that is starting.
    pub fn begin_turn(&self, parent_key: &str) -> Result<Vec<CompletionRecord>> {
        self.claim(parent_key, usize::MAX, CompletionClaimMode::HoldForNextTurn)
    }

    /// Claims up to `max` `Off` completions the parent asked to pull.
    pub fn pull(&self, parent_key: &str, max: usize) -> Result<Vec<CompletionRecord>> {
        self.claim(parent_key, max, CompletionClaimMode::Pull)
    }

    fn claim(
        &self,
        parent_key: &str,
        max: usize,
        mode: CompletionClaimMode,
    ) -> Result<Vec<CompletionRecord>> {
        let mut state = self.lock()?;
        let _gate = ParentGateGuard::acquire(Arc::clone(&state.store), parent_key)?;
        let Some(parent_epoch) = Self::admit_child_locked(&state, parent_key, _gate.claim())?
        else {
            return Ok(Vec::new());
        };
        let token = uuid::Uuid::new_v4().to_string();
        let visible_after = SystemTime::now() + CLAIM_VISIBILITY;
        let mut candidates = state.store.claim_pending(
            parent_key,
            parent_epoch,
            _gate.claim(),
            mode,
            max,
            &token,
            visible_after,
        )?;
        sort_oldest_first(&mut candidates);
        for record in &candidates {
            state.leased.insert(record.task_id.clone());
            if let Some(receipt) = record.receipt() {
                state
                    .active_receipts
                    .insert(record.task_id.clone(), receipt);
            }
        }
        if !candidates.is_empty() {
            tracing::debug!(
                parent_key = %parent_key,
                claimed = candidates.len(),
                "{LOG_PREFIX} claimed"
            );
        }
        Ok(candidates)
    }

    /// Settles claimed (or pulled) records as delivered. Returns how many
    /// changed; records that are not pending are left alone.
    pub fn mark_delivered(&self, receipts: &[CompletionReceipt]) -> Result<usize> {
        let mut state = self.lock()?;
        let mut changed = 0;
        for receipt in receipts {
            let resolved = state.store.resolve_claim(
                &receipt.task_id,
                &receipt.parent_key,
                receipt.parent_epoch,
                ParentGateGuard::acquire(Arc::clone(&state.store), &receipt.parent_key)?.claim(),
                &receipt.token,
                ClaimResolution::Delivered,
            )?;
            if state.active_receipts.get(&receipt.task_id) == Some(receipt) {
                state.active_receipts.remove(&receipt.task_id);
                state.leased.remove(&receipt.task_id);
            }
            if resolved.is_some() {
                changed += 1;
            }
        }
        tracing::debug!(changed, "{LOG_PREFIX} delivered");
        Ok(changed)
    }

    /// Reports a failed delivery. The lease is released so the record can be
    /// claimed again, unless its attempts have reached [`Self::max_attempts`]:
    /// then it becomes [`CompletionState::GaveUp`] and is returned so the host
    /// can apply its own give-up policy (the router never delivers it again).
    pub fn mark_failed(&self, receipts: &[CompletionReceipt]) -> Result<Vec<CompletionRecord>> {
        let mut state = self.lock()?;
        let mut gave_up = Vec::new();
        for receipt in receipts {
            let record = state.store.resolve_claim(
                &receipt.task_id,
                &receipt.parent_key,
                receipt.parent_epoch,
                ParentGateGuard::acquire(Arc::clone(&state.store), &receipt.parent_key)?.claim(),
                &receipt.token,
                ClaimResolution::Failed {
                    max_attempts: self.max_attempts,
                },
            )?;
            if state.active_receipts.get(&receipt.task_id) == Some(receipt) {
                state.active_receipts.remove(&receipt.task_id);
                state.leased.remove(&receipt.task_id);
            }
            if let Some(record) = record.filter(|record| record.state == CompletionState::GaveUp) {
                tracing::warn!(task_id = %record.task_id, parent_key = %record.parent_key, attempts = record.attempts, "{LOG_PREFIX} gave up");
                gave_up.push(record);
            }
        }
        Ok(gave_up)
    }

    /// Drops settled records older than `retain` from the store (and, for the
    /// JSONL store, the log). See [`CompletionStore::compact`].
    pub fn compact(&self, retain: Duration) -> Result<usize> {
        let state = self.lock()?;
        let dropped = state.store.compact(retain)?;
        tracing::debug!(dropped, "{LOG_PREFIX} compacted");
        Ok(dropped)
    }
}

fn sort_oldest_first(records: &mut [CompletionRecord]) {
    records.sort_by(|a, b| {
        a.finished_at
            .cmp(&b.finished_at)
            .then_with(|| a.task_id.cmp(&b.task_id))
    });
}
