//! Host-driven delivery timing around durable completion claims.

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tinyagents_harness::error::Result;

use super::{CompletionRecord, CompletionRouter};

/// Timing and batch bounds for one delivery drain.
#[derive(Clone, Copy, Debug)]
pub struct CompletionDeliveryConfig {
    /// Wait for a burst of completions to settle before claiming a batch.
    pub debounce: Duration,
    /// Delay between failed delivery turns.
    pub retry_backoff: Duration,
    /// Poll interval while the parent is busy.
    pub busy_poll: Duration,
    /// Maximum busy polls before leaving records pending.
    pub max_busy_polls: u32,
    /// Maximum completions in one delivery turn.
    pub batch_size: usize,
}

impl Default for CompletionDeliveryConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_secs(3),
            retry_backoff: Duration::from_secs(2),
            busy_poll: Duration::from_secs(5),
            max_busy_polls: 360,
            batch_size: 32,
        }
    }
}

/// Why a drain ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// There were no eligible completions.
    Empty,
    /// The delivery turn was acknowledged.
    Delivered { count: usize },
    /// Delivery failed and the records remain retryable or gave up.
    Failed { count: usize, gave_up: usize },
    /// The parent remained busy beyond the configured polling window.
    Busy,
    /// The host's ownership fence stopped this drain.
    Fenced,
}

/// Timing policy owned by the host's background worker; delivery state lives
/// in [`CompletionRouter`]. Callbacks are seams for host policy and execution.
pub struct CompletionDelivery {
    config: CompletionDeliveryConfig,
}

impl CompletionDelivery {
    /// Creates a delivery drain with explicit timing bounds.
    pub fn new(config: CompletionDeliveryConfig) -> Self {
        Self { config }
    }

    /// Delivers one batch, retrying failed turns with backoff up to the
    /// router's attempt limit. The host supplies busy/fence checks, the actual
    /// parent turn, and a durable writer for records which give up.
    pub async fn deliver_once<B, F, D, Fut, E, U>(
        &self,
        router: &CompletionRouter,
        parent_key: &str,
        mut is_busy: B,
        mut admitted: F,
        mut deliver: D,
        mut write_undelivered: U,
    ) -> DeliveryOutcome
    where
        B: FnMut() -> bool,
        F: FnMut() -> bool,
        D: FnMut(&[CompletionRecord]) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        self.drain(
            router,
            parent_key,
            &mut is_busy,
            &mut admitted,
            &mut deliver,
            &mut write_undelivered,
        )
        .await
    }

    /// Delivers records recovered from durable storage after process startup.
    /// It uses the same claim/retry path as a live completion and is fenced
    /// before any wait or claim.
    pub async fn recover_on_boot<B, F, D, Fut, E, U>(
        &self,
        router: &CompletionRouter,
        parent_key: &str,
        is_busy: B,
        admitted: F,
        deliver: D,
        write_undelivered: U,
    ) -> DeliveryOutcome
    where
        B: FnMut() -> bool,
        F: FnMut() -> bool,
        D: FnMut(&[CompletionRecord]) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        self.deliver_once(router, parent_key, is_busy, admitted, deliver, write_undelivered)
            .await
    }

    async fn drain<B, F, D, Fut, E, U>(
        &self,
        router: &CompletionRouter,
        parent_key: &str,
        is_busy: &mut B,
        admitted: &mut F,
        deliver: &mut D,
        write_undelivered: &mut U,
    ) -> DeliveryOutcome
    where
        B: FnMut() -> bool,
        F: FnMut() -> bool,
        D: FnMut(&[CompletionRecord]) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
    {
        if !sleep_if_admitted(self.config.debounce, admitted).await {
            return DeliveryOutcome::Fenced;
        }
        for _ in 0..self.config.max_busy_polls {
            if !admitted() {
                return DeliveryOutcome::Fenced;
            }
            if !is_busy() {
                break;
            }
            if !sleep_if_admitted(self.config.busy_poll, admitted).await {
                return DeliveryOutcome::Fenced;
            }
        }
        if !admitted() {
            return DeliveryOutcome::Fenced;
        }
        if is_busy() {
            return DeliveryOutcome::Busy;
        }

        let mut batch = match router.claim_pending(parent_key, self.config.batch_size) {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(parent_key, error = %error, "completion claim failed");
                return DeliveryOutcome::Failed { count: 0, gave_up: 0 };
            }
        };
        if batch.is_empty() {
            return DeliveryOutcome::Empty;
        }

        loop {
            if !admitted() {
                let ids: Vec<_> = batch.iter().map(|record| record.task_id.as_str()).collect();
                router.release(&ids);
                return DeliveryOutcome::Fenced;
            }
            match deliver(&batch).await {
                Ok(()) => {
                    let ids: Vec<_> = batch.iter().map(|record| record.task_id.as_str()).collect();
                    return match router.mark_delivered(&ids) {
                        Ok(_) => DeliveryOutcome::Delivered { count: batch.len() },
                        Err(error) => {
                            tracing::warn!(parent_key, error = %error, "completion acknowledgement failed");
                            DeliveryOutcome::Failed { count: batch.len(), gave_up: 0 }
                        }
                    };
                }
                Err(error) => {
                    tracing::warn!(parent_key, error = %error, "completion delivery turn failed");
                    let ids: Vec<_> = batch.iter().map(|record| record.task_id.as_str()).collect();
                    let gave_up = match router.mark_failed(&ids) {
                        Ok(records) => records,
                        Err(error) => {
                            tracing::warn!(parent_key, error = %error, "completion failure update failed");
                            return DeliveryOutcome::Failed { count: batch.len(), gave_up: 0 };
                        }
                    };
                    if !gave_up.is_empty() {
                        write_undelivered(&gave_up);
                    }
                    if gave_up.len() == batch.len() || !admitted() {
                        return DeliveryOutcome::Failed { count: batch.len(), gave_up: gave_up.len() };
                    }
                    if !sleep_if_admitted(self.config.retry_backoff, admitted).await {
                        return DeliveryOutcome::Failed { count: batch.len(), gave_up: gave_up.len() };
                    }
                    batch = match router.claim_pending(parent_key, self.config.batch_size) {
                        Ok(batch) if !batch.is_empty() => batch,
                        Ok(_) => return DeliveryOutcome::Failed { count: 0, gave_up: gave_up.len() },
                        Err(error) => {
                            tracing::warn!(parent_key, error = %error, "completion retry claim failed");
                            return DeliveryOutcome::Failed { count: 0, gave_up: gave_up.len() };
                        }
                    };
                }
            }
        }
    }
}

async fn sleep_if_admitted<F: FnMut() -> bool>(duration: Duration, admitted: &mut F) -> bool {
    if !admitted() {
        return false;
    }
    if duration.is_zero() {
        return admitted();
    }
    tokio::time::sleep(duration).await;
    admitted()
}

/// Parent turn activity gate. Dropping a lease clears only the guard it owns.
#[derive(Clone, Default)]
pub struct ParentBusyGate(Arc<Mutex<HashSet<(String, u64)>>>);

/// RAII guard marking one parent turn busy.
pub struct ParentBusyLease {
    gate: ParentBusyGate,
    key: String,
    id: u64,
}

impl ParentBusyGate {
    /// Marks a user turn busy. Returns `None` if this parent is already busy.
    pub fn enter(&self, parent_key: impl Into<String>) -> Option<ParentBusyLease> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let key = parent_key.into();
        let mut entries = self.0.lock().ok()?;
        if entries.iter().any(|(existing, _)| existing == &key) {
            return None;
        }
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        entries.insert((key.clone(), id));
        Some(ParentBusyLease { gate: self.clone(), key, id })
    }

    /// Whether a parent has an active busy lease.
    pub fn is_busy(&self, parent_key: &str) -> bool {
        self.0.lock().is_ok_and(|entries| entries.iter().any(|(key, _)| key == parent_key))
    }
}

impl Drop for ParentBusyLease {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.gate.0.lock() {
            entries.remove(&(self.key.clone(), self.id));
        }
    }
}

/// At-most-one live delivery turn per thread, released automatically on drop.
#[derive(Clone, Default)]
pub struct ThreadDeliveryLeases(Arc<Mutex<HashSet<String>>>);

/// RAII exclusive delivery lease for one thread.
pub struct ThreadDeliveryLease {
    leases: ThreadDeliveryLeases,
    key: String,
}

impl ThreadDeliveryLeases {
    /// Acquires the per-thread delivery slot, if currently free.
    pub fn try_acquire(&self, thread_key: impl Into<String>) -> Option<ThreadDeliveryLease> {
        let key = thread_key.into();
        let mut leases = self.0.lock().ok()?;
        if !leases.insert(key.clone()) {
            return None;
        }
        Some(ThreadDeliveryLease { leases: self.clone(), key })
    }
}

impl Drop for ThreadDeliveryLease {
    fn drop(&mut self) {
        if let Ok(mut leases) = self.leases.0.lock() {
            leases.remove(&self.key);
        }
    }
}

/// Host fence check. Kept as a closure in the drain API so lease/fencing policy
/// stays outside the provider-neutral task crate.
pub trait DeliveryFence: Send + Sync {
    /// Whether this worker still owns the right to deliver.
    fn admitted(&self) -> bool;
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
