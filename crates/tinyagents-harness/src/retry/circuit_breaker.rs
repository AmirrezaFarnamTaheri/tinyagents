//! A shared, per-key failure breaker for providers that remain unavailable
//! across independent run contexts.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};

/// Backoff and terminal failure settings for [`PersistentFailoverBreaker`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PersistentFailoverBreakerConfig {
    /// Delay after the first recorded failure.
    pub base_backoff_ms: u64,
    /// Maximum delay between attempts.
    pub max_backoff_ms: u64,
    /// Number of consecutive failures after which the caller should stop
    /// scheduling retries until an operator or another policy resets the key.
    pub failure_limit: u32,
}

impl Default for PersistentFailoverBreakerConfig {
    fn default() -> Self {
        Self {
            base_backoff_ms: 30_000,
            max_backoff_ms: 15 * 60_000,
            failure_limit: 8,
        }
    }
}

/// A cloneable breaker. Clones share state, so independently built run
/// contexts can coordinate through one instance. Keys are stored as supplied;
/// callers are responsible for including their complete tenant/provider/model
/// identity without lossy concatenation.
pub struct PersistentFailoverBreaker<K> {
    config: PersistentFailoverBreakerConfig,
    entries: Arc<Mutex<HashMap<K, Entry>>>,
}

impl<K> Clone for PersistentFailoverBreaker<K> {
    fn clone(&self) -> Self {
        Self {
            config: self.config,
            entries: Arc::clone(&self.entries),
        }
    }
}

impl<K> Default for PersistentFailoverBreaker<K> {
    fn default() -> Self {
        Self::new(PersistentFailoverBreakerConfig::default())
    }
}

impl<K> PersistentFailoverBreaker<K>
where
    K: Eq + Hash + Clone,
{
    /// Creates a breaker with explicit backoff and failure-limit settings.
    pub fn new(config: PersistentFailoverBreakerConfig) -> Self {
        Self {
            config,
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Reserves one attempt for `key` at caller-supplied monotonic time
    /// `now_ms`. Pass the same key and breaker from every run context that
    /// should share outage state. This is synchronous and never holds a lock
    /// across an await point.
    pub fn try_acquire(
        &self,
        key: K,
        now_ms: u64,
    ) -> Result<PersistentFailoverAttempt<K>, BreakerRejection> {
        let mut entries = self.lock_entries();
        let entry = entries.entry(key.clone()).or_default();
        if entry.in_flight {
            return Err(BreakerRejection::InFlight);
        }
        if entry.failures > 0 && now_ms < entry.next_attempt_ms {
            return Err(BreakerRejection::Backoff {
                next_attempt_ms: entry.next_attempt_ms,
                failure_count: entry.failures,
                failure_limit_reached: entry.failures >= self.config.failure_limit,
            });
        }
        entry.generation = entry.generation.wrapping_add(1).max(1);
        entry.in_flight = true;
        Ok(PersistentFailoverAttempt {
            key,
            generation: entry.generation,
            entries: Arc::clone(&self.entries),
            config: self.config,
            finished: false,
        })
    }

    /// Clears the failure streak for `key`. A live attempt retains ownership
    /// and can still report its completion; its generation is advanced so a
    /// stale guard cannot mutate later state.
    pub fn reset(&self, key: &K) {
        let mut entries = self.lock_entries();
        let entry = entries.entry(key.clone()).or_default();
        entry.failures = 0;
        entry.next_attempt_ms = 0;
        entry.in_flight = false;
        entry.generation = entry.generation.wrapping_add(1).max(1);
        // Any active attempt is invalidated and its slot released. Its stale
        // guard cannot mutate a later attempt's state.
    }

    fn lock_entries(&self) -> MutexGuard<'_, HashMap<K, Entry>> {
        self.entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Entry {
    failures: u32,
    next_attempt_ms: u64,
    in_flight: bool,
    generation: u64,
}

/// Why a persistent breaker did not admit an attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerRejection {
    /// Another run context currently owns the attempt for this key.
    InFlight,
    /// The key is still within its backoff interval.
    Backoff {
        /// Earliest time, in the caller's monotonic millisecond clock, to retry.
        next_attempt_ms: u64,
        /// Consecutive failures recorded for this key.
        failure_count: u32,
        /// Whether the configured failure cap has been reached.
        failure_limit_reached: bool,
    },
    /// The configured consecutive-failure cap has been reached.
    FailureLimit {
        /// The next eligibility time after the capped cooldown.
        next_attempt_ms: u64,
        /// Consecutive failures recorded for this key.
        failure_count: u32,
    },
}

/// Result of recording an admitted attempt's failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PersistentFailoverFailure {
    /// Consecutive failures after recording this failure.
    pub failure_count: u32,
    /// Earliest retry time on the caller's monotonic millisecond clock.
    pub next_attempt_ms: u64,
    /// Whether `failure_count` reached the configured cap.
    pub failure_limit_reached: bool,
}

/// RAII ownership of one in-flight attempt. Dropping it releases the slot
/// without recording a failure, including when a future is cancelled.
pub struct PersistentFailoverAttempt<K> {
    key: K,
    generation: u64,
    entries: Arc<Mutex<HashMap<K, Entry>>>,
    config: PersistentFailoverBreakerConfig,
    finished: bool,
}

impl<K> PersistentFailoverAttempt<K>
where
    K: Eq + Hash,
{
    /// Records success and resets the consecutive failure count.
    pub fn record_success(&mut self) {
        let mut entries = self.entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = entries.get_mut(&self.key)
            && entry.generation == self.generation
            && entry.in_flight
        {
            entry.failures = 0;
            entry.next_attempt_ms = 0;
            entry.in_flight = false;
            entry.generation = entry.generation.wrapping_add(1).max(1);
        }
        self.finished = true;
    }

    /// Records failure using caller-supplied monotonic time `now_ms`.
    /// Returns `None` if this guard has already completed or has been made
    /// stale by an explicit reset.
    pub fn record_failure(&mut self, now_ms: u64) -> Option<PersistentFailoverFailure> {
        if self.finished {
            return None;
        }
        let mut entries = self.entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = entries.get_mut(&self.key)?;
        if entry.generation != self.generation || !entry.in_flight {
            self.finished = true;
            return None;
        }
        entry.failures = entry.failures.saturating_add(1);
        let exponent = entry.failures.saturating_sub(1).min(63);
        let delay = self
            .config
            .base_backoff_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.config.max_backoff_ms);
        entry.next_attempt_ms = now_ms.saturating_add(delay);
        entry.in_flight = false;
        entry.generation = entry.generation.wrapping_add(1).max(1);
        self.finished = true;
        Some(PersistentFailoverFailure {
            failure_count: entry.failures,
            next_attempt_ms: entry.next_attempt_ms,
            failure_limit_reached: entry.failures >= self.config.failure_limit,
        })
    }
}

impl<K> Drop for PersistentFailoverAttempt<K>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut entries = self.entries.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = entries.get_mut(&self.key)
            && entry.generation == self.generation
            && entry.in_flight
        {
            entry.in_flight = false;
            entry.generation = entry.generation.wrapping_add(1).max(1);
        }
    }
}

#[cfg(test)]
#[path = "circuit_breaker_tests.rs"]
mod tests;
