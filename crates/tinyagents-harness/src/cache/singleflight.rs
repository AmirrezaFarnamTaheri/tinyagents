//! Stampede protection: collapse concurrent identical cache misses into one
//! provider call.
//!
//! # The gap this fills
//!
//! A cache in front of a slow provider does nothing for the *first* N callers.
//! Ten sub-agents that ask the same sub-question at the same moment all miss,
//! all call the provider, and nine of those calls are pure waste — paid for,
//! rate-limit consuming, and then thrown away when the last writer wins.
//!
//! Neither reference implementation solves this: LangChain's own unit test
//! documents the race rather than preventing it. So this is greenfield, and
//! deliberately small — one in-flight map plus a shared result and
//! `tokio::sync::Notify` per key.
//!
//! # Semantics
//!
//! The first caller for a key becomes the **leader** and runs the closure. Any
//! caller arriving while the leader is in flight becomes a **follower** and
//! waits for the leader's outcome instead of running the closure. Followers
//! receive a clone of the leader's success.
//!
//! Errors are **not** shared: a follower whose leader failed re-runs the
//! closure itself. Sharing the failure would turn one caller's transient 503
//! into every concurrent caller's failure while hiding the fact that each had
//! its own retry budget — and an error is not a value worth caching.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, future::Future};

use crate::Result;
use tinyinference_llm::model::ModelResponse;

/// A typed in-flight registry for callers that need to choose their own
/// follower policy. Entries are shared across clones of this registry.
///
/// Unlike [`SingleFlight`], this primitive does not decide whether followers
/// retry on an error; it publishes the leader's complete typed result.
#[derive(Clone)]
pub struct SharedFlightMap<K, V, E> {
    entries: Arc<Mutex<HashMap<K, SharedFlightEntry<V, E>>>>,
}

impl<K, V, E> Default for SharedFlightMap<K, V, E> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// A claim on one typed in-flight entry.
pub struct SharedFlightEntry<V, E> {
    state: Arc<SharedFlightState<V, E>>,
}

impl<V, E> Clone for SharedFlightEntry<V, E> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

struct SharedFlightState<V, E> {
    result: Mutex<Option<std::result::Result<V, E>>>,
    notify: tokio::sync::Notify,
    waiters: AtomicUsize,
}

struct Waiter<'a>(&'a AtomicUsize);

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<K, V, E> SharedFlightMap<K, V, E>
where
    K: Eq + std::hash::Hash,
{
    /// Creates an empty typed in-flight registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Claims leadership for `key`, or returns the current shared entry.
    pub fn claim(&self, key: K) -> (SharedFlightEntry<V, E>, bool) {
        let mut entries = self.entries.lock().expect("shared-flight map poisoned");
        match entries.get(&key) {
            Some(entry) => (entry.clone(), false),
            None => {
                let entry = SharedFlightEntry {
                    state: Arc::new(SharedFlightState {
                        result: Mutex::new(None),
                        notify: tokio::sync::Notify::new(),
                        waiters: AtomicUsize::new(0),
                    }),
                };
                entries.insert(key, entry.clone());
                (entry, true)
            }
        }
    }

    /// Looks up a current entry for coordination and diagnostics.
    pub fn get(&self, key: &K) -> Option<SharedFlightEntry<V, E>> {
        self.entries
            .lock()
            .expect("shared-flight map poisoned")
            .get(key)
            .cloned()
    }

    /// Removes `key` only if it still points at `entry`.
    pub fn retire(&self, key: &K, entry: &SharedFlightEntry<V, E>) {
        let mut entries = self.entries.lock().expect("shared-flight map poisoned");
        if entries
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current.state, &entry.state))
        {
            entries.remove(key);
        }
    }
}

impl<V: Clone, E: Clone> SharedFlightEntry<V, E> {
    /// Number of callers currently waiting for this leader's result.
    pub fn waiter_count(&self) -> usize {
        self.state.waiters.load(Ordering::Relaxed)
    }

    /// Whether the leader has published a result.
    pub fn is_complete(&self) -> bool {
        self.state
            .result
            .lock()
            .expect("shared-flight result poisoned")
            .is_some()
    }

    /// Waits for the leader to publish its complete result.
    pub async fn wait(&self) -> std::result::Result<V, E> {
        self.state.waiters.fetch_add(1, Ordering::Relaxed);
        let _waiter = Waiter(&self.state.waiters);
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self
                .state
                .result
                .lock()
                .expect("shared-flight result poisoned")
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }

    /// Waits for the leader, returning `None` when this observer is cancelled.
    pub async fn wait_or_cancel(
        &self,
        cancellation: &crate::CancellationToken,
    ) -> Option<std::result::Result<V, E>> {
        self.state.waiters.fetch_add(1, Ordering::Relaxed);
        let _waiter = Waiter(&self.state.waiters);
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self
                .state
                .result
                .lock()
                .expect("shared-flight result poisoned")
                .clone()
            {
                return Some(result);
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return None,
                _ = &mut notified => {}
            }
        }
    }

    /// Publishes a result to all current and future waiters on this entry.
    pub fn complete(&self, result: std::result::Result<V, E>) {
        *self
            .state
            .result
            .lock()
            .expect("shared-flight result poisoned") = Some(result);
        self.state.notify.notify_waiters();
    }
}

/// Collapses concurrent duplicate model calls that share a cache key.
///
/// Cheap to clone; clones share the same in-flight map.
#[derive(Clone, Default)]
pub struct SingleFlight {
    inflight: SharedFlightMap<String, ModelResponse, ()>,
}

/// Removes a leader's entry even when its future is cancelled mid-call.
///
/// An unfinished guard publishes a retry signal before retiring the entry, so
/// followers never wait forever behind an abandoned leader.
struct LeaderGuard {
    inflight: SharedFlightMap<String, ModelResponse, ()>,
    key: String,
    entry: SharedFlightEntry<ModelResponse, ()>,
    finished: bool,
}

impl LeaderGuard {
    fn finish(&mut self, result: &Result<ModelResponse>) {
        self.entry.complete(match result {
            Ok(response) => Ok(response.clone()),
            Err(_) => Err(()),
        });
        self.finished = true;
        self.inflight.retire(&self.key, &self.entry);
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.entry.complete(Err(()));
            self.inflight.retire(&self.key, &self.entry);
        }
    }
}

impl std::fmt::Debug for SingleFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inflight = self.inflight.entries.lock().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("SingleFlight")
            .field("inflight", &inflight)
            .finish()
    }
}

impl SingleFlight {
    /// Creates an empty single-flight gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of keys currently in flight. Intended for tests and diagnostics.
    pub fn inflight_len(&self) -> usize {
        self.inflight.entries.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Runs `call` for `key`, or waits for an already in-flight call with the
    /// same key and returns its result.
    ///
    /// Returns `(response, was_follower)` so the caller can skip a redundant
    /// cache write when it merely rode along on someone else's call.
    pub async fn run<F, Fut>(&self, key: &str, call: F) -> Result<(ModelResponse, bool)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ModelResponse>>,
    {
        // The lock is acquired and released inside this helper so no
        // `MutexGuard` is ever alive across an `await` — which would make the
        // whole future `!Send` and unusable from `tokio::spawn`.
        let (entry, is_leader) = self.inflight.claim(key.to_owned());

        // Follower: wait for the leader rather than duplicating the call.
        if !is_leader {
            tracing::debug!(key = %key, "[cache] joining an in-flight identical model call");
            match entry.wait().await {
                Ok(response) => return Ok((response, true)),
                // Leader failed, or dropped the channel without sending (a
                // cancelled or panicking leader). Either way, run it ourselves.
                Err(()) => {
                    tracing::debug!(
                        key = %key,
                        "[cache] in-flight leader did not produce a response; issuing our own call"
                    );
                    return call().await.map(|response| (response, false));
                }
            }
        }

        // Leader: install the guard before awaiting. If this future is dropped,
        // the guard publishes a retry signal and retires this exact entry;
        // without it, a cancelled leader wedges this key forever.
        let mut leader = LeaderGuard {
            inflight: self.inflight.clone(),
            key: key.to_string(),
            entry,
            finished: false,
        };
        let result = call().await;
        leader.finish(&result);
        result.map(|response| (response, false))
    }
}

#[cfg(test)]
#[path = "singleflight_tests.rs"]
mod shared_tests;
