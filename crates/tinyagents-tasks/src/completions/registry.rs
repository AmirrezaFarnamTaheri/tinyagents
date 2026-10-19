//! Lazily opened completion routers, one per host-defined scope.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tinyagents_harness::error::Result as HarnessResult;
use tinystoragedrivers_core::{Scope, StorageBackend};

use super::{CompletionRouter, CompletionStore, DriverCompletionStore};

/// Outcome of replacing one cached router only if it is still current.
pub enum CompletionRouterReplaceOutcome {
    /// `expected` was cached and was replaced by this router.
    Replaced(Arc<CompletionRouter>),
    /// Another router is cached; callers should use this winner.
    Current(Arc<CompletionRouter>),
    /// No router is cached, commonly because the pool was cleared meanwhile.
    Missing,
}

/// Why a router-pool lookup could not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionRouterPoolError {
    /// The pool mutex was poisoned by a panic in another task.
    Lock(String),
    /// A router could not release its active durable claims. Earlier routers
    /// in the same call may already have released theirs; retry is safe.
    Release(String),
}

impl std::fmt::Display for CompletionRouterPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lock(detail) => write!(f, "completion router pool lock poisoned: {detail}"),
            Self::Release(detail) => write!(f, "completion router pool could not release claims: {detail}"),
        }
    }
}

impl std::error::Error for CompletionRouterPoolError {}

type OpenFn<K> = dyn Fn(&K) -> Arc<CompletionRouter> + Send + Sync;

/// Whether a failed durable completion-store open may use process-local
/// storage. Shared-backend callers must use
/// [`CompletionStoreFallback::FailClosed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionStoreFallback {
    /// Return the backend or declaration error; never make cross-process work
    /// appear durable when it is only held in process memory.
    FailClosed,
    /// Explicitly permit ephemeral local behavior if durable storage cannot be
    /// opened. The caller owns deciding that the scope is local-only.
    LocalMemory,
}

/// Process-local cache of one durable completion router per scope key.
pub struct CompletionRouterPool<K: Eq + Hash + Clone + Send + Sync> {
    routers: Mutex<HashMap<K, Arc<CompletionRouter>>>,
    open: Arc<OpenFn<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync> CompletionRouterPool<K> {
    /// Builds a pool whose host-provided opener decides scope and durability.
    pub fn new(open: impl Fn(&K) -> Arc<CompletionRouter> + Send + Sync + 'static) -> Self {
        Self {
            routers: Mutex::new(HashMap::new()),
            open: Arc::new(open),
        }
    }

    /// Returns the cached router for `key`, opening it once on first access.
    pub fn get_or_open(&self, key: &K) -> Result<Arc<CompletionRouter>, CompletionRouterPoolError> {
        let mut routers = self.lock()?;
        if let Some(router) = routers.get(key) {
            return Ok(Arc::clone(router));
        }
        let router = (self.open)(key);
        routers.insert(key.clone(), Arc::clone(&router));
        Ok(router)
    }

    /// Returns a cached router without opening it.
    pub fn get(&self, key: &K) -> Result<Option<Arc<CompletionRouter>>, CompletionRouterPoolError> {
        Ok(self.lock()?.get(key).map(Arc::clone))
    }

    /// Removes one cached router without disturbing any other scope.
    ///
    /// The returned router and any other caller-held `Arc` remain usable; this
    /// only stops the pool from returning it for future lookups. Durable records
    /// and in-flight claims remain owned by the held router until those handles
    /// are dropped or their claims are settled.
    pub fn remove(
        &self,
        key: &K,
    ) -> Result<Option<Arc<CompletionRouter>>, CompletionRouterPoolError> {
        Ok(self.lock()?.remove(key))
    }

    /// Drops cached routers so their stores can be reopened after a lifecycle
    /// boundary. Durable records remain in the backing store.
    pub fn clear(&self) -> Result<(), CompletionRouterPoolError> {
        self.lock()?.clear();
        Ok(())
    }

    /// Releases all active claims held by cached routers without clearing the
    /// pool. Each router is snapshotted before store I/O, so the pool mutex is
    /// not held while a backend is accessed. On failure, earlier routers may
    /// already have released claims; calling this again is safe.
    pub fn release_all(&self) -> Result<usize, CompletionRouterPoolError> {
        let routers: Vec<_> = self.lock()?.values().map(Arc::clone).collect();
        routers.into_iter().try_fold(0usize, |released, router| {
            router
                .release_all()
                .map(|count| released + count)
                .map_err(|error| CompletionRouterPoolError::Release(error.to_string()))
        })
    }

    /// Atomically replaces `expected` only while it remains the cached router.
    ///
    /// A concurrent replacement wins and is returned as `Current`. If the
    /// entry was removed (for example by [`Self::clear`]), returns `Missing`
    /// without restoring a possibly stale router.
    pub fn replace_if_current(
        &self,
        key: &K,
        expected: &Arc<CompletionRouter>,
        replacement: Arc<CompletionRouter>,
    ) -> Result<CompletionRouterReplaceOutcome, CompletionRouterPoolError> {
        let mut routers = self.lock()?;
        match routers.get(key) {
            Some(current) if Arc::ptr_eq(current, expected) => {
                routers.insert(key.clone(), Arc::clone(&replacement));
                Ok(CompletionRouterReplaceOutcome::Replaced(replacement))
            }
            Some(current) => Ok(CompletionRouterReplaceOutcome::Current(Arc::clone(current))),
            None => Ok(CompletionRouterReplaceOutcome::Missing),
        }
    }

    /// Number of opened scopes.
    pub fn len(&self) -> Result<usize, CompletionRouterPoolError> {
        Ok(self.lock()?.len())
    }

    /// Whether no scope has been opened.
    pub fn is_empty(&self) -> Result<bool, CompletionRouterPoolError> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<K, Arc<CompletionRouter>>>,
        CompletionRouterPoolError,
    > {
        self.routers
            .lock()
            .map_err(|error| CompletionRouterPoolError::Lock(error.to_string()))
    }
}

impl<K: Eq + Hash + Clone + Send + Sync> std::fmt::Debug for CompletionRouterPool<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open = self.routers.lock().map(|routers| routers.len());
        f.debug_struct("CompletionRouterPool")
            .field("open_scopes", &open.ok())
            .finish()
    }
}

/// Opens a durable completion store over the tenant-scoped backend.
///
/// Shared-store setup fails closed; callers that intentionally want local
/// storage can construct [`InMemoryCompletionStore`] explicitly.
pub fn open_driver_completion_store(
    backend: Arc<dyn StorageBackend>,
    scope: Scope,
) -> HarnessResult<Arc<dyn CompletionStore>> {
    let store = DriverCompletionStore::new(backend, scope)
        .map_err(|error| tinyagents_harness::error::TinyAgentsError::Storage(error.to_string()))?;
    store.declare_collections()?;
    Ok(Arc::new(store))
}

/// Opens a driver-backed store, optionally falling back to explicit local
/// memory behavior when the caller has established that the scope is local.
///
/// Shared/durable scopes must pass [`CompletionStoreFallback::FailClosed`].
/// The local-memory option is an explicit policy choice, never an automatic
/// response to a shared-backend failure.
pub fn open_driver_completion_store_or_memory(
    backend: Arc<dyn StorageBackend>,
    scope: Scope,
    fallback: CompletionStoreFallback,
) -> HarnessResult<Arc<dyn CompletionStore>> {
    match open_driver_completion_store(backend, scope) {
        Ok(store) => Ok(store),
        Err(error) if fallback == CompletionStoreFallback::LocalMemory => {
            tracing::warn!(error = %error, "durable completion store unavailable; using explicitly permitted local memory");
            Ok(Arc::new(super::InMemoryCompletionStore::new()))
        }
        Err(error) => Err(error),
    }
}
