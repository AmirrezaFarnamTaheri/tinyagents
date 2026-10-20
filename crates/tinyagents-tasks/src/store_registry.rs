//! Keyed, cached provider of [`TaskStore`]s — one store per logical scope.
//!
//! A host that serves several isolated scopes (one workspace directory per
//! project, one per tenant) needs exactly one durable store per scope, opened
//! once and shared thereafter. Opening a second store over the same append log
//! gives two writers with independently replayed state.
//!
//! [`TaskStoreRegistry`] is that cache. It is deliberately unopinionated about
//! what a scope *is*: the key is whatever the host uses to tell scopes apart,
//! and the opener is a host closure, so path layout and durability policy stay
//! with the caller.
//!
//! # Lock poisoning
//!
//! Every accessor returns [`TaskStoreRegistryError`] rather than unwrapping, for
//! the same reason [`DetachedTaskRegistry`](crate::DetachedTaskRegistry) does: a
//! panic in an unrelated task must not turn every later store lookup into a
//! second panic.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::driver_store::DriverTaskStore;
use crate::store::{InMemoryTaskStore, TaskStore};

/// Why a registry lookup could not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStoreRegistryError {
    /// The registry mutex was poisoned by a panic in another task.
    Lock(String),
}

impl std::fmt::Display for TaskStoreRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lock(detail) => write!(f, "task store registry lock poisoned: {detail}"),
        }
    }
}

impl std::error::Error for TaskStoreRegistryError {}

type OpenFn<K> = dyn Fn(&K) -> Arc<dyn TaskStore> + Send + Sync;

/// The registry's cache, guarded for the lifetime of one accessor call.
type StoreGuard<'a, K> = std::sync::MutexGuard<'a, HashMap<K, Arc<dyn TaskStore>>>;

/// A process-wide cache of [`TaskStore`]s keyed by scope.
pub struct TaskStoreRegistry<K: Eq + Hash + Clone + Send + Sync> {
    stores: Mutex<HashMap<K, Arc<dyn TaskStore>>>,
    open: Arc<OpenFn<K>>,
}

impl<K: Eq + Hash + Clone + Send + Sync> TaskStoreRegistry<K> {
    /// Builds a registry that opens missing stores with `open`.
    ///
    /// `open` is infallible by design: a store that cannot be made durable
    /// should degrade to an in-memory one rather than take the caller's
    /// orchestration surface down with it. See
    /// [`open_driver_task_store_or_memory`].
    pub fn new(open: impl Fn(&K) -> Arc<dyn TaskStore> + Send + Sync + 'static) -> Self {
        Self {
            stores: Mutex::new(HashMap::new()),
            open: Arc::new(open),
        }
    }

    /// Returns the store for `key`, opening and caching it on first use.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn get_or_open(&self, key: &K) -> Result<Arc<dyn TaskStore>, TaskStoreRegistryError> {
        let mut stores = self.lock()?;
        if let Some(existing) = stores.get(key) {
            return Ok(Arc::clone(existing));
        }
        let opened = (self.open)(key);
        stores.insert(key.clone(), Arc::clone(&opened));
        Ok(opened)
    }

    /// Returns the store for `key` only if it has already been opened.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn get(&self, key: &K) -> Result<Option<Arc<dyn TaskStore>>, TaskStoreRegistryError> {
        Ok(self.lock()?.get(key).map(Arc::clone))
    }

    /// Every store opened so far, in unspecified order.
    ///
    /// For supervisors that need a view across scopes — reconciling or
    /// reporting over every workspace this process has touched — without
    /// knowing the key set in advance.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn values(&self) -> Result<Vec<Arc<dyn TaskStore>>, TaskStoreRegistryError> {
        Ok(self.lock()?.values().map(Arc::clone).collect())
    }

    /// Number of scopes with an open store.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn len(&self) -> Result<usize, TaskStoreRegistryError> {
        Ok(self.lock()?.len())
    }

    /// `true` when no store has been opened yet.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn is_empty(&self) -> Result<bool, TaskStoreRegistryError> {
        Ok(self.lock()?.is_empty())
    }

    /// Drops every cached store, so the next lookup reopens.
    ///
    /// # Errors
    ///
    /// [`TaskStoreRegistryError::Lock`] when the registry mutex is poisoned.
    pub fn clear(&self) -> Result<(), TaskStoreRegistryError> {
        self.lock()?.clear();
        Ok(())
    }

    fn lock(&self) -> Result<StoreGuard<'_, K>, TaskStoreRegistryError> {
        self.stores
            .lock()
            .map_err(|err| TaskStoreRegistryError::Lock(err.to_string()))
    }
}

impl<K: Eq + Hash + Clone + Send + Sync> std::fmt::Debug for TaskStoreRegistry<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open = self.stores.lock().map(|s| s.len());
        f.debug_struct("TaskStoreRegistry")
            .field("open_stores", &open.ok())
            .finish()
    }
}

/// Opens a durable [`DriverTaskStore`] over `docs`, degrading to an in-memory store.
///
/// A failure to start the store's bridge thread or to declare its collections
/// yields an [`InMemoryTaskStore`] instead of an error: losing durability across
/// a restart is a far smaller harm than refusing to run orchestration at all, so
/// a host whose backend is briefly unavailable can still spawn work.
pub fn open_driver_task_store_or_memory(
    docs: Arc<dyn tinyagents_storage_kit::DocumentStore>,
) -> Arc<dyn TaskStore> {
    let store = match DriverTaskStore::new(docs) {
        Ok(store) => store,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "[orchestration] durable task store unavailable; falling back to memory"
            );
            return Arc::new(InMemoryTaskStore::new());
        }
    };
    // Declare eagerly so an unusable backend degrades now, not at first use.
    if let Err(err) = store.declare_collections() {
        tracing::warn!(
            error = %err,
            "[orchestration] durable task store unavailable; falling back to memory"
        );
        return Arc::new(InMemoryTaskStore::new());
    }
    Arc::new(store)
}
