//! Harness store module — long-term key-value storage backends.
//!
//! In the recursive architecture the store is the durable, shared substrate
//! that outlives any single run: parent and child runs, sub-agents, and
//! nested executions read and write the same namespaced values, so a
//! deeply nested call can persist a result that a sibling or a later turn picks
//! up. It is the harness-side persistence layer, distinct from graph
//! checkpointing.
//!
//! The store is the persistence layer for harness runtime data: events, model
//! call records, tool call records, message history, artifacts, and memory. It
//! is intentionally separate from graph checkpointing (which belongs to the
//! graph module) and from prompt/model context assembly (which belongs to the
//! model and prompt modules).
//!
//! # Primary types
//! - [`Store`] — the core async trait every backend implements.
//! - [`namespaced::NamespacedStore`] — the hierarchical, TTL-aware,
//!   batch-oriented long-term store. Additive: `Store` is unchanged, and
//!   [`namespaced::FlatNamespacedStore`] adapts the new trait back to it.
//! - [`InMemoryStore`] — ephemeral in-process store for tests and examples.
//! - [`DriverStore`] / [`DriverAppendStore`] — the durable backends: any
//!   `tinystoragedrivers` backend (SQLite, MongoDB, ...) the host opened.
//! - [`StoreRegistry`] — named bag of stores injected into `RunContext`.
//! - [`conformance`] — contract suites (`run_store_conformance`,
//!   `run_namespaced_store_conformance`) any backend of either trait can be
//!   certified against.
//!
//! # Namespace convention
//! Use slash-free, lowercase names like `"threads"`, `"events"`, `"cache"`,
//! `"artifacts"`. The registry does not enforce a naming scheme, but
//! consistent names make multi-store applications easier to audit.

pub mod conformance;
mod drivers;
pub mod namespaced;
mod schema_admission;
mod types;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

pub use types::*;

pub(crate) use drivers::map_error as map_driver_error;
pub use drivers::{DriverAppendStore, DriverStore};
pub use schema_admission::SchemaAdmission;

use crate::error::{Result, TinyAgentsError};
use crate::ids::now_ms;

// ── InMemoryStore ─────────────────────────────────────────────────────────────

impl InMemoryStore {
    /// Creates a new, empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        let data = self
            .data
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("store lock poisoned: {e}")))?;
        Ok(data.get(namespace).and_then(|ns| ns.get(key)).cloned())
    }

    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("store lock poisoned: {e}")))?;
        data.entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("store lock poisoned: {e}")))?;
        if let Some(ns) = data.get_mut(namespace) {
            ns.remove(key);
        }
        Ok(())
    }

    async fn list(&self, namespace: &str) -> Result<Vec<String>> {
        let data = self
            .data
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("store lock poisoned: {e}")))?;
        Ok(data
            .get(namespace)
            .map(|ns| ns.keys().cloned().collect())
            .unwrap_or_default())
    }
}

// ── InMemoryAppendStore ─────────────────────────────────────────────────────────

impl InMemoryAppendStore {
    /// Creates a new, empty, **unbounded** in-memory append store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps every stream at `max` retained entries, evicting the **oldest**
    /// entries once a stream grows past the cap.
    ///
    /// Offsets stay monotonically increasing across eviction: an evicted
    /// offset is no longer readable (`read_from` silently starts at the oldest
    /// retained entry when asked for an earlier offset), and `len` keeps
    /// returning the logical stream length. A `max` of `0` retains nothing —
    /// every append is immediately evicted (offsets still advance).
    ///
    /// The default (via [`Self::new`] / [`Default`]) is unbounded, preserving
    /// full journal semantics for tests and replay.
    pub fn with_max_entries_per_stream(mut self, max: usize) -> Self {
        self.max_entries_per_stream = Some(max);
        self
    }
}

#[async_trait]
impl AppendStore for InMemoryAppendStore {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        let mut streams = self
            .streams
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("append store lock poisoned: {e}")))?;
        let buf = streams.entry(stream.to_string()).or_default();
        let offset = buf.base_offset + buf.entries.len() as u64;
        buf.entries.push_back((now_ms(), value));
        if let Some(max) = self.max_entries_per_stream {
            while buf.entries.len() > max {
                buf.entries.pop_front();
                buf.base_offset += 1;
            }
        }
        Ok(offset)
    }

    async fn read_from(&self, stream: &str, offset: u64) -> Result<Vec<(u64, Value)>> {
        let streams = self
            .streams
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("append store lock poisoned: {e}")))?;
        let Some(buf) = streams.get(stream) else {
            return Ok(Vec::new());
        };
        // Entries before `base_offset` have been evicted; reading from an
        // evicted offset resumes at the oldest retained entry.
        let skip = offset.saturating_sub(buf.base_offset) as usize;
        Ok(buf
            .entries
            .iter()
            .enumerate()
            .skip(skip)
            .map(|(i, (_ts, value))| (buf.base_offset + i as u64, value.clone()))
            .collect())
    }

    async fn read_window(
        &self,
        stream: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Value)>> {
        let streams = self
            .streams
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("append store lock poisoned: {e}")))?;
        let Some(buf) = streams.get(stream) else {
            return Ok(Vec::new());
        };
        let skip = offset.saturating_sub(buf.base_offset) as usize;
        Ok(buf
            .entries
            .iter()
            .enumerate()
            .skip(skip)
            .take(limit)
            .map(|(i, (_ts, value))| (buf.base_offset + i as u64, value.clone()))
            .collect())
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        let streams = self
            .streams
            .lock()
            .map_err(|e| TinyAgentsError::Validation(format!("append store lock poisoned: {e}")))?;
        Ok(streams
            .get(stream)
            .map(|buf| buf.base_offset + buf.entries.len() as u64)
            .unwrap_or(0))
    }
}

// ── StoreRegistry ─────────────────────────────────────────────────────────────

impl StoreRegistry {
    /// Creates a registry with a built-in default in-memory store.
    ///
    /// Named stores can be added with [`Self::register`].
    pub fn new() -> Self {
        Self {
            stores: HashMap::new(),
            default_store: Arc::new(InMemoryStore::new()),
        }
    }

    /// Registers `store` under `name`.
    ///
    /// Replaces any previously registered store with the same name. Returns
    /// `&mut self` for convenient builder-style chaining.
    pub fn register(&mut self, name: impl Into<String>, store: Arc<dyn Store>) -> &mut Self {
        self.stores.insert(name.into(), store);
        self
    }

    /// Looks up a named store by `name`, returning `None` if no store with
    /// that name has been registered.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Store>> {
        self.stores.get(name).cloned()
    }

    /// Returns the built-in default in-memory store.
    ///
    /// This store is always available regardless of registered backends.
    pub fn default_store(&self) -> Arc<dyn Store> {
        Arc::clone(&self.default_store)
    }
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
