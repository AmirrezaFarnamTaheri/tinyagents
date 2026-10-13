//! A [`TaskCache`] over `tinystoragedrivers` document ports.
//!
//! [`DriverTaskCache`] keeps cached node results on whatever backend the host
//! opened. The document handle is already bound to a tenant scope, so two
//! tenants never see each other's entries even when graph, node and input hash
//! coincide.
//!
//! # Layout
//!
//! One collection (default [`DriverTaskCache::DEFAULT_COLLECTION`]) with one
//! document per [`TaskCacheKey`]:
//! `{ graph, node, hash, value, expiresAtMs }`. The id is derived from the
//! length-prefixed key parts, so no two keys meet. `expiresAtMs` is a Unix
//! epoch millisecond instant (or `null` for an entry that never expires).
//!
//! # Expiry
//!
//! Expiry is enforced on read, against this process's clock, on every backend:
//! an entry whose instant has passed is a miss and is removed. When the backend
//! advertises native TTL support the collection also declares `expiresAtMs` as
//! its TTL field, so the driver stops serving and may reclaim expired entries
//! itself; the read-side check keeps behaviour identical where it does not.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, DocumentStore, Filter, IndexSpec, Precondition, StorageError,
};
use tokio::sync::OnceCell;

use super::{TaskCache, TaskCacheKey};
use crate::{Result, TinyAgentsError};
use tinyagents_harness::ids::{GraphId, now_ms};

/// Map a storage driver failure onto the graph error type.
fn map_error(error: StorageError) -> TinyAgentsError {
    TinyAgentsError::Graph(format!("task cache storage: {error}"))
}

/// A [`TaskCache`] that stores entries in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverTaskCache {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverTaskCache {
    /// The collection used by [`DriverTaskCache::new`].
    pub const DEFAULT_COLLECTION: &'static str = "graph_task_cache";

    /// Cache in [`Self::DEFAULT_COLLECTION`].
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// Cache in `collection`, so several independent caches can share a backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The declaration a cache using `collection` needs on a backend with or
    /// without native TTL support.
    pub fn collection_spec(collection: impl Into<String>, native_ttl: bool) -> CollectionSpec {
        let spec = CollectionSpec::new(collection).index(IndexSpec::new("by_graph", ["graph"]));
        if native_ttl {
            spec.ttl("expiresAtMs")
        } else {
            spec
        }
    }

    /// The document id for `key`: length-prefixed parts, hashed so it fits any
    /// driver's id limit however long the input hash is.
    fn id(key: &TaskCacheKey) -> String {
        let mut digest = Sha256::new();
        for part in [key.graph_id.as_str(), key.node_id.as_str(), &key.hash] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part.as_bytes());
        }
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                let native = self.docs.capabilities().contains(Capability::Ttl);
                self.docs
                    .ensure_collection(&Self::collection_spec(self.collection.clone(), native))
                    .await
                    .map_err(map_error)
            })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl TaskCache for DriverTaskCache {
    async fn get(&self, key: &TaskCacheKey) -> Result<Option<Value>> {
        self.declared().await?;
        let id = Self::id(key);
        let Some(found) = self
            .docs
            .get(&self.collection, &id)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        let expired = found
            .doc
            .get("expiresAtMs")
            .and_then(Value::as_u64)
            .is_some_and(|at| at <= now_ms());
        if expired {
            // Only the version we saw: a concurrent fresh `put` must survive.
            match self
                .docs
                .delete(&self.collection, &id, Precondition::Version(found.version))
                .await
            {
                Ok(_) => {}
                Err(error) if error.kind() == tinystoragedrivers_core::ErrorKind::Conflict => {}
                Err(error) => return Err(map_error(error)),
            }
            return Ok(None);
        }
        Ok(found.doc.get("value").cloned())
    }

    async fn put(&self, key: &TaskCacheKey, value: Value, ttl: Option<Duration>) -> Result<()> {
        self.declared().await?;
        let expires_at = ttl.map(|ttl| now_ms().saturating_add(ttl.as_millis() as u64));
        let doc = json!({
            "graph": key.graph_id.as_str(),
            "node": key.node_id.as_str(),
            "hash": key.hash,
            "value": value,
            "expiresAtMs": expires_at,
        });
        self.docs
            .put(&self.collection, &Self::id(key), doc, Precondition::None)
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    async fn clear(&self, graph_id: &GraphId) -> Result<()> {
        self.declared().await?;
        self.docs
            .delete_where(&self.collection, &Filter::eq("graph", graph_id.as_str()))
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
