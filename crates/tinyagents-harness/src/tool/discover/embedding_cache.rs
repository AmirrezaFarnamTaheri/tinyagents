//! An [`EmbeddingCache`] over `tinystoragedrivers` document ports.
//!
//! [`DriverEmbeddingCache`] keeps rebuildable catalogue vectors on whatever
//! backend the host opened, so a cold process pays for the catalogue once.
//! The document handle is already bound to a tenant scope; the embedding-space
//! signature keeps vectors of different models apart within it.
//!
//! # Layout
//!
//! One collection (default [`DriverEmbeddingCache::DEFAULT_COLLECTION`]) with
//! one document per `(signature, key)`: `{ sig, key, vector }`. `key` is the
//! ranker's 64-bit hash, written as 16 hex digits because a JSON/BSON number
//! cannot carry a full `u64`. Writes are per-entry upserts, so concurrent
//! writers with disjoint deltas never replace each other's entries and a
//! repeated store is idempotent. A `signature` is never compared against
//! another's vectors.
//!
//! A vector with a non-finite component cannot be represented in JSON and is
//! skipped rather than stored: the ranker would reject it on load anyway.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{CollectionSpec, DocumentStore, IndexSpec, Precondition};
use tokio::sync::OnceCell;

use super::EmbeddingCache;
use crate::driver_ids::hashed_id;
use crate::error::{Result, TinyAgentsError};
use crate::store::map_driver_error as map_error;

/// Document ids fetched per concurrent wave when loading.
const LOAD_CONCURRENCY: usize = 32;

/// A durable [`EmbeddingCache`] stored in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverEmbeddingCache {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverEmbeddingCache {
    /// The collection used by [`DriverEmbeddingCache::new`].
    pub const DEFAULT_COLLECTION: &'static str = "harness_embedding_cache";

    /// Cache in [`Self::DEFAULT_COLLECTION`].
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// Cache in `collection`, so independent caches can share a backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The declaration a cache using `collection` needs.
    pub fn collection_spec(collection: impl Into<String>) -> CollectionSpec {
        CollectionSpec::new(collection).index(IndexSpec::new("by_signature", ["sig"]))
    }

    fn id(signature: &str, key: u64) -> String {
        hashed_id(&[signature, &format!("{key:016x}")])
    }

    async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                self.docs
                    .ensure_collection(&Self::collection_spec(self.collection.clone()))
                    .await
                    .map_err(map_error)
            })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl EmbeddingCache for DriverEmbeddingCache {
    async fn load(&self, signature: &str, keys: &[u64]) -> Result<HashMap<u64, Vec<f32>>> {
        self.declared().await?;
        let mut found = HashMap::new();
        for wave in keys.chunks(LOAD_CONCURRENCY) {
            let reads = wave.iter().map(|key| {
                let id = Self::id(signature, *key);
                async move { (*key, self.docs.get(&self.collection, &id).await) }
            });
            for (key, row) in futures::future::join_all(reads).await {
                let Some(row) = row.map_err(map_error)? else {
                    continue;
                };
                // A document from another signature can only collide by hash
                // accident; the stored signature is the authority.
                if row.doc.get("sig").and_then(Value::as_str) != Some(signature) {
                    continue;
                }
                let Some(vector) = row.doc.get("vector").and_then(Value::as_array) else {
                    continue;
                };
                let vector: Option<Vec<f32>> = vector
                    .iter()
                    .map(|component| component.as_f64().map(|value| value as f32))
                    .collect();
                if let Some(vector) = vector {
                    found.insert(key, vector);
                }
            }
        }
        Ok(found)
    }

    async fn store(&self, signature: &str, values: &[(u64, Vec<f32>)]) -> Result<()> {
        self.declared().await?;
        for (key, vector) in values {
            if vector.iter().any(|component| !component.is_finite()) {
                tracing::debug!(
                    key,
                    "[tool-search] not persisting a vector with a non-finite component"
                );
                continue;
            }
            let doc = json!({
                "sig": signature,
                "key": format!("{key:016x}"),
                "vector": vector,
            });
            self.docs
                .put(
                    &self.collection,
                    &Self::id(signature, *key),
                    doc,
                    Precondition::None,
                )
                .await
                .map_err(map_error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "embedding_cache_tests.rs"]
mod tests;

// Keep the error type referenced for docs.
#[allow(dead_code)]
type _Unused = TinyAgentsError;
