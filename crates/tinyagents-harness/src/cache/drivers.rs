//! A [`ResponseCache`] over `tinystoragedrivers` document ports.
//!
//! [`DriverResponseCache`] keeps cached model responses on whatever backend the
//! host opened (SQLite on a desktop, MongoDB in the cloud, memory in tests), so
//! a restart does not pay the whole provider bill again. The document handle is
//! already bound to a tenant scope; the namespace below separates populations
//! *within* a tenant (a control and an experiment arm, say).
//!
//! # Layout
//!
//! One collection (default [`DriverResponseCache::DEFAULT_COLLECTION`]) with one
//! document per `(namespace, key)`:
//! `{ ns, key, value, expiresAtMs }`. The id is a digest of the length-prefixed
//! pair, so no two entries meet and any key length is accepted.
//!
//! # Expiry
//!
//! `expiresAtMs` is an absolute Unix epoch millisecond instant, because the
//! value has to outlive the process. It is enforced on read on every backend:
//! an expired entry is a miss and is removed. When the backend advertises native
//! TTL support the collection also declares the field, so the driver reclaims
//! expired entries itself.
//!
//! # Statistics
//!
//! [`ResponseCache::stats`] is synchronous, so entry and byte counts (which need
//! a query) stay zero; hits, misses, writes and expirations are counted in this
//! process and shared by every clone of one handle.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::driver_ids::hashed_id;
use async_trait::async_trait;
use serde_json::{Value, json};
use tinyinference_llm::model::ModelResponse;
use tinystoragedrivers_core::{
    Capability, CollectionSpec, DocumentStore, ErrorKind, Filter, IndexSpec, Precondition,
};
use tokio::sync::OnceCell;

use super::types::{CacheStats, ResponseCache};
use crate::error::{Result, TinyAgentsError};
use crate::ids::now_ms;
use crate::store::map_driver_error as map_error;

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
    expirations: AtomicU64,
}

/// A durable [`ResponseCache`] stored in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverResponseCache {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    namespace: String,
    declared: Arc<OnceCell<()>>,
    counters: Arc<Counters>,
}

impl DriverResponseCache {
    /// The collection used by [`DriverResponseCache::new`].
    pub const DEFAULT_COLLECTION: &'static str = "harness_response_cache";
    /// The namespace used when none is given.
    pub const DEFAULT_NAMESPACE: &'static str = "default";

    /// Cache in [`Self::DEFAULT_COLLECTION`] under [`Self::DEFAULT_NAMESPACE`].
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// Cache in `collection`, so independent caches can share a backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            namespace: Self::DEFAULT_NAMESPACE.to_string(),
            declared: Arc::new(OnceCell::new()),
            counters: Arc::new(Counters::default()),
        }
    }

    /// A handle scoped to `namespace`, sharing this handle's backend.
    ///
    /// Two harnesses that must not cross-serve point at the same backend with
    /// different namespaces; [`clear`](ResponseCache::clear) then drops only the
    /// handle's own population.
    #[must_use]
    pub fn with_namespace(&self, namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ..self.clone()
        }
    }

    /// This handle's namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The declaration a cache using `collection` needs.
    pub fn collection_spec(collection: impl Into<String>, native_ttl: bool) -> CollectionSpec {
        let spec = CollectionSpec::new(collection).index(IndexSpec::new("by_ns", ["ns"]));
        if native_ttl {
            spec.ttl("expiresAtMs")
        } else {
            spec
        }
    }

    fn id(&self, key: &str) -> String {
        hashed_id(&[&self.namespace, key])
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
impl ResponseCache for DriverResponseCache {
    async fn get(&self, key: &str) -> Result<Option<ModelResponse>> {
        self.declared().await?;
        let id = self.id(key);
        let Some(found) = self
            .docs
            .get(&self.collection, &id)
            .await
            .map_err(map_error)?
        else {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        let expired = found
            .doc
            .get("expiresAtMs")
            .and_then(Value::as_u64)
            .is_some_and(|at| at <= now_ms());
        if expired {
            // Only the version we saw: a concurrent fresh write must survive.
            match self
                .docs
                .delete(&self.collection, &id, Precondition::Version(found.version))
                .await
            {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(map_error(error)),
            }
            tracing::debug!(key = %key, "[cache] driver entry expired; treating as miss");
            self.counters.expirations.fetch_add(1, Ordering::Relaxed);
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let value =
            found.doc.get("value").cloned().ok_or_else(|| {
                TinyAgentsError::Storage("response cache entry has no value".into())
            })?;
        let response: ModelResponse = serde_json::from_value(value)
            .map_err(|e| TinyAgentsError::Storage(format!("decode response cache entry: {e}")))?;
        self.counters.hits.fetch_add(1, Ordering::Relaxed);
        Ok(Some(response))
    }

    async fn put(&self, key: &str, value: ModelResponse) -> Result<()> {
        self.put_with_ttl(key, value, None).await
    }

    async fn put_with_ttl(
        &self,
        key: &str,
        value: ModelResponse,
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.declared().await?;
        let expires_at = ttl.map(|ttl| now_ms().saturating_add(ttl.as_millis() as u64));
        let doc = json!({
            "ns": self.namespace,
            "key": key,
            "value": serde_json::to_value(&value)
                .map_err(|e| TinyAgentsError::Storage(format!("encode response cache entry: {e}")))?,
            "expiresAtMs": expires_at,
        });
        self.docs
            .put(&self.collection, &self.id(key), doc, Precondition::None)
            .await
            .map_err(map_error)?;
        self.counters.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn clear(&self) -> Result<()> {
        self.declared().await?;
        let dropped = self
            .docs
            .delete_where(&self.collection, &Filter::eq("ns", self.namespace.as_str()))
            .await
            .map_err(map_error)?;
        tracing::debug!(
            namespace = %self.namespace,
            dropped,
            "[cache] cleared the driver response cache namespace"
        );
        Ok(())
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            writes: self.counters.writes.load(Ordering::Relaxed),
            expirations: self.counters.expirations.load(Ordering::Relaxed),
            ..CacheStats::default()
        }
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
