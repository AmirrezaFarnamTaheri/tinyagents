//! A [`NamespacedStore`] over `tinystoragedrivers` document ports.
//!
//! [`DriverNamespacedStore`] keeps the hierarchical, TTL-aware long-term store
//! on whatever backend the host opened. The handle is already bound to a tenant
//! scope, so two tenants never see each other's items.
//!
//! # Layout
//!
//! One collection (default [`DriverNamespacedStore::DEFAULT_COLLECTION`]) with
//! one document per `(namespace, key)`:
//!
//! ```text
//! { root, ns: [segments], key, value, createdAtMs, updatedAtMs, expiresAtMs }
//! ```
//!
//! `root` is the namespace's first segment, indexed, so a subtree search or a
//! namespace listing reads one root's documents rather than the collection. The
//! document id is a digest of the length-prefixed segments and key, so no two
//! items meet and any segment or key length is accepted.
//!
//! # Semantics
//!
//! The matching rules are the in-memory store's, applied by the same functions:
//! field filters, case-insensitive substring `query`, `*` wildcards in
//! namespace listings, deterministic `(namespace, key)` order for paging. Prefix
//! narrowing by root is the only part the backend does; everything else runs on
//! the returned documents, so a subtree search costs the size of its root.
//!
//! Expiry is enforced on read against this process's clock on every backend,
//! and [`sweep_expired`](NamespacedStore::sweep_expired) reclaims space. When the
//! backend advertises native TTL the collection declares `expiresAtMs` too.
//!
//! Operations in one [`batch`](NamespacedStore::batch) run in order and each is
//! individually atomic; a batch is not a transaction.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, DocumentStore, DocumentStoreExt, ErrorKind, Filter, IndexSpec,
    Precondition, Query, Versioned,
};
use tokio::sync::OnceCell;

use super::{
    Item, ListNamespacesQuery, Namespace, NamespacedStore, SearchQuery, StoreOp, StoreResult,
    TtlConfig, expiry_at, matches_filter, matches_query, matches_wildcards,
};
use crate::driver_ids::hashed_id;
use crate::error::{Result, TinyAgentsError};
use crate::ids::now_ms;
use crate::store::map_driver_error as map_error;

/// How many times a read-modify-write retries a version conflict.
const CAS_ATTEMPTS: usize = 16;

/// A [`NamespacedStore`] stored in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverNamespacedStore {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    ttl: TtlConfig,
    declared: Arc<OnceCell<()>>,
}

impl DriverNamespacedStore {
    /// The collection used by [`DriverNamespacedStore::new`].
    pub const DEFAULT_COLLECTION: &'static str = "harness_namespaced_store";

    /// Store in [`Self::DEFAULT_COLLECTION`] with no expiry.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// Store in `collection`, so independent stores can share a backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            ttl: TtlConfig::default(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// Sets the TTL policy.
    #[must_use]
    pub fn with_ttl(mut self, ttl: TtlConfig) -> Self {
        self.ttl = ttl;
        self
    }

    /// The declaration a store using `collection` needs.
    pub fn collection_spec(collection: impl Into<String>, native_ttl: bool) -> CollectionSpec {
        let spec = CollectionSpec::new(collection).index(IndexSpec::new("by_root", ["root"]));
        if native_ttl {
            spec.ttl("expiresAtMs")
        } else {
            spec
        }
    }

    fn id(namespace: &Namespace, key: &str) -> String {
        let mut parts: Vec<&str> = namespace.segments().iter().map(String::as_str).collect();
        parts.push(key);
        hashed_id(&parts)
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

    fn doc(item: &Item) -> Value {
        json!({
            "root": item.namespace.segments().first(),
            "ns": item.namespace.segments(),
            "key": item.key,
            "value": item.value,
            "createdAtMs": item.created_at_ms,
            "updatedAtMs": item.updated_at_ms,
            "expiresAtMs": item.expires_at_ms,
        })
    }

    fn item(doc: &Value) -> Result<Item> {
        let bad = |what: &str| TinyAgentsError::Storage(format!("namespaced item has no {what}"));
        let segments: Vec<String> = doc
            .get("ns")
            .and_then(Value::as_array)
            .ok_or_else(|| bad("namespace"))?
            .iter()
            .filter_map(|segment| segment.as_str().map(str::to_owned))
            .collect();
        Ok(Item {
            namespace: Namespace(segments),
            key: doc
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| bad("key"))?
                .to_owned(),
            value: doc.get("value").cloned().ok_or_else(|| bad("value"))?,
            created_at_ms: doc.get("createdAtMs").and_then(Value::as_u64).unwrap_or(0),
            updated_at_ms: doc.get("updatedAtMs").and_then(Value::as_u64).unwrap_or(0),
            expires_at_ms: doc.get("expiresAtMs").and_then(Value::as_u64),
        })
    }

    /// Live items of the subtrees rooted at `prefix`'s first segment (every
    /// root when the prefix is empty), with their versions.
    async fn live_items(&self, prefix: &[String], now: u64) -> Result<Vec<Item>> {
        let query = match prefix.first() {
            Some(root) => Query::filter(Filter::eq("root", root.as_str())),
            None => Query::default(),
        };
        let rows = self
            .docs
            .query_all(&self.collection, &query)
            .await
            .map_err(map_error)?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let item = Self::item(&row.doc)?;
            if !item.is_expired(now) {
                items.push(item);
            }
        }
        Ok(items)
    }

    async fn get_item(
        &self,
        namespace: &Namespace,
        key: &str,
        refresh_ttl: Option<bool>,
        now: u64,
    ) -> Result<Option<Item>> {
        let id = Self::id(namespace, key);
        for _ in 0..CAS_ATTEMPTS {
            let Some(row) = self
                .docs
                .get(&self.collection, &id)
                .await
                .map_err(map_error)?
            else {
                return Ok(None);
            };
            let mut item = Self::item(&row.doc)?;
            if item.is_expired(now) {
                // Only the version we saw: a concurrent fresh write must survive.
                match self
                    .docs
                    .delete(&self.collection, &id, Precondition::Version(row.version))
                    .await
                {
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(map_error(error)),
                }
                return Ok(None);
            }
            let refresh = refresh_ttl.unwrap_or(self.ttl.refresh_on_read);
            let Some(minutes) = self.ttl.default_ttl_minutes.filter(|_| refresh) else {
                return Ok(Some(item));
            };
            item.expires_at_ms = expiry_at(now, Some(minutes));
            item.updated_at_ms = now;
            match self
                .docs
                .put(
                    &self.collection,
                    &id,
                    Self::doc(&item),
                    Precondition::Version(row.version),
                )
                .await
            {
                Ok(_) => return Ok(Some(item)),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(map_error(error)),
            }
        }
        Err(TinyAgentsError::Storage(
            "namespaced store read contention budget exhausted".into(),
        ))
    }

    async fn put_item(
        &self,
        namespace: &Namespace,
        key: &str,
        value: Value,
        ttl_minutes: Option<f64>,
        now: u64,
    ) -> Result<()> {
        let id = Self::id(namespace, key);
        let ttl = ttl_minutes.or(self.ttl.default_ttl_minutes);
        for _ in 0..CAS_ATTEMPTS {
            let previous: Option<Versioned<Value>> = self
                .docs
                .get(&self.collection, &id)
                .await
                .map_err(map_error)?;
            let created = previous
                .as_ref()
                .and_then(|row| row.doc.get("createdAtMs").and_then(Value::as_u64))
                .unwrap_or(now);
            let item = Item {
                namespace: namespace.clone(),
                key: key.to_owned(),
                value: value.clone(),
                created_at_ms: created,
                updated_at_ms: now,
                expires_at_ms: expiry_at(now, ttl),
            };
            let precondition = previous.as_ref().map_or(Precondition::Absent, |row| {
                Precondition::Version(row.version)
            });
            match self
                .docs
                .put(&self.collection, &id, Self::doc(&item), precondition)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(map_error(error)),
            }
        }
        Err(TinyAgentsError::Storage(
            "namespaced store write contention budget exhausted".into(),
        ))
    }

    async fn search_items(&self, query: &SearchQuery, now: u64) -> Result<Vec<Item>> {
        let mut found: Vec<Item> = self
            .live_items(&query.namespace_prefix, now)
            .await?
            .into_iter()
            .filter(|item| item.namespace.starts_with(&query.namespace_prefix))
            .filter(|item| matches_filter(item, &query.filter))
            .filter(|item| {
                query
                    .query
                    .as_ref()
                    .is_none_or(|needle| matches_query(item, needle))
            })
            .collect();
        found.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| a.key.cmp(&b.key))
        });
        Ok(found
            .into_iter()
            .skip(query.offset)
            .take(query.limit.unwrap_or(usize::MAX))
            .collect())
    }

    async fn list_namespace_items(
        &self,
        query: &ListNamespacesQuery,
        now: u64,
    ) -> Result<Vec<Namespace>> {
        // A wildcard first segment cannot narrow by root.
        let narrow: Vec<String> = query
            .prefix
            .iter()
            .flatten()
            .take(1)
            .filter(|segment| segment.as_str() != "*")
            .cloned()
            .collect();
        let mut seen: BTreeSet<Namespace> = BTreeSet::new();
        for item in self.live_items(&narrow, now).await? {
            let ns = &item.namespace;
            if let Some(prefix) = &query.prefix
                && !(ns.segments().len() >= prefix.len()
                    && matches_wildcards(&ns.segments()[..prefix.len()], prefix))
            {
                continue;
            }
            if let Some(suffix) = &query.suffix {
                let len = ns.segments().len();
                if !(len >= suffix.len()
                    && matches_wildcards(&ns.segments()[len - suffix.len()..], suffix))
                {
                    continue;
                }
            }
            let truncated = match query.max_depth {
                Some(depth) if ns.segments().len() > depth => {
                    Namespace(ns.segments()[..depth].to_vec())
                }
                _ => ns.clone(),
            };
            seen.insert(truncated);
        }
        Ok(seen
            .into_iter()
            .skip(query.offset)
            .take(query.limit.unwrap_or(usize::MAX))
            .collect())
    }
}

#[async_trait]
impl NamespacedStore for DriverNamespacedStore {
    fn ttl_config(&self) -> TtlConfig {
        self.ttl
    }

    async fn sweep_expired(&self) -> Result<usize> {
        self.declared().await?;
        let now = now_ms();
        let rows = self
            .docs
            .query_all(&self.collection, &Query::default())
            .await
            .map_err(map_error)?;
        let mut reclaimed = 0;
        for row in rows {
            if !Self::item(&row.doc)?.is_expired(now) {
                continue;
            }
            match self
                .docs
                .delete(
                    &self.collection,
                    &row.id,
                    Precondition::Version(row.version),
                )
                .await
            {
                Ok(removed) => reclaimed += usize::from(removed),
                // Refreshed or rewritten since we read it: no longer expired.
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(map_error(error)),
            }
        }
        if reclaimed > 0 {
            tracing::debug!("[store:namespaced] sweep_expired reclaimed={reclaimed}");
        }
        Ok(reclaimed)
    }

    async fn batch(&self, ops: &[StoreOp]) -> Result<Vec<StoreResult>> {
        self.declared().await?;
        let now = now_ms();
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            out.push(match op {
                StoreOp::Get {
                    namespace,
                    key,
                    refresh_ttl,
                } => {
                    namespace.validate()?;
                    StoreResult::Item(
                        self.get_item(namespace, key, *refresh_ttl, now)
                            .await?
                            .map(Box::new),
                    )
                }
                StoreOp::Put {
                    namespace,
                    key,
                    value,
                    ttl_minutes,
                } => {
                    namespace.validate()?;
                    match value {
                        None => {
                            self.docs
                                .delete(
                                    &self.collection,
                                    &Self::id(namespace, key),
                                    Precondition::None,
                                )
                                .await
                                .map_err(map_error)?;
                        }
                        Some(value) => {
                            self.put_item(namespace, key, value.clone(), *ttl_minutes, now)
                                .await?;
                        }
                    }
                    StoreResult::Ack
                }
                StoreOp::Search(query) => StoreResult::Items(self.search_items(query, now).await?),
                StoreOp::ListNamespaces(query) => {
                    StoreResult::Namespaces(self.list_namespace_items(query, now).await?)
                }
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
