//! [`Store`] and [`AppendStore`] over `tinystoragedrivers` ports.
//!
//! A host that has opened a storage backend (SQLite on a desktop, MongoDB in
//! the cloud, memory in tests) hands the harness a scoped
//! [`DocumentStore`] / [`StreamStore`] and wraps it here; the harness keeps
//! talking to its own narrow traits and never learns which backend it runs
//! on. The handles are already bound to a tenant scope, so nothing here takes
//! one.
//!
//! # Layout
//!
//! - [`DriverStore`] is a view of the shared `tsd.kv` primitive: every harness
//!   namespace is a `Kv` namespace of the store's *owner* (default
//!   [`DriverStore::DEFAULT_OWNER`], `harness`). An entry is the document
//!   `{ "owner", "ns", "key", "value" }` under `Key::of([owner, ns, key])`, so
//!   a namespace or key may hold any characters, `list` is an indexed query,
//!   and the stored value is the caller's JSON unchanged. Stores of different
//!   owners (the session crate's key-value store is one) share the collection
//!   without meeting.
//! - [`DriverAppendStore`] maps each harness stream to the driver stream
//!   `<len>:<prefix><stream>`, the prefix empty when there is none. The
//!   length is always written, so no two `(prefix, stream)` pairs, prefixed
//!   or not, ever address one stream. Offsets are the
//!   driver's dense offsets, which already match the [`AppendStore`]
//!   contract. (The run journal moves to its sealed v2 stream family in a
//!   later change.)

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_core::primitives::{self, Kv};
use tinystoragedrivers_core::{CollectionSpec, DocumentStore, ErrorKind, StorageError, StreamStore};
use tokio::sync::OnceCell;

use super::{AppendStore, SchemaAdmission, Store};
use crate::error::{Result, TinyAgentsError};

/// Map a storage driver failure onto the harness error type. Invalid input
/// (a name the backend cannot store) is a validation error; everything else
/// is a storage error carrying the driver's message.
pub(crate) fn map_error(error: StorageError) -> TinyAgentsError {
    match error.kind() {
        ErrorKind::InvalidInput => TinyAgentsError::Validation(error.to_string()),
        _ => TinyAgentsError::Storage(error.to_string()),
    }
}

/// A harness [`Store`] over the `tsd.kv` primitive of a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverStore {
    docs: Arc<dyn DocumentStore>,
    owner: String,
    declared: Arc<OnceCell<()>>,
    schema_admission: SchemaAdmission,
}

impl DriverStore {
    /// The owner used by [`DriverStore::new`].
    pub const DEFAULT_OWNER: &'static str = "harness";

    /// Store harness namespaces under the owner [`Self::DEFAULT_OWNER`].
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_owner(docs, Self::DEFAULT_OWNER)
    }

    /// Store namespaces under `owner`, so independent stores (the harness's
    /// and the session crate's, say) can share one backend.
    pub fn with_owner(docs: Arc<dyn DocumentStore>, owner: impl Into<String>) -> Self {
        Self::with_owner_and_schema_admission(docs, owner, SchemaAdmission::Declare)
    }

    /// Store namespaces with an immutable schema policy and a fresh admission cache.
    pub fn with_owner_and_schema_admission(
        docs: Arc<dyn DocumentStore>,
        owner: impl Into<String>,
        schema_admission: SchemaAdmission,
    ) -> Self {
        Self {
            docs,
            owner: owner.into(),
            declared: Arc::new(OnceCell::new()),
            schema_admission,
        }
    }

    /// The declaration required by every store: the shared `tsd.kv` collection.
    ///
    /// Trusted hosts can prepare it before issuing actor handles
    /// (`Registry::prepare` declares it too). Returning a declaration grants no
    /// access and does not initialize the backend.
    pub fn collection_spec() -> CollectionSpec {
        primitives::collection_specs()
            .into_iter()
            .find(|spec| spec.name == primitives::KV)
            .expect("the primitives always declare tsd.kv")
    }

    /// The `Kv` namespace `namespace` of this store's owner.
    fn kv(&self, namespace: &str) -> Result<Kv> {
        Kv::ns(&self.docs, self.owner.clone(), namespace).map_err(map_error)
    }

    /// Declare the collection (with its index) once per handle.
    async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                self.schema_admission
                    .admit(self.docs.as_ref(), &Self::collection_spec())
                    .await
                    .map_err(map_error)
            })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl Store for DriverStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        self.declared().await?;
        self.kv(namespace)?
            .get::<Value>(key)
            .await
            .map_err(map_error)
    }

    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        self.declared().await?;
        self.kv(namespace)?
            .put(key, &value)
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        self.declared().await?;
        self.kv(namespace)?
            .delete(key)
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    async fn list(&self, namespace: &str) -> Result<Vec<String>> {
        self.declared().await?;
        Ok(self
            .kv(namespace)?
            .list::<Value>("")
            .await
            .map_err(map_error)?
            .into_iter()
            .map(|(key, _)| key)
            .collect())
    }
}

/// A harness [`AppendStore`] over a driver [`StreamStore`].
#[derive(Debug, Clone)]
pub struct DriverAppendStore {
    streams: Arc<dyn StreamStore>,
    prefix: String,
}

impl DriverAppendStore {
    /// Map each harness stream `s` to the driver stream `0:s`.
    pub fn new(streams: Arc<dyn StreamStore>) -> Self {
        Self::with_prefix(streams, "")
    }

    /// Map each harness stream to `<len>:<prefix><stream>`, so several
    /// independent append stores can share one backend. The prefix length is
    /// part of the name, so `("a", "bc")` and `("ab", "c")` stay apart.
    pub fn with_prefix(streams: Arc<dyn StreamStore>, prefix: impl Into<String>) -> Self {
        Self {
            streams,
            prefix: prefix.into(),
        }
    }

    fn name(&self, stream: &str) -> String {
        format!("{}:{}{stream}", self.prefix.len(), self.prefix)
    }
}

/// Entries read per driver call when following a stream to its end.
const READ_PAGE: usize = 1_000;

#[async_trait]
impl AppendStore for DriverAppendStore {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.streams
            .append(&self.name(stream), value)
            .await
            .map_err(map_error)
    }

    async fn read_from(&self, stream: &str, offset: u64) -> Result<Vec<(u64, Value)>> {
        let name = self.name(stream);
        let mut out = Vec::new();
        let mut next = offset;
        loop {
            let page = self
                .streams
                .read_window(&name, next, READ_PAGE)
                .await
                .map_err(map_error)?;
            let full = page.len() == READ_PAGE;
            for entry in page {
                next = entry.offset + 1;
                out.push((entry.offset, entry.value));
            }
            if !full {
                return Ok(out);
            }
        }
    }

    async fn read_window(
        &self,
        stream: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Value)>> {
        let page = self
            .streams
            .read_window(&self.name(stream), offset, limit)
            .await
            .map_err(map_error)?;
        Ok(page
            .into_iter()
            .map(|entry| (entry.offset, entry.value))
            .collect())
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        self.streams
            .len(&self.name(stream))
            .await
            .map_err(map_error)
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
