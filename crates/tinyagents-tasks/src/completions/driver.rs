//! A [`CompletionStore`] over `tinystoragedrivers` document ports.
//!
//! [`DriverCompletionStore`] keeps the latest [`CompletionRecord`] per task on
//! whatever backend the host opened, so a completion the router reported as
//! stored survives a restart. The document handle is already bound to a tenant
//! scope.
//!
//! # Layout
//!
//! One collection (default [`DriverCompletionStore::DEFAULT_COLLECTION`]) with
//! one document per task id:
//! `{ taskId, parentKey, state, updatedAtMs, record }`. The scalar fields are
//! indexed projections for [`CompletionStore::list`] and
//! [`CompletionStore::compact`]; `record` is the full record. `put` is an
//! upsert, which is all the contract asks of it: the router serialises its
//! read-modify-write sequences.
//!
//! # Sync trait
//!
//! [`CompletionStore`] is synchronous, so each call runs its port futures on a
//! [`Blocking`] bridge thread; this works from any caller, inside a tokio
//! runtime or not.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tinyagents_harness::driver_ids::hashed_id;
use tinyagents_harness::error::{Result, TinyAgentsError};
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, DocumentStoreExt, Filter, IndexSpec, Precondition,
    Query, StorageError,
};
use tokio::sync::OnceCell;

use super::store::CompletionStore;
use super::types::{CompletionRecord, CompletionState};

const LOG_PREFIX: &str = "[completion-store]";

fn map_error(error: StorageError) -> TinyAgentsError {
    TinyAgentsError::Storage(format!("completion store: {error}"))
}

fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |elapsed| {
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    })
}

fn decode(record: &Value) -> Option<CompletionRecord> {
    match serde_json::from_value(record.clone()) {
        Ok(record) => Some(record),
        Err(error) => {
            // One unreadable record must not block every other completion.
            tracing::warn!(error = %error, "{LOG_PREFIX} skipping unreadable record");
            None
        }
    }
}

fn decode_strict(record: Option<&Value>) -> Result<CompletionRecord> {
    let value = record
        .ok_or_else(|| TinyAgentsError::Storage("completion store row has no record".into()))?;
    serde_json::from_value(value.clone()).map_err(|error| {
        TinyAgentsError::Storage(format!(
            "completion store contains unreadable record: {error}"
        ))
    })
}

/// A durable [`CompletionStore`] stored in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverCompletionStore {
    docs: Arc<dyn DocumentStore>,
    bridge: Blocking,
    collection: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverCompletionStore {
    /// The collection used by [`DriverCompletionStore::new`].
    pub const DEFAULT_COLLECTION: &'static str = "orchestration_completions";

    /// Store completions in [`Self::DEFAULT_COLLECTION`], with its own bridge.
    ///
    /// # Errors
    ///
    /// When the bridge's runtime thread cannot start.
    pub fn new(docs: Arc<dyn DocumentStore>) -> std::result::Result<Self, StorageError> {
        Ok(Self::with_bridge(docs, Blocking::new()?))
    }

    /// Store completions in [`Self::DEFAULT_COLLECTION`], running the
    /// synchronous trait on `bridge` so several stores share one thread.
    pub fn with_bridge(docs: Arc<dyn DocumentStore>, bridge: Blocking) -> Self {
        Self::with_collection(docs, bridge, Self::DEFAULT_COLLECTION)
    }

    /// Store completions in `collection`, so independent stores can share a backend.
    pub fn with_collection(
        docs: Arc<dyn DocumentStore>,
        bridge: Blocking,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            docs,
            bridge,
            collection: collection.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The declaration a store using `collection` needs.
    pub fn collection_spec(collection: impl Into<String>) -> CollectionSpec {
        CollectionSpec::new(collection)
            .index(IndexSpec::new("by_parent", ["parentKey"]))
            .index(IndexSpec::new("by_state_updated", ["state", "updatedAtMs"]))
    }

    /// Declares the backing collection eagerly so a host can decide whether to
    /// keep this durable store or fall back to memory before accepting work.
    pub fn declare_collections(&self) -> Result<()> {
        let this = self.clone();
        self.run(async move { this.declare().await })
    }

    fn run<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        self.bridge
            .run(work)
            .map_err(|e| TinyAgentsError::Storage(format!("completion store bridge: {e}")))?
    }

    async fn declare(&self) -> Result<()> {
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

impl CompletionStore for DriverCompletionStore {
    fn get(&self, task_id: &str) -> Option<CompletionRecord> {
        let this = self.clone();
        let id = hashed_id(&[task_id]);
        self.run(async move {
            this.declare().await?;
            let found = this
                .docs
                .get(&this.collection, &id)
                .await
                .map_err(map_error)?;
            Ok(found.and_then(|row| decode(row.doc.get("record")?)))
        })
        .inspect_err(|error| tracing::warn!(error = %error, "{LOG_PREFIX} read failed"))
        .ok()
        .flatten()
    }

    fn put(&self, record: &CompletionRecord) -> Result<()> {
        let this = self.clone();
        let record = record.clone();
        self.run(async move {
            this.declare().await?;
            let doc = json!({
                "taskId": record.task_id,
                "parentKey": record.parent_key,
                "state": serde_json::to_value(record.state)?,
                "updatedAtMs": millis(record.updated_at),
                "record": serde_json::to_value(&record)?,
            });
            this.docs
                .put(
                    &this.collection,
                    &hashed_id(&[&record.task_id]),
                    doc,
                    Precondition::None,
                )
                .await
                .map(|_| ())
                .map_err(map_error)
        })
    }

    fn list(&self, parent_key: Option<&str>) -> Vec<CompletionRecord> {
        let this = self.clone();
        let parent_key = parent_key.map(str::to_owned);
        self.run(async move {
            this.declare().await?;
            let filter = match &parent_key {
                Some(parent) => Filter::eq("parentKey", parent.as_str()),
                None => Filter::default(),
            };
            let rows = this
                .docs
                .query_all(&this.collection, &Query::filter(filter))
                .await
                .map_err(map_error)?;
            Ok(rows
                .into_iter()
                .filter_map(|row| decode(row.doc.get("record")?))
                .collect())
        })
        .inspect_err(|error| tracing::warn!(error = %error, "{LOG_PREFIX} list failed"))
        .unwrap_or_default()
    }

    fn try_list(&self, parent_key: Option<&str>) -> Result<Vec<CompletionRecord>> {
        let this = self.clone();
        let parent_key = parent_key.map(str::to_owned);
        self.run(async move {
            this.declare().await?;
            let filter = match &parent_key {
                Some(parent) => Filter::eq("parentKey", parent.as_str()),
                None => Filter::default(),
            };
            let rows = this
                .docs
                .query_all(&this.collection, &Query::filter(filter))
                .await
                .map_err(map_error)?;
            rows.into_iter()
                .map(|row| decode_strict(row.doc.get("record")))
                .collect()
        })
    }

    fn compact(&self, retain: Duration) -> Result<usize> {
        let this = self.clone();
        let cutoff = millis(SystemTime::now())
            .saturating_sub(u64::try_from(retain.as_millis()).unwrap_or(u64::MAX));
        self.run(async move {
            this.declare().await?;
            // Settled means anything but pending. `lt` on the millisecond
            // projection finds candidates; the exact instant is re-checked on
            // the record so a sub-millisecond boundary never drops live work.
            let filter = Filter::ne("state", serde_json::to_value(CompletionState::Pending)?)
                .and(Filter::lt("updatedAtMs", cutoff.saturating_add(1)));
            let rows = this
                .docs
                .query_all(&this.collection, &Query::filter(filter))
                .await
                .map_err(map_error)?;
            let now = SystemTime::now();
            let mut dropped = 0;
            for row in rows {
                let Some(record) = row.doc.get("record").and_then(decode) else {
                    continue;
                };
                let expired = record.state != CompletionState::Pending
                    && now
                        .duration_since(record.updated_at)
                        .is_ok_and(|age| age > retain);
                if !expired {
                    continue;
                }
                // Only the version we saw: a record refreshed since is live again.
                match this
                    .docs
                    .delete(
                        &this.collection,
                        &row.id,
                        Precondition::Version(row.version),
                    )
                    .await
                {
                    Ok(removed) => dropped += usize::from(removed),
                    Err(error) if error.kind() == tinystoragedrivers_core::ErrorKind::Conflict => {}
                    Err(error) => return Err(map_error(error)),
                }
            }
            tracing::debug!(dropped, "{LOG_PREFIX} compacted");
            Ok(dropped)
        })
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
