//! Durable [`HarnessStatusStore`] over `tinystoragedrivers` document ports, plus
//! the run-id and redaction helpers a host needs to attach a durable journal to
//! a run.
//!
//! The crate ships [`super::types::InMemoryStatusStore`], which does not survive
//! a process restart. [`DriverStatusStore`] keeps one document per run on
//! whatever backend the host opened and answers the lineage and liveness
//! queries with indexed filters, which is what lets a supervisor reattach after
//! the UI died and find every active descendant of a root run.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, Filter, IndexSpec, Precondition, Query,
};
use tokio::sync::OnceCell;

use super::HarnessStatusStore;
use crate::driver_ids::hashed_id;
use crate::error::{Result, TinyAgentsError};
use crate::events::HarnessRunStatus;
use crate::ids::{ExecutionStatus, RunId};
use crate::store::map_driver_error as map_error;

/// Name fragments that mark an environment variable as credential material.
const SECRET_NAME_MARKERS: [&str; 7] = [
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
    "BEARER",
];

/// Shortest value (in bytes, after trimming) worth masking.
const MIN_SECRET_LEN: usize = 8;

/// Mints a fresh, slash-free, process-unique run id (`run.<32-hex>`).
///
/// One id serves a whole turn: the `EventSink::with_stream_id` prefix (so
/// persisted `event_id`s are the restart-stable `{run_id}-evt-{offset}`), the
/// journal stream key, and the status-store key. The `simple()` uuid form (no
/// hyphens) stays inside [`FileStore`]'s allowed-character set.
pub fn mint_run_id() -> RunId {
    RunId::new(format!("run.{}", uuid::Uuid::new_v4().simple()))
}

/// Values of process environment variables whose name looks like a secret
/// (contains `KEY`, `TOKEN`, `SECRET`, `PASSWORD`, `PASSWD`, `CREDENTIAL` or
/// `BEARER`, case-insensitively) and whose trimmed value is at least eight
/// bytes long.
///
/// Seed a [`super::RedactingSink`] with this so an API key or bearer token
/// echoed into model text, a tool argument fragment or an error string is never
/// persisted in the clear. Short values are left alone so unrelated config is
/// not masked.
pub fn process_env_secrets() -> Vec<String> {
    secrets_from_vars(std::env::vars())
}

/// Pure core of [`process_env_secrets`], over an explicit variable list.
pub fn secrets_from_vars(vars: impl IntoIterator<Item = (String, String)>) -> Vec<String> {
    let mut secrets = Vec::new();
    for (name, value) in vars {
        let upper = name.to_ascii_uppercase();
        if SECRET_NAME_MARKERS.iter().any(|m| upper.contains(m))
            && value.trim().len() >= MIN_SECRET_LEN
        {
            secrets.push(value);
        }
    }
    tracing::debug!(
        count = secrets.len(),
        "[journal] redaction policy seeded with secret value(s)"
    );
    secrets
}

/// A durable [`HarnessStatusStore`] stored in a driver [`DocumentStore`].
///
/// One document per run: `{ runId, rootRunId, threadId, active, record }`, where
/// `record` is the full [`HarnessRunStatus`] and the rest are indexed
/// projections for the queries. Each snapshot is compact (ids, phase, counters,
/// timestamps, error), never prompts or payloads. The handle is already bound to
/// a tenant scope, so runs of two tenants never meet.
#[derive(Debug, Clone)]
pub struct DriverStatusStore {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverStatusStore {
    /// The collection used by [`DriverStatusStore::new`].
    pub const DEFAULT_COLLECTION: &'static str = "harness_run_status";

    /// Store statuses in [`Self::DEFAULT_COLLECTION`].
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// Store statuses in `collection`, so independent stores can share a backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The declaration a store using `collection` needs.
    pub fn collection_spec(collection: impl Into<String>) -> CollectionSpec {
        CollectionSpec::new(collection)
            .index(IndexSpec::new("by_root", ["rootRunId"]))
            .index(IndexSpec::new("by_thread", ["threadId"]))
            .index(IndexSpec::new("by_active", ["active"]))
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

    /// Every status matching `filter`. A document that no longer decodes (a
    /// record from an incompatible build) is skipped, never fatal.
    async fn matching(&self, filter: Filter) -> Result<Vec<HarnessRunStatus>> {
        self.declared().await?;
        let rows = self
            .docs
            .query_all(&self.collection, &Query::filter(filter))
            .await
            .map_err(map_error)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let record = row.doc.get("record").cloned()?;
                match serde_json::from_value::<HarnessRunStatus>(record) {
                    Ok(status) => Some(status),
                    Err(err) => {
                        tracing::debug!(
                            "[journal] skipping undecodable run status id={} err={err}",
                            row.id
                        );
                        None
                    }
                }
            })
            .collect())
    }
}

#[async_trait]
impl HarnessStatusStore for DriverStatusStore {
    async fn put_status(&self, status: HarnessRunStatus) -> Result<()> {
        self.declared().await?;
        let doc = json!({
            "runId": status.run_id.as_str(),
            "rootRunId": status.root_run_id.as_str(),
            "threadId": status.thread_id.as_ref().map(|thread| thread.as_str()),
            "active": is_active(&status),
            "record": serde_json::to_value(&status)?,
        });
        self.docs
            .put(
                &self.collection,
                &hashed_id(&[status.run_id.as_str()]),
                doc,
                Precondition::None,
            )
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    async fn get_status(&self, run_id: &str) -> Result<Option<HarnessRunStatus>> {
        self.declared().await?;
        let Some(found) = self
            .docs
            .get(&self.collection, &hashed_id(&[run_id]))
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        let record: Value = found
            .doc
            .get("record")
            .cloned()
            .ok_or_else(|| TinyAgentsError::Storage("run status has no record".into()))?;
        Ok(serde_json::from_value(record).ok())
    }

    async fn list_by_thread(&self, thread_id: &str) -> Result<Vec<HarnessRunStatus>> {
        self.matching(Filter::eq("threadId", thread_id)).await
    }

    async fn list_by_root(&self, root_run_id: &str) -> Result<Vec<HarnessRunStatus>> {
        self.matching(Filter::eq("rootRunId", root_run_id)).await
    }

    async fn list_active(&self) -> Result<Vec<HarnessRunStatus>> {
        self.matching(Filter::eq("active", true)).await
    }
}

/// Whether a run is still live (`Pending`, `Running` or `Interrupted`).
pub fn is_active(status: &HarnessRunStatus) -> bool {
    matches!(
        status.status,
        ExecutionStatus::Pending | ExecutionStatus::Running | ExecutionStatus::Interrupted
    )
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
