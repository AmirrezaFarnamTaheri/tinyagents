//! A [`TaskStore`] over `tinystoragedrivers` document ports.
//!
//! [`DriverTaskStore`] keeps orchestration task records and their per-task
//! lifecycle history on whatever backend the host opened (SQLite on a desktop,
//! MongoDB in the cloud, memory in tests). The document handle is already bound
//! to a tenant scope, so two tenants' tasks never meet even when they share a
//! task id.
//!
//! # Layout
//!
//! Two collections, named from a prefix (default `orchestration`):
//!
//! - `<prefix>_tasks`: the latest record per task,
//!   `{ taskId, status, kind, parentRunId, rootRunId, threadId, nodeId, rev, record }`.
//!   The scalar fields are indexed projections for [`TaskStore::list`]; `rev`
//!   counts transitions.
//! - `<prefix>_task_history`: one immutable snapshot per transition,
//!   `{ taskId, rev, record }`, so [`TaskStore::history`] returns the whole
//!   timeline oldest to newest.
//!
//! # Transitions
//!
//! Every transition reads the record, applies the same pure state machine
//! [`InMemoryTaskStore`](crate::InMemoryTaskStore) uses, and commits the new
//! record together with its history snapshot in one atomic batch guarded by the
//! version it read. A concurrent writer makes the batch conflict and the
//! transition is re-evaluated against the winner's record, so two executors
//! racing to settle one task cannot both succeed.
//!
//! # Sync trait
//!
//! [`TaskStore`] is synchronous, so each call runs its port futures on a
//! [`Blocking`] bridge thread; this works from any caller, inside a tokio
//! runtime or not.

use std::sync::Arc;

use serde_json::{Value, json};
use tinyagents_harness::driver_ids::hashed_id;
use tinyagents_harness::error::{Result, TinyAgentsError};
use tinyagents_harness::ids::TaskId;
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, DocumentStoreExt, ErrorKind, Filter, IndexSpec,
    Precondition, Query, Sort, StorageError, WriteOp,
};
use tokio::sync::OnceCell;

use crate::store::{control_outcome, orchestration_not_found, transition};
use crate::types::{
    OrchestrationControlOutcome, OrchestrationTaskFilter, OrchestrationTaskRecord,
    OrchestrationTaskResult, OrchestrationTaskSpec,
};

/// How many times a transition retries a version conflict.
const CAS_ATTEMPTS: usize = 64;

fn map_error(error: StorageError) -> TinyAgentsError {
    TinyAgentsError::Storage(format!("task store: {error}"))
}

fn decode(record: Value) -> Result<OrchestrationTaskRecord> {
    serde_json::from_value(record)
        .map_err(|e| TinyAgentsError::Storage(format!("task store: decode record: {e}")))
}

/// A durable [`TaskStore`] stored in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverTaskStore {
    docs: Arc<dyn DocumentStore>,
    bridge: Blocking,
    tasks: String,
    history: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverTaskStore {
    /// The collection prefix used by [`DriverTaskStore::new`].
    pub const DEFAULT_PREFIX: &'static str = "orchestration";

    /// Store tasks in the `orchestration_*` collections, with its own bridge.
    ///
    /// # Errors
    ///
    /// When the bridge's runtime thread cannot start.
    pub fn new(docs: Arc<dyn DocumentStore>) -> std::result::Result<Self, StorageError> {
        Ok(Self::with_bridge(docs, Blocking::new()?))
    }

    /// Store tasks in the `orchestration_*` collections, running the
    /// synchronous trait on `bridge` so several stores share one thread.
    pub fn with_bridge(docs: Arc<dyn DocumentStore>, bridge: Blocking) -> Self {
        Self::with_prefix(docs, bridge, Self::DEFAULT_PREFIX)
    }

    /// Store tasks in `<prefix>_tasks` and `<prefix>_task_history`, so
    /// independent stores can share a backend.
    pub fn with_prefix(
        docs: Arc<dyn DocumentStore>,
        bridge: Blocking,
        prefix: impl AsRef<str>,
    ) -> Self {
        let prefix = prefix.as_ref();
        Self {
            docs,
            bridge,
            tasks: format!("{prefix}_tasks"),
            history: format!("{prefix}_task_history"),
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// The declarations a store using `prefix` needs.
    pub fn collection_specs(prefix: impl AsRef<str>) -> Vec<CollectionSpec> {
        let prefix = prefix.as_ref();
        vec![
            CollectionSpec::new(format!("{prefix}_tasks"))
                .index(IndexSpec::new("status", ["status"]))
                .index(IndexSpec::new("root", ["rootRunId"]))
                .index(IndexSpec::new("parent", ["parentRunId"]))
                .index(IndexSpec::new("thread", ["threadId"]))
                .index(IndexSpec::new("node", ["nodeId"]))
                .index(IndexSpec::new("kind", ["kind"])),
            CollectionSpec::new(format!("{prefix}_task_history"))
                .index(IndexSpec::new("task_rev", ["taskId", "rev"]).unique()),
        ]
    }

    fn run<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T>> + Send + 'static,
    ) -> Result<T> {
        self.bridge
            .run(work)
            .map_err(|e| TinyAgentsError::Storage(format!("task store bridge: {e}")))?
    }

    async fn declare(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                let prefix = self.tasks.trim_end_matches("_tasks");
                for spec in Self::collection_specs(prefix) {
                    self.docs.ensure_collection(&spec).await.map_err(map_error)?;
                }
                Ok::<(), TinyAgentsError>(())
            })
            .await
            .map(|_| ())
    }

    fn task_doc(record: &OrchestrationTaskRecord, rev: u64) -> Result<Value> {
        Ok(json!({
            "taskId": record.spec.task_id.as_str(),
            "status": serde_json::to_value(record.status)?,
            "kind": record.spec.kind.as_str(),
            "parentRunId": record.spec.parent_run_id.as_ref().map(|id| id.as_str()),
            "rootRunId": record.spec.root_run_id.as_ref().map(|id| id.as_str()),
            "threadId": record.spec.thread_id.as_ref().map(|id| id.as_str()),
            "nodeId": record.spec.node_id.as_ref().map(|id| id.as_str()),
            "rev": rev,
            "record": serde_json::to_value(record)?,
        }))
    }

    fn history_write(record: &OrchestrationTaskRecord, rev: u64, history: &str) -> Result<WriteOp> {
        Ok(WriteOp::Put {
            collection: history.to_owned(),
            id: hashed_id(&[record.spec.task_id.as_str(), &rev.to_string()]),
            doc: json!({
                "taskId": record.spec.task_id.as_str(),
                "rev": rev,
                "record": serde_json::to_value(record)?,
            }),
            precondition: Precondition::Absent,
        })
    }

    /// Read `task_id`, apply `change`, and commit the result with its history
    /// snapshot, retrying only version conflicts.
    fn modify<R: Send + 'static>(
        &self,
        task_id: &TaskId,
        change: impl Fn(&mut OrchestrationTaskRecord) -> Result<R> + Send + Sync + 'static,
    ) -> Result<R> {
        let this = self.clone();
        let task_id = task_id.clone();
        self.run(async move {
            this.declare().await?;
            let id = hashed_id(&[task_id.as_str()]);
            for _ in 0..CAS_ATTEMPTS {
                let Some(found) = this
                    .docs
                    .get(&this.tasks, &id)
                    .await
                    .map_err(map_error)?
                else {
                    return Err(orchestration_not_found(&task_id));
                };
                let rev = found.doc.get("rev").and_then(Value::as_u64).unwrap_or(0);
                let mut record =
                    decode(found.doc.get("record").cloned().unwrap_or(Value::Null))?;
                let outcome = change(&mut record)?;
                let next = rev + 1;
                let writes = vec![
                    WriteOp::Put {
                        collection: this.tasks.clone(),
                        id: id.clone(),
                        doc: Self::task_doc(&record, next)?,
                        precondition: Precondition::Version(found.version),
                    },
                    Self::history_write(&record, next, &this.history)?,
                ];
                match this.docs.atomic_batch(writes).await {
                    Ok(_) => return Ok(outcome),
                    Err(error) if error.kind() == ErrorKind::Conflict => continue,
                    Err(error) => return Err(map_error(error)),
                }
            }
            Err(TinyAgentsError::Storage(
                "task store: transition contention budget exhausted".into(),
            ))
        })
    }
}

impl crate::store::TaskStore for DriverTaskStore {
    fn insert(&self, spec: OrchestrationTaskSpec) -> Result<OrchestrationTaskRecord> {
        let this = self.clone();
        self.run(async move {
            this.declare().await?;
            let record = OrchestrationTaskRecord::pending(spec);
            let id = hashed_id(&[record.spec.task_id.as_str()]);
            let writes = vec![
                WriteOp::Put {
                    collection: this.tasks.clone(),
                    id,
                    doc: Self::task_doc(&record, 1)?,
                    precondition: Precondition::Absent,
                },
                Self::history_write(&record, 1, &this.history)?,
            ];
            match this.docs.atomic_batch(writes).await {
                Ok(_) => Ok(record),
                Err(error)
                    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) =>
                {
                    Err(TinyAgentsError::DuplicateComponent(format!(
                        "orchestration task `{}`",
                        record.spec.task_id
                    )))
                }
                Err(error) => Err(map_error(error)),
            }
        })
    }

    fn get(&self, task_id: &TaskId) -> Option<OrchestrationTaskRecord> {
        let this = self.clone();
        let id = hashed_id(&[task_id.as_str()]);
        self.run(async move {
            this.declare().await?;
            let found = this.docs.get(&this.tasks, &id).await.map_err(map_error)?;
            found
                .map(|row| decode(row.doc.get("record").cloned().unwrap_or(Value::Null)))
                .transpose()
        })
        .inspect_err(|error| tracing::warn!("[orchestration] task store read failed: {error}"))
        .ok()
        .flatten()
    }

    fn list(&self, filter: OrchestrationTaskFilter) -> Vec<OrchestrationTaskRecord> {
        let this = self.clone();
        self.run(async move {
            this.declare().await?;
            let mut query_filter = Filter::default();
            for (field, value) in [
                ("rootRunId", filter.root_run_id.as_ref().map(|id| id.as_str())),
                ("parentRunId", filter.parent_run_id.as_ref().map(|id| id.as_str())),
                ("threadId", filter.thread_id.as_ref().map(|id| id.as_str())),
                ("nodeId", filter.node_id.as_ref().map(|id| id.as_str())),
                ("kind", filter.kind.as_deref()),
            ] {
                if let Some(value) = value {
                    query_filter = query_filter.and(Filter::eq(field, value));
                }
            }
            if let Some(status) = filter.status {
                query_filter =
                    query_filter.and(Filter::eq("status", serde_json::to_value(status)?));
            }
            let rows = this
                .docs
                .query_all(&this.tasks, &Query::filter(query_filter))
                .await
                .map_err(map_error)?;
            let mut records = Vec::with_capacity(rows.len());
            for row in rows {
                let record = decode(row.doc.get("record").cloned().unwrap_or(Value::Null))?;
                // The time window is the part the projections cannot answer.
                if filter.matches(&record) {
                    records.push(record);
                }
            }
            records.sort_by(|a, b| a.spec.task_id.as_str().cmp(b.spec.task_id.as_str()));
            Ok(records)
        })
        .inspect_err(|error| tracing::warn!("[orchestration] task store list failed: {error}"))
        .unwrap_or_default()
    }

    fn history(&self, task_id: &TaskId) -> Vec<OrchestrationTaskRecord> {
        let this = self.clone();
        let task_id = task_id.as_str().to_owned();
        self.run(async move {
            this.declare().await?;
            let rows = this
                .docs
                .query_all(
                    &this.history,
                    &Query::filter(Filter::eq("taskId", task_id)).sort(Sort::asc("rev")),
                )
                .await
                .map_err(map_error)?;
            rows.into_iter()
                .map(|row| decode(row.doc.get("record").cloned().unwrap_or(Value::Null)))
                .collect()
        })
        .inspect_err(|error| tracing::warn!("[orchestration] task history read failed: {error}"))
        .unwrap_or_default()
    }

    fn mark_running(&self, task_id: &TaskId) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, |record| {
            transition::mark_running(record)?;
            Ok(record.clone())
        })
    }

    fn mark_awaiting(&self, task_id: &TaskId) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, |record| {
            transition::mark_awaiting(record)?;
            Ok(record.clone())
        })
    }

    fn mark_awaiting_with_question(
        &self,
        task_id: &TaskId,
        question: String,
    ) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, move |record| {
            transition::mark_awaiting_with_question(record, question.clone())?;
            Ok(record.clone())
        })
    }

    fn complete(
        &self,
        task_id: &TaskId,
        result: OrchestrationTaskResult,
    ) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, move |record| {
            transition::complete(record, result.clone())?;
            Ok(record.clone())
        })
    }

    fn fail(&self, task_id: &TaskId, error: String) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, move |record| {
            transition::fail(record, error.clone())?;
            Ok(record.clone())
        })
    }

    fn timeout(&self, task_id: &TaskId, error: String) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, move |record| {
            transition::timeout(record, error.clone())?;
            Ok(record.clone())
        })
    }

    fn request_cancel(&self, task_id: &TaskId) -> Result<OrchestrationControlOutcome> {
        self.modify(task_id, |record| {
            let message = transition::request_cancel(record)?;
            Ok(control_outcome(record, message))
        })
    }

    fn mark_cancelled(&self, task_id: &TaskId) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, |record| {
            transition::mark_cancelled(record)?;
            Ok(record.clone())
        })
    }

    fn kill(&self, task_id: &TaskId) -> Result<OrchestrationControlOutcome> {
        self.modify(task_id, |record| {
            let message = transition::kill(record)?;
            Ok(control_outcome(record, message))
        })
    }

    fn set_timeout_ms(&self, task_id: &TaskId, timeout_ms: u64) -> Result<OrchestrationTaskRecord> {
        self.modify(task_id, move |record| {
            transition::set_timeout_ms(record, timeout_ms)?;
            Ok(record.clone())
        })
    }
}

#[cfg(test)]
#[path = "driver_store_tests.rs"]
mod tests;
