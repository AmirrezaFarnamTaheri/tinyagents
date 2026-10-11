//! Versioned record envelopes, scoped declarations, conditional updates and paging.

use super::DriverRunLedger;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStoreExt, ErrorKind, Filter, IndexSpec, Precondition, Query, Sort,
    StorageError, Versioned,
};

pub(super) const WORKFLOWS: &str = "run_ledger_workflow_runs";
pub(super) const TELEMETRY: &str = "run_ledger_telemetry";
pub(super) const AGENTS: &str = "run_ledger_agent_runs";
pub(super) const TEAMS: &str = "run_ledger_teams";
pub(super) const MEMBERS: &str = "run_ledger_members";
pub(super) const EFFECTS: &str = "run_ledger_tool_effects";
pub(super) const IMPORTS: &str = "run_ledger_agent_imports";
pub(super) const EVENTS: &str = "run_ledger_event_outbox";
pub(super) const EVENT_HEADS: &str = "run_ledger_event_heads";
pub(super) const TASKS: &str = "run_ledger_tasks";
pub(super) const CAS_ATTEMPTS: usize = 64;

/// Length-prefixed identifiers preserve arbitrary classic IDs without exceeding driver limits.
pub(super) fn key(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    hex::encode(digest.finalize())
}

/// Ignore trim-empty filter inputs, retaining nonblank values verbatim as SQL does.
pub(super) fn eq_nonblank(filter: Filter, field: &str, value: Option<&str>) -> Filter {
    match value.filter(|value| !value.trim().is_empty()) {
        Some(value) => filter.and(Filter::eq(field, value)),
        None => filter,
    }
}

/// Declare only portable indexes; expiry never deletes durable run metadata.
pub(super) fn specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(IMPORTS),
        CollectionSpec::new(EVENT_HEADS),
        CollectionSpec::new(EVENTS).index(IndexSpec::new("run_sequence", ["record.runId", "record.sequence"]).unique()),
        CollectionSpec::new(EFFECTS)
            .index(IndexSpec::new("run", ["record.runId"]))
            .index(IndexSpec::new("status", ["record.status"]))
            .index(IndexSpec::new("started", ["startedSql"])),
        CollectionSpec::new(WORKFLOWS)
            .index(IndexSpec::new("definition", ["record.definitionId"]))
            .index(IndexSpec::new("status", ["record.status"]))
            .index(IndexSpec::new("thread", ["record.parentThreadId"]))
            .index(IndexSpec::new("updated", ["updatedSql"]))
            .index(IndexSpec::new("lease", ["record.leaseExpiresAt"])),
        CollectionSpec::new(TELEMETRY).index(IndexSpec::new("updated", ["updatedSql"])),
        CollectionSpec::new(AGENTS)
            .index(IndexSpec::new("status", ["record.status"]))
            .index(IndexSpec::new("kind", ["record.kind"]))
            .index(IndexSpec::new("parent", ["record.parentRunId"]))
            .index(IndexSpec::new("thread", ["record.parentThreadId"]))
            .index(IndexSpec::new("updated", ["updatedSql"]))
            .index(IndexSpec::new("worker", ["record.workerThreadId"])),
        CollectionSpec::new(TEAMS)
            .index(IndexSpec::new("thread", ["record.parentThreadId"]))
            .index(IndexSpec::new("status", ["record.status"]))
            .index(IndexSpec::new("updated", ["updatedSql"])),
        CollectionSpec::new(MEMBERS)
            .index(IndexSpec::new("team", ["record.teamId"]))
            .index(IndexSpec::new("team_name", ["record.teamId", "record.name"]).unique())
            .index(IndexSpec::new("created", ["createdSql"])),
        CollectionSpec::new(TASKS)
            .index(IndexSpec::new("team", ["record.teamId"]))
            .index(IndexSpec::new("status", ["record.status"]))
            .index(IndexSpec::new("claimed", ["record.claimedByMemberId"]))
            .index(IndexSpec::new(
                "order",
                ["record.teamId", "record.orderIndex", "createdSql"],
            )),
    ]
}

/// Store SQL-compatible timestamp sort keys alongside the domain's camelCase serde shape.
pub(super) fn encode<T: Serialize + DeserializeOwned>(record: &T) -> Result<Value, StorageError> {
    let record = serde_json::to_value(record)?;
    // JSON may silently encode nonfinite floats as null; refuse a body that
    // the same domain type cannot read back before any native write occurs.
    let _: T = serde_json::from_value(record.clone())?;
    let sql_time = |field: &str| -> Result<Value, StorageError> {
        record
            .get(field)
            .and_then(Value::as_str)
            .map(|time| {
                DateTime::parse_from_rfc3339(time)
                    .map(|time| Value::String(time.with_timezone(&Utc).to_rfc3339()))
                    .map_err(|_| StorageError::serialization("invalid ledger timestamp"))
            })
            .transpose()
            .map(|time| time.unwrap_or(Value::Null))
    };
    Ok(
        json!({"schemaVersion":1,"updatedSql":sql_time("updatedAt")?,"createdSql":sql_time("createdAt")?,"startedSql":sql_time("startedAt")?,"record":record}),
    )
}

pub(super) fn decode<T: DeserializeOwned>(value: Value) -> Result<T, StorageError> {
    if value.get("schemaVersion").and_then(Value::as_u64) != Some(1) {
        return Err(StorageError::serialization(
            "unsupported run-ledger document schema",
        ));
    }
    serde_json::from_value(
        value
            .get("record")
            .cloned()
            .ok_or_else(|| StorageError::serialization("missing ledger record"))?,
    )
    .map_err(Into::into)
}

impl DriverRunLedger {
    /// Retry failed declarations on a later operation instead of silently using an undeclared layout.
    pub(super) async fn declare(&self) -> Result<(), StorageError> {
        self.declared
            .get_or_try_init(|| async {
                for spec in specs() {
                    if self.validate_collections {
                        self.scoped.documents().validate_collection_spec(&spec).await?;
                    } else {
                        self.scoped.documents().ensure_collection(&spec).await?;
                    }
                }
                Ok::<(), StorageError>(())
            })
            .await
            .map(|_| ())
    }

    pub(super) async fn read<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<Versioned<T>>, StorageError> {
        self.declare().await?;
        self.scoped
            .documents()
            .get(collection, &key(&[id]))
            .await?
            .map(|found| {
                Ok(Versioned {
                    id: found.id,
                    version: found.version,
                    doc: decode(found.doc)?,
                })
            })
            .transpose()
    }

    /// A merge returns precisely its committed body; a concurrent successor cannot change its read-back.
    pub(super) async fn merge<T, F>(
        &self,
        collection: &str,
        id: &str,
        merge: F,
    ) -> Result<T, StorageError>
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync,
        F: Fn(Option<T>) -> T + Send + Sync,
    {
        self.change(collection, id, |previous| {
            let record = merge(previous);
            Ok((Some(record.clone()), record))
        })
        .await
    }

    /// Evaluate eligibility against the read version, re-evaluating only native CAS conflicts.
    pub(super) async fn change<T, R, F>(
        &self,
        collection: &str,
        id: &str,
        change: F,
    ) -> Result<R, StorageError>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
        R: Send,
        F: Fn(Option<T>) -> Result<(Option<T>, R), StorageError> + Send + Sync,
    {
        for _ in 0..CAS_ATTEMPTS {
            let previous = self.read::<T>(collection, id).await?;
            let condition = previous
                .as_ref()
                .map(|row| Precondition::Version(row.version))
                .unwrap_or(Precondition::Absent);
            let (record, result) = change(previous.map(|row| row.doc))?;
            let Some(record) = record else {
                return Ok(result);
            };
            match self
                .scoped
                .documents()
                .put(collection, &key(&[id]), encode(&record)?, condition)
                .await
            {
                Ok(_) => return Ok(result),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::conflict(
            "run-ledger update contention budget exhausted",
        ))
    }

    pub(super) async fn records<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        query: Query,
    ) -> Result<Vec<T>, StorageError> {
        self.declare().await?;
        self.scoped
            .documents()
            .query_all(collection, &query)
            .await?
            .into_iter()
            .map(|row| decode(row.doc))
            .collect()
    }

    /// Count all matches, then follow driver cursors for the requested SQL-style offset/page.
    pub(super) async fn page<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        filter: Filter,
        limit: u64,
        offset: u64,
    ) -> Result<(Vec<T>, usize), StorageError> {
        if offset > i64::MAX as u64 {
            return Err(StorageError::invalid_input(
                "run-ledger offset exceeds i64::MAX",
            ));
        }
        self.declare().await?;
        let count = usize::try_from(self.scoped.documents().count(collection, &filter).await?)
            .map_err(|_| StorageError::invalid_input("run-ledger count exceeds usize"))?;
        let mut records = Vec::new();
        if limit == 0 {
            return Ok((records, count));
        }
        let mut skip = offset;
        let mut query = Query::filter(filter)
            .sort(Sort::desc("updatedSql"))
            .limit(256);
        loop {
            let page = self.scoped.documents().query(collection, &query).await?;
            for row in page.items {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                records.push(decode(row.doc)?);
                if records.len() as u64 == limit {
                    return Ok((records, count));
                }
            }
            match page.next {
                Some(cursor) => query.cursor = Some(cursor),
                None => return Ok((records, count)),
            }
        }
    }
}
