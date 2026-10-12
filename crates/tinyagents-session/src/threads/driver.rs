//! Admitted scoped conversation metadata and immutable committed-prefix readers.
//!
//! No host paths/backend lookup, FullText shortcut, or persistent process cache.
//! Plain append uses a recoverable intent; edits and distributed leases remain unsupported.

use super::persistence::*;
use super::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, ErrorKind, Filter, Precondition, Query, ScopedStorage,
    StorageError, StreamStore, Versioned,
};
mod append;
mod pending;
mod reader;

const COLLECTION: &str = "conversation_threads_v1";
const PAGE_SIZE: usize = 64;
const RETRIES: usize = 32;

#[derive(Clone, Serialize, Deserialize)]
struct ThreadDoc {
    schema: u32,
    incarnation: String,
    deleted: bool,
    visible_end: u64,
    thread: ConversationThread,
}
#[derive(Serialize, Deserialize)]
struct MessageRecord {
    schema: u32,
    incarnation: String,
    message: ThreadMessage,
}

/// Conversation handle whose scope/native admission has already been bound.
///
/// Schema1 metadata edits commit one document; append atomically publishes its
/// metadata and retires a companion intent. Committed messages
/// live in an immutable per-incarnation prefix. This adapter exposes no message
/// importer or claimed commit; plain append requires its own prepared intent collection.
#[derive(Clone)]
pub struct DriverConversationStore {
    docs: Arc<dyn DocumentStore>,
    streams: Arc<dyn StreamStore>,
    validate_only: bool,
}
impl DriverConversationStore {
    /// Consume explicit trusted scoped ports with unchanged legacy schema behavior.
    ///
    /// This constructor neither declares nor validates application schema and
    /// does not establish actor schema safety. Use explicit trusted preparation
    /// and [`Self::new_validate_only`] for restricted actor admission. No ambient
    /// backend or file fallback is possible.
    pub fn new(scoped: ScopedStorage) -> Self {
        Self {
            docs: scoped.documents().clone(),
            streams: scoped.streams().clone(),
            validate_only: false,
        }
    }
    /// Consume admitted ports with immutable read-only schema validation.
    ///
    /// Every implemented operation validates persisted required features anew;
    /// no successful validation is cached. Compatible native declaration
    /// supersets are accepted. This neither restricts the supplied backend's
    /// implicit initialization nor establishes actor/scope authority: the host
    /// must supply native existing-schema handles and its original fence.
    pub fn new_validate_only(scoped: ScopedStorage) -> Self {
        Self {
            validate_only: true,
            ..Self::new(scoped)
        }
    }

    /// Existing metadata declaration; streams retain their schema1 layout.
    ///
    /// There are no required secondary indexes, TTL or full-text fields.
    /// Native validation accepts compatible supersets, not exact equality.
    pub fn collection_specs() -> Vec<CollectionSpec> {
        vec![CollectionSpec::new(COLLECTION)]
    }

    /// Declare application metadata under explicitly trusted bootstrap authority.
    ///
    /// This does not prepare native internal schemas, bind an actor, migrate
    /// data or enable message writes. Restricted actor handles must refuse.
    ///
    /// # Errors
    /// The native declaration error, preserving its kind and original cause.
    pub async fn prepare_schema(
        documents: &dyn DocumentStore,
    ) -> tinystoragedrivers_core::Result<()> {
        for spec in Self::collection_specs() {
            documents.ensure_collection(&spec).await?;
        }
        Ok(())
    }

    async fn admit(&self) -> ConversationResult<()> {
        if self.validate_only {
            for spec in Self::collection_specs() {
                self.docs.validate_collection_spec(&spec).await?;
            }
        }
        Ok(())
    }

    fn key(id: &str) -> ConversationResult<String> {
        if id.is_empty() || id.contains('\0') || id.len() > 1024 * 1024 {
            return Err(StorageError::invalid_input(
                "thread id must be nonempty, contain no NUL, and fit 1MiB",
            )
            .into());
        }
        Ok(hex::encode(Sha256::digest(id.as_bytes())))
    }
    fn stream(key: &str, incarnation: &str) -> String {
        format!("conversation_messages_v1:{key}:{incarnation}")
    }
    fn decode(row: Versioned<serde_json::Value>) -> ConversationResult<Versioned<ThreadDoc>> {
        let doc: ThreadDoc = serde_json::from_value(row.doc).map_err(|e| {
            StorageError::serialization("invalid conversation metadata").with_source(e)
        })?;
        let stored_key = Self::key(&doc.thread.id).map_err(|error| {
            StorageError::serialization("invalid stored conversation identity").with_source(error)
        })?;
        if doc.schema != 1
            || uuid::Uuid::parse_str(&doc.incarnation).is_err()
            || stored_key != row.id
            || u64::try_from(doc.thread.message_count).ok() != Some(doc.visible_end)
        {
            return Err(StorageError::serialization(
                "conversation metadata identity/schema/prefix mismatch",
            )
            .into());
        }
        Ok(Versioned {
            id: row.id,
            version: row.version,
            doc,
        })
    }
    async fn load(&self, id: &str) -> ConversationResult<Option<Versioned<ThreadDoc>>> {
        self.admit().await?;
        let key = Self::key(id)?;
        self.docs
            .get(COLLECTION, &key)
            .await?
            .map(Self::decode)
            .transpose()
    }
    async fn change(
        &self,
        id: &str,
        operation: impl Fn(Option<&ThreadDoc>) -> ConversationResult<ThreadDoc>,
    ) -> ConversationResult<ConversationThread> {
        let key = Self::key(id)?;
        for _ in 0..RETRIES {
            let current = self.load(id).await?;
            let replacement = operation(current.as_ref().map(|row| &row.doc))?;
            let body = serde_json::to_value(&replacement).map_err(|e| {
                StorageError::serialization("encode conversation metadata").with_source(e)
            })?;
            let precondition = current
                .as_ref()
                .map_or(Precondition::Absent, Versioned::unchanged);
            match self.docs.put(COLLECTION, &key, body, precondition).await {
                Ok(_) => return Ok(replacement.thread),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::conflict("conversation metadata retry bound exhausted").into())
    }
    fn live(current: Option<&ThreadDoc>) -> ConversationResult<ThreadDoc> {
        current
            .filter(|doc| !doc.deleted)
            .cloned()
            .ok_or_else(|| StorageError::not_found("conversation thread not found").into())
    }
    fn ensure(current: Option<&ThreadDoc>, request: &CreateConversationThread) -> ThreadDoc {
        if let Some(current) = current.filter(|doc| !doc.deleted) {
            let mut next = current.clone();
            next.thread.title = request.title.clone();
            if let Some(parent) = &request.parent_thread_id {
                next.thread.parent_thread_id = Some(parent.clone());
            }
            if let Some(labels) = &request.labels {
                next.thread.labels = store::normalize_labels(labels.clone());
            }
            if let Some(personality) = &request.personality_id {
                next.thread.personality_id = Some(personality.clone());
            }
            if let Some(dir) = &request.working_dir {
                next.thread.working_dir = (!dir.is_empty()).then(|| dir.clone());
            }
            next
        } else {
            ThreadDoc {
                schema: 1,
                incarnation: uuid::Uuid::new_v4().to_string(),
                deleted: false,
                visible_end: 0,
                thread: ConversationThread {
                    id: request.id.clone(),
                    title: request.title.clone(),
                    chat_id: None,
                    is_active: true,
                    message_count: 0,
                    last_message_at: request.created_at.clone(),
                    created_at: request.created_at.clone(),
                    parent_thread_id: request.parent_thread_id.clone(),
                    labels: request
                        .labels
                        .clone()
                        .map(store::normalize_labels)
                        .unwrap_or_else(|| store::infer_labels(&request.id)),
                    personality_id: request.personality_id.clone(),
                    working_dir: request.working_dir.clone().filter(|dir| !dir.is_empty()),
                },
            }
        }
    }
}

#[async_trait]
impl ConversationPersistence for DriverConversationStore {
    async fn ensure_thread(
        &self,
        request: CreateConversationThread,
    ) -> ConversationResult<ConversationThread> {
        self.admit().await?;
        self.change(&request.id, |current| Ok(Self::ensure(current, &request)))
            .await
    }
    async fn get_thread(&self, thread_id: &str) -> ConversationResult<Option<ConversationThread>> {
        self.admit().await?;
        Ok(self
            .load(thread_id)
            .await?
            .filter(|row| !row.doc.deleted)
            .map(|row| row.doc.thread))
    }
    async fn list_threads(&self) -> ConversationResult<Vec<ConversationThread>> {
        self.admit().await?;
        let mut rows: Vec<_> = self
            .live_rows()
            .await?
            .into_iter()
            .map(|row| row.doc.thread)
            .collect();
        rows.sort_by(|a, b| {
            timestamp(&b.last_message_at)
                .cmp(&timestamp(&a.last_message_at))
                .then_with(|| timestamp(&b.created_at).cmp(&timestamp(&a.created_at)))
        });
        Ok(rows)
    }
    async fn get_messages(&self, thread_id: &str) -> ConversationResult<Vec<ThreadMessage>> {
        self.admit().await?;
        self.messages(thread_id).await
    }
    async fn update_thread_title(
        &self,
        thread_id: &str,
        title: &str,
        _updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        self.admit().await?;
        self.change(thread_id, |current| {
            let mut doc = Self::live(current)?;
            doc.thread.title = title.into();
            Ok(doc)
        })
        .await
    }
    async fn update_thread_labels(
        &self,
        thread_id: &str,
        labels: Vec<String>,
        _updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        self.admit().await?;
        self.change(thread_id, |current| {
            let mut doc = Self::live(current)?;
            doc.thread.labels = store::normalize_labels(labels.clone());
            Ok(doc)
        })
        .await
    }
    async fn update_thread_working_dir(
        &self,
        thread_id: &str,
        working_dir: Option<String>,
        _updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        self.admit().await?;
        self.change(thread_id, |current| {
            let mut doc = Self::live(current)?;
            doc.thread.working_dir = working_dir
                .as_ref()
                .map(|dir| dir.trim().to_owned())
                .filter(|dir| !dir.is_empty());
            Ok(doc)
        })
        .await
    }
    async fn delete_thread(&self, thread_id: &str, _deleted_at: &str) -> ConversationResult<bool> {
        self.admit().await?;
        let key = Self::key(thread_id)?;
        for _ in 0..RETRIES {
            let Some(mut row) = self.load(thread_id).await? else {
                return Ok(false);
            };
            if row.doc.deleted {
                return Ok(false);
            }
            row.doc.deleted = true;
            let body = serde_json::to_value(&row.doc)
                .map_err(|e| StorageError::serialization("encode tombstone").with_source(e))?;
            match self.docs.put(COLLECTION, &key, body, row.unchanged()).await {
                Ok(_) => return Ok(true),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::conflict("conversation deletion retry bound exhausted").into())
    }
    async fn search_cross_thread_messages(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> ConversationResult<Vec<CrossThreadHit>> {
        self.admit().await?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut index = inverted_index::InvertedIndex::new();
        for row in self.live_rows().await? {
            if exclude_thread_id == Some(row.doc.thread.id.as_str()) {
                continue;
            }
            for message in self.messages(&row.doc.thread.id).await? {
                index.insert(&row.doc.thread.id, message);
            }
        }
        Ok(index.search(query, limit, exclude_thread_id))
    }
    async fn append_message(
        &self,
        thread_id: &str,
        message: ThreadMessage,
    ) -> ConversationResult<ThreadMessage> {
        self.append_native(thread_id, message).await
    }
    async fn update_message(
        &self,
        _thread_id: &str,
        _message_id: &str,
        _patch: ThreadMessagePatch,
    ) -> ConversationResult<ThreadMessage> {
        self.admit().await?;
        Err(ConversationError::Unsupported {
            operation: "update_message",
        })
    }
    async fn delete_messages_from(
        &self,
        _thread_id: &str,
        _message_id: &str,
    ) -> ConversationResult<Option<usize>> {
        self.admit().await?;
        Err(ConversationError::Unsupported {
            operation: "delete_messages_from",
        })
    }
    async fn purge_threads(&self) -> ConversationResult<ConversationPurgeStats> {
        self.admit().await?;
        Err(ConversationError::Unsupported {
            operation: "purge_threads",
        })
    }
}
fn timestamp(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|v| v.timestamp_millis())
        .unwrap_or(i64::MIN)
}
#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "driver_schema_tests.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "driver_append_tests.rs"]
mod append_tests;
