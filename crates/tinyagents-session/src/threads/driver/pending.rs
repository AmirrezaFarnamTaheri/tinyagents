//! A durable intent to append one message; old metadata writers cannot erase it.
//!
//! The document is `conversations.appends` at `Key::of([thread_id, incarnation])`
//! and holds the exact stream row beside the offset it goes to.
use super::*;

pub(super) use super::super::dto::AppendIntent;
pub(super) use super::super::schema::APPENDS;

impl AppendIntent {
    pub fn new(thread: &ThreadDoc, message: &ThreadMessage) -> ConversationResult<Self> {
        let record = MessageDto::from_message(message, to_ms)?;
        Ok(Self {
            token: uuid::Uuid::new_v4().to_string(),
            thread_id: thread.thread.id.clone(),
            incarnation: thread.incarnation.clone(),
            offset: thread.visible_end,
            record: serde_json::to_value(record).map_err(|error| {
                StorageError::serialization("encode conversation append intent").with_source(error)
            })?,
        })
    }
    /// The document id of the intent for `thread_id`'s `incarnation`.
    pub fn key(thread_id: &str, incarnation: &str) -> Result<String, StorageError> {
        Ok(Key::of([thread_id, incarnation])?.as_str().to_owned())
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn record(&self) -> ConversationResult<MessageDto> {
        serde_json::from_value(self.record.clone()).map_err(|error| {
            StorageError::serialization("invalid conversation append record")
                .with_source(error)
                .into()
        })
    }
    pub fn message(&self) -> ConversationResult<ThreadMessage> {
        Ok(self.record()?.into_message()?)
    }
    pub fn value(&self) -> ConversationResult<serde_json::Value> {
        serde_json::to_value(self).map_err(|error| {
            StorageError::serialization("encode conversation append envelope")
                .with_source(error)
                .into()
        })
    }
    pub fn record_value(&self) -> &serde_json::Value {
        &self.record
    }
    pub fn decode(
        row: Versioned<serde_json::Value>,
        thread_id: &str,
        incarnation: &str,
    ) -> ConversationResult<Versioned<Self>> {
        let intent: Self = serde_json::from_value(row.doc).map_err(|error| {
            StorageError::serialization("invalid conversation append envelope").with_source(error)
        })?;
        let record = intent.record()?;
        // A new intent must round-trip exactly through the typed record. Old
        // committed readers remain deliberately compatible, but a pending
        // writer must not publish fields its typed result silently drops.
        let canonical = serde_json::to_value(&record).map_err(|error| {
            StorageError::serialization("encode canonical conversation append record")
                .with_source(error)
        })?;
        if canonical != *intent.record_value() {
            return Err(
                StorageError::serialization("noncanonical conversation append record").into(),
            );
        }
        if uuid::Uuid::parse_str(&intent.token).is_err()
            || intent.thread_id != thread_id
            || intent.incarnation != incarnation
            || row.id != Self::key(thread_id, incarnation)?
        {
            return Err(StorageError::serialization(
                "conversation append identity or schema mismatch",
            )
            .into());
        }
        Ok(Versioned {
            id: row.id,
            version: row.version,
            doc: intent,
            meta: row.meta,
        })
    }
}
