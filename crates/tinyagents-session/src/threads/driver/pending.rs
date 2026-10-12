//! Durable all-string intent; old schema1 metadata writers cannot erase it.
use super::*;

pub(super) const APPENDS: &str = "conversation_appends_v1";

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AppendIntent {
    pub schema: String,
    pub token: String,
    pub thread_id: String,
    pub incarnation: String,
    pub offset: String,
    pub record: String,
}
impl AppendIntent {
    pub fn new(thread: &ThreadDoc, message: ThreadMessage) -> ConversationResult<Self> {
        let record = MessageRecord {
            schema: 1,
            incarnation: thread.incarnation.clone(),
            message,
        };
        Ok(Self {
            schema: "1".into(),
            token: uuid::Uuid::new_v4().to_string(),
            thread_id: thread.thread.id.clone(),
            incarnation: thread.incarnation.clone(),
            offset: thread.visible_end.to_string(),
            record: serde_json::to_string(&record).map_err(|error| {
                StorageError::serialization("encode conversation append intent").with_source(error)
            })?,
        })
    }
    pub fn key(thread_key: &str, incarnation: &str) -> String {
        format!("{thread_key}:{incarnation}")
    }
    pub fn offset(&self) -> ConversationResult<u64> {
        self.offset.parse().map_err(|error| {
            StorageError::serialization("invalid conversation append offset")
                .with_source(error)
                .into()
        })
    }
    pub fn record(&self) -> ConversationResult<MessageRecord> {
        serde_json::from_str(&self.record).map_err(|error| {
            StorageError::serialization("invalid conversation append record")
                .with_source(error)
                .into()
        })
    }
    pub fn value(&self) -> ConversationResult<serde_json::Value> {
        serde_json::to_value(self).map_err(|error| {
            StorageError::serialization("encode conversation append envelope")
                .with_source(error)
                .into()
        })
    }
    pub fn record_value(&self) -> ConversationResult<serde_json::Value> {
        serde_json::from_str(&self.record).map_err(|error| {
            StorageError::serialization("invalid conversation append record")
                .with_source(error)
                .into()
        })
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
        // New intents must round-trip exactly through the unchanged schema1
        // record. Old committed readers remain deliberately compatible, but a
        // pending writer must not publish fields its typed result silently drops.
        let canonical = serde_json::to_value(&record).map_err(|error| {
            StorageError::serialization("encode canonical conversation append record")
                .with_source(error)
        })?;
        if canonical != intent.record_value()? {
            return Err(
                StorageError::serialization("noncanonical conversation append record").into(),
            );
        }
        let offset = intent.offset()?;
        if intent.schema != "1"
            || uuid::Uuid::parse_str(&intent.token).is_err()
            || intent.thread_id != thread_id
            || intent.incarnation != incarnation
            || record.schema != 1
            || record.incarnation != incarnation
            || intent.offset != offset.to_string()
            || row.id != Self::key(&DriverConversationStore::key(thread_id)?, incarnation)
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
        })
    }
}
