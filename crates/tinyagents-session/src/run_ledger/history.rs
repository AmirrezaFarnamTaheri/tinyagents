//! Object-safe contract for the classic session history: sessions, messages,
//! tool calls and the full-text index over them.
//!
//! The history shares a handle with the run ledger so retention
//! ([`super::RunLedgerStore::apply_retention`], `trim_session_messages`,
//! `reindex_fts`) and the writers below see one set of records. Message and
//! tool-call ids are dense positive integers allocated by the store, exactly as
//! the SQLite rowids they replace; an importer may supply explicit ids.

use async_trait::async_trait;

use super::error::LedgerResult as Result;
use crate::types::{
    SessionMessage, SessionRecord, SessionSearchParams, SessionSearchResult, SessionStatus,
    SessionToolCall,
};

/// Cap on the bytes of tool output persisted per call; longer output is
/// truncated on a character boundary with a marker appended.
pub const MAX_TOOL_OUTPUT_BYTES: usize = 32 * 1024;

/// Fields that open a session (`status = running`).
#[derive(Debug, Clone, Default)]
pub struct SessionStart {
    pub id: String,
    pub agent_definition_id: String,
    pub agent_definition_name: String,
    pub session_key: String,
    pub parent_session_id: Option<String>,
    pub thread_id: Option<String>,
    pub source_channel: Option<String>,
    pub model: Option<String>,
    pub transcript_path: Option<String>,
}

/// Terminal fields recorded when a session ends.
#[derive(Debug, Clone)]
pub struct SessionEnd {
    pub status: SessionStatus,
    pub turn_count: u32,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cost_usd: f64,
}

/// A message to append to a session.
#[derive(Debug, Clone, Default)]
pub struct NewSessionMessage {
    pub session_id: String,
    pub role: String,
    pub content: String,
    /// Hidden model reasoning; blank values are dropped.
    pub reasoning_content: Option<String>,
    pub model: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cost_usd: Option<f64>,
}

/// A tool call to append to a session.
#[derive(Debug, Clone, Default)]
pub struct NewSessionToolCall {
    pub session_id: String,
    pub message_id: Option<i64>,
    pub tool_name: String,
    pub tool_input: Option<String>,
    /// Bounded to [`MAX_TOOL_OUTPUT_BYTES`] on write.
    pub tool_output: Option<String>,
    pub status: String,
    pub duration_ms: Option<i64>,
}

/// Bound storage for the classic session history.
#[async_trait]
pub trait SessionHistoryStore: Send + Sync {
    /// Create a `running` session and index it. A duplicate id is a conflict.
    async fn record_session_start(&self, start: SessionStart) -> Result<SessionRecord>;

    /// Mark a session terminal and return the updated row; a missing session is an error.
    async fn record_session_end(&self, id: &str, end: SessionEnd) -> Result<SessionRecord>;

    /// Append a message and its index entry in one atomic commit; returns its id.
    async fn record_message(&self, message: NewSessionMessage) -> Result<i64>;

    /// Append a tool call (output bounded) and its index entry; returns its id.
    async fn record_tool_call(&self, call: NewSessionToolCall) -> Result<i64>;

    /// One session by id.
    async fn get_session(&self, id: &str) -> Result<Option<SessionRecord>>;

    /// Sessions newest first with optional status/parent filters; `limit` is capped at 500.
    async fn list_sessions(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
        status: Option<&str>,
        parent_id: Option<&str>,
    ) -> Result<SessionSearchResult>;

    /// Full-text plus structured search, newest first; `limit` is capped at 500.
    async fn search_sessions(&self, params: &SessionSearchParams) -> Result<SessionSearchResult>;

    /// A session's messages in id order; `limit` is capped at 1000.
    async fn list_messages(
        &self,
        session_id: &str,
        limit: Option<u32>,
    ) -> Result<Vec<SessionMessage>>;

    /// A session's tool calls in id order; `limit` is capped at 1000.
    async fn list_tool_calls(
        &self,
        session_id: &str,
        limit: Option<u32>,
    ) -> Result<Vec<SessionToolCall>>;

    /// Direct child sessions, oldest first.
    async fn list_children(&self, session_id: &str) -> Result<Vec<SessionRecord>>;

    /// Mark every `running` session `interrupted`; returns how many changed.
    async fn mark_interrupted(&self) -> Result<usize>;
}
