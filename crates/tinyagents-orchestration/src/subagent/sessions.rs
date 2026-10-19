//! Durable identities and storage for reusable subagent sessions.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tinyinference_llm::message::{AssistantMessage, ContentBlock, Message};
use tinyinference_llm::tool::ToolCall;
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, DocumentStoreExt, ErrorKind, Precondition, Query,
};
use tokio::sync::OnceCell;

use super::{SubagentError, SubagentTaskKey};

const COLLECTION: &str = "subagent_sessions";
const CAS_ATTEMPTS: usize = 32;

fn session_id_for_selector(selector: &SubagentSessionSelector) -> String {
    let encoded =
        serde_json::to_string(selector).expect("serializing a session selector is infallible");
    format!(
        "subsess-{}",
        tinyagents_harness::driver_ids::hashed_id(&[&encoded])
    )
}

/// Durable state of a reusable subagent session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableSubagentStatus {
    Running,
    Idle,
    AwaitingUser,
    Failed,
    Closed,
}

impl DurableSubagentStatus {
    /// Whether a session may be selected for reuse.
    pub fn reusable(self) -> bool {
        matches!(self, Self::Running | Self::Idle | Self::AwaitingUser)
    }
}

/// Stable attributes used to select an existing reusable session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentSessionSelector {
    pub parent_session: String,
    pub parent_thread_id: Option<String>,
    pub agent_id: String,
    pub model: Option<String>,
    pub sandbox_mode: String,
    pub action_root: Option<String>,
    pub task_key: String,
}

/// Durable reusable conversation with one subagent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableSubagentSession {
    pub subagent_session_id: String,
    pub parent_session: String,
    pub parent_thread_id: Option<String>,
    pub worker_thread_id: Option<String>,
    pub agent_id: String,
    pub display_name: Option<String>,
    pub model: Option<String>,
    pub sandbox_mode: String,
    pub action_root: Option<String>,
    pub task_key: String,
    /// Older product-owned rows can be listed before a lifecycle identity is
    /// recoverable. Hosts may populate this from a validated checkpoint; a
    /// missing key never authorizes continuation by task id alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_key: Option<SubagentTaskKey>,
    pub task_title: String,
    pub current_task_id: Option<String>,
    pub status: DurableSubagentStatus,
    pub reusable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_session_history"
    )]
    pub latest_history: Option<Vec<Message>>,
    pub latest_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_used_at: String,
}

#[derive(Deserialize)]
struct LegacyTranscriptMessage {
    role: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    tool_calls: Vec<LegacyTranscriptToolCall>,
    #[serde(default)]
    tool_call_id: Option<String>,
}

#[derive(Deserialize)]
struct LegacyTranscriptToolCall {
    id: String,
    name: String,
    #[serde(default)]
    arguments: String,
}

fn deserialize_session_history<'de, D>(deserializer: D) -> Result<Option<Vec<Message>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(rows) = Option::<Vec<Value>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    rows.into_iter()
        .map(|row| {
            if let Ok(message) = serde_json::from_value::<Message>(row.clone()) {
                return Ok(message);
            }
            let legacy: LegacyTranscriptMessage =
                serde_json::from_value(row).map_err(serde::de::Error::custom)?;
            let message = match legacy.role.as_str() {
                "system" => Message::system(legacy.content),
                "user" => Message::user(legacy.content),
                "tool" => Message::tool(
                    legacy.tool_call_id.ok_or_else(|| {
                        serde::de::Error::custom("legacy tool row has no call id")
                    })?,
                    legacy.content,
                ),
                "assistant" => Message::Assistant(AssistantMessage {
                    id: None,
                    content: vec![ContentBlock::Text(legacy.content)],
                    tool_calls: legacy
                        .tool_calls
                        .into_iter()
                        .map(|call| match serde_json::from_str(&call.arguments) {
                            Ok(arguments) => ToolCall::new(call.id, call.name, arguments),
                            Err(error) => ToolCall::invalid(
                                call.id,
                                call.name,
                                call.arguments,
                                error.to_string(),
                            ),
                        })
                        .collect(),
                    usage: None,
                    origin: None,
                }),
                _ => return Err(serde::de::Error::custom("unknown legacy transcript role")),
            };
            Ok(message)
        })
        .collect::<Result<Vec<_>, D::Error>>()
        .map(Some)
}

impl DurableSubagentSession {
    /// Whether this session is reusable for `selector`.
    pub fn matches_selector(&self, selector: &SubagentSessionSelector) -> bool {
        self.reusable && self.status.reusable() && self.same_selector(selector)
    }

    fn same_selector(&self, selector: &SubagentSessionSelector) -> bool {
        self.parent_session == selector.parent_session
            && self.parent_thread_id == selector.parent_thread_id
            && self.agent_id == selector.agent_id
            && self.model == selector.model
            && self.sandbox_mode == selector.sandbox_mode
            && self.action_root == selector.action_root
            && self.task_key == selector.task_key
    }
}

/// Minimal session fields needed to start or reuse a child session.
#[derive(Debug, Clone)]
pub struct SubagentSessionUpsert {
    pub selector: SubagentSessionSelector,
    pub lifecycle_key: SubagentTaskKey,
    pub display_name: Option<String>,
    pub task_title: String,
    pub worker_thread_id: Option<String>,
    pub task_id: String,
    pub force_fresh: bool,
}

/// User-visible summary that omits transcript history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableSubagentSessionSummary {
    pub subagent_session_id: String,
    pub parent_thread_id: Option<String>,
    pub worker_thread_id: Option<String>,
    pub agent_id: String,
    pub display_name: Option<String>,
    pub model: Option<String>,
    pub sandbox_mode: String,
    pub action_root: Option<String>,
    pub task_key: String,
    pub task_title: String,
    pub current_task_id: Option<String>,
    pub status: DurableSubagentStatus,
    pub reusable: bool,
    pub latest_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub last_used_at: String,
}

impl From<&DurableSubagentSession> for DurableSubagentSessionSummary {
    fn from(session: &DurableSubagentSession) -> Self {
        Self {
            subagent_session_id: session.subagent_session_id.clone(),
            parent_thread_id: session.parent_thread_id.clone(),
            worker_thread_id: session.worker_thread_id.clone(),
            agent_id: session.agent_id.clone(),
            display_name: session.display_name.clone(),
            model: session.model.clone(),
            sandbox_mode: session.sandbox_mode.clone(),
            action_root: session.action_root.clone(),
            task_key: session.task_key.clone(),
            task_title: session.task_title.clone(),
            current_task_id: session.current_task_id.clone(),
            status: session.status,
            reusable: session.reusable,
            latest_error: session.latest_error.clone(),
            created_at: session.created_at.clone(),
            updated_at: session.updated_at.clone(),
            last_used_at: session.last_used_at.clone(),
        }
    }
}

/// Outcome of deciding whether a new request reused an earlier session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReuseDecision {
    ReusedRunning,
    ReusedIdle,
    SpawnedNew,
    ForcedFresh,
}

impl ReuseDecision {
    /// Stable wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReusedRunning => "reused_running",
            Self::ReusedIdle => "reused_idle",
            Self::SpawnedNew => "spawned_new",
            Self::ForcedFresh => "forced_fresh",
        }
    }
}

/// Storage contract for reusable subagent sessions.
#[async_trait]
pub trait SubagentSessionStore: Send + Sync {
    /// Lists sessions owned by the supplied parent session.
    async fn list_for_parent(
        &self,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<Vec<DurableSubagentSession>, SubagentError>;
    /// Reads a session by its durable id.
    async fn get(&self, id: &str) -> Result<Option<DurableSubagentSession>, SubagentError>;
    /// Selects a reusable session or creates one, then marks it running.
    async fn upsert_running(
        &self,
        upsert: SubagentSessionUpsert,
    ) -> Result<(DurableSubagentSession, ReuseDecision), SubagentError>;
    /// Closes a session so it cannot be reused.
    async fn close(&self, id: &str) -> Result<bool, SubagentError>;
    /// Closes only the current task lifecycle. A stale owner cannot close a
    /// replacement that reused the same durable session id.
    async fn close_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<bool, SubagentError> {
        let _ = (id, task_id, lifecycle_key, parent_session, parent_thread_id);
        Err(SubagentError::Persistence(
            "session store does not support atomic lifecycle close".into(),
        ))
    }
    /// Records the latest result and reusable state after a completed turn.
    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError>;
    /// Owner- and lifecycle-checked completion for an owner-bound session.
    // Each argument is part of the atomic ownership predicate or the new
    // session result; combining them would hide which values are checked.
    #[allow(clippy::too_many_arguments)]
    async fn mark_finished_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        let _ = (
            id,
            task_id,
            lifecycle_key,
            parent_session,
            parent_thread_id,
            status,
            history,
        );
        Err(SubagentError::Persistence(
            "session store does not support atomic owner-checked completion".into(),
        ))
    }
    /// Records a failed turn while preserving the durable session for listing.
    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> Result<bool, SubagentError>;
    /// Owner- and lifecycle-checked failure for an owner-bound session.
    async fn mark_failed_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        error: String,
    ) -> Result<bool, SubagentError> {
        let _ = (
            id,
            task_id,
            lifecycle_key,
            parent_session,
            parent_thread_id,
            error,
        );
        Err(SubagentError::Persistence(
            "session store does not support atomic owner-checked failure".into(),
        ))
    }
}

/// Owner-bound view of a session store for one exact parent session/thread.
/// Missing thread identity is exact (threadless), never a wildcard.
#[derive(Clone)]
pub struct ScopedSubagentSessionStore<S> {
    inner: Arc<S>,
    parent_session: String,
    parent_thread_id: Option<String>,
    close_callback: Option<Arc<dyn Fn(&str, &str, &SubagentTaskKey) + Send + Sync>>,
}

impl<S> ScopedSubagentSessionStore<S> {
    /// Binds the underlying store to one parent owner.
    pub fn new(
        inner: Arc<S>,
        parent_session: impl Into<String>,
        parent_thread_id: Option<String>,
    ) -> Self {
        Self {
            inner,
            parent_session: parent_session.into(),
            parent_thread_id,
            close_callback: None,
        }
    }

    /// Binds a host cancellation callback to successful lifecycle closes.
    /// The callback receives the closed session id, task id and lifecycle key
    /// only after the store's conditional close wins.
    pub fn with_close_callback(
        inner: Arc<S>,
        parent_session: impl Into<String>,
        parent_thread_id: Option<String>,
        callback: impl Fn(&str, &str, &SubagentTaskKey) + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            parent_session: parent_session.into(),
            parent_thread_id,
            close_callback: Some(Arc::new(callback)),
        }
    }

    fn owns(&self, session: &DurableSubagentSession) -> bool {
        session.parent_session == self.parent_session
            && session.parent_thread_id == self.parent_thread_id
    }

    fn owns_selector(&self, selector: &SubagentSessionSelector) -> bool {
        selector.parent_session == self.parent_session
            && selector.parent_thread_id == self.parent_thread_id
    }

    fn owner_error() -> SubagentError {
        SubagentError::Persistence("subagent session is outside the bound owner scope".into())
    }
}

#[async_trait]
impl<S: SubagentSessionStore + 'static> SubagentSessionStore for ScopedSubagentSessionStore<S> {
    async fn list_for_parent(
        &self,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<Vec<DurableSubagentSession>, SubagentError> {
        if parent_session != self.parent_session
            || parent_thread_id != self.parent_thread_id.as_deref()
        {
            return Err(Self::owner_error());
        }
        Ok(self
            .inner
            .list_for_parent(&self.parent_session, self.parent_thread_id.as_deref())
            .await?
            .into_iter()
            .filter(|session| self.owns(session))
            .collect())
    }

    async fn get(&self, id: &str) -> Result<Option<DurableSubagentSession>, SubagentError> {
        Ok(self
            .inner
            .get(id)
            .await?
            .filter(|session| self.owns(session)))
    }

    async fn upsert_running(
        &self,
        upsert: SubagentSessionUpsert,
    ) -> Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        if !self.owns_selector(&upsert.selector) {
            return Err(Self::owner_error());
        }
        let (session, decision) = self.inner.upsert_running(upsert).await?;
        if !self.owns(&session) {
            return Err(Self::owner_error());
        }
        Ok((session, decision))
    }

    async fn close(&self, id: &str) -> Result<bool, SubagentError> {
        let Some(session) = self.get(id).await? else {
            return Ok(false);
        };
        let Some(task_id) = session.current_task_id.as_deref() else {
            return Ok(false);
        };
        let Some(lifecycle_key) = session.lifecycle_key.as_ref() else {
            return Ok(false);
        };
        let closed = self
            .inner
            .close_if_current(
                id,
                task_id,
                lifecycle_key,
                &self.parent_session,
                self.parent_thread_id.as_deref(),
            )
            .await?;
        if closed && let Some(callback) = &self.close_callback {
            callback(id, task_id, lifecycle_key);
        }
        Ok(closed)
    }

    async fn close_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<bool, SubagentError> {
        if parent_session != self.parent_session
            || parent_thread_id != self.parent_thread_id.as_deref()
        {
            return Ok(false);
        }
        let closed = self
            .inner
            .close_if_current(
            id,
            task_id,
            lifecycle_key,
            &self.parent_session,
            self.parent_thread_id.as_deref(),
        )
        .await?;
        if closed && let Some(callback) = &self.close_callback {
            callback(id, task_id, lifecycle_key);
        }
        Ok(closed)
    }

    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        let Some(session) = self.get(id).await? else {
            return Ok(false);
        };
        let Some(lifecycle_key) = session.lifecycle_key.as_ref() else {
            return Ok(false);
        };
        self.inner
            .mark_finished_if_current(
                id,
                task_id,
                lifecycle_key,
                &self.parent_session,
                self.parent_thread_id.as_deref(),
                status,
                history,
            )
            .await
    }

    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> Result<bool, SubagentError> {
        let Some(session) = self.get(id).await? else {
            return Ok(false);
        };
        let Some(lifecycle_key) = session.lifecycle_key.as_ref() else {
            return Ok(false);
        };
        self.inner
            .mark_failed_if_current(
                id,
                task_id,
                lifecycle_key,
                &self.parent_session,
                self.parent_thread_id.as_deref(),
                error,
            )
            .await
    }
}

/// A `SubagentSessionStore` backed by the host's `tinystoragedrivers` document
/// port. The document handle is already scoped by the host.
#[derive(Clone)]
pub struct DriverSubagentSessionStore {
    docs: Arc<dyn DocumentStore>,
    bridge: Blocking,
    declared: Arc<OnceCell<()>>,
}

impl DriverSubagentSessionStore {
    /// Creates a store with its own blocking bridge.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Result<Self, SubagentError> {
        let bridge = Blocking::new()
            .map_err(|error| SubagentError::Persistence(format!("session bridge: {error}")))?;
        Ok(Self::with_bridge(docs, bridge))
    }

    /// Creates a store that shares a host-provided blocking bridge.
    pub fn with_bridge(docs: Arc<dyn DocumentStore>, bridge: Blocking) -> Self {
        Self {
            docs,
            bridge,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// Collection declaration required by this store.
    pub fn collection_specs() -> Vec<CollectionSpec> {
        vec![CollectionSpec::new(COLLECTION)]
    }

    fn run<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, SubagentError>> + Send + 'static,
    ) -> Result<T, SubagentError> {
        self.bridge
            .run(future)
            .map_err(|error| SubagentError::Persistence(format!("session bridge: {error}")))?
    }

    async fn declare(&self) -> Result<(), SubagentError> {
        self.declared
            .get_or_try_init(|| async {
                for spec in Self::collection_specs() {
                    self.docs.ensure_collection(&spec).await.map_err(|error| {
                        SubagentError::Persistence(format!("session store: {error}"))
                    })?;
                }
                Ok::<(), SubagentError>(())
            })
            .await
            .map(|_| ())
    }

    async fn load_all(&self) -> Result<Vec<DurableSubagentSession>, SubagentError> {
        self.declare().await?;
        self.docs
            .query_all(COLLECTION, &Query::all())
            .await
            .map_err(|error| SubagentError::Persistence(format!("session store: {error}")))?
            .into_iter()
            .map(|document| {
                serde_json::from_value(document.doc)
                    .map_err(|error| SubagentError::Persistence(format!("session decode: {error}")))
            })
            .collect()
    }

    async fn mutate(
        &self,
        id: &str,
        mut apply: impl FnMut(&mut DurableSubagentSession) -> bool,
    ) -> Result<bool, SubagentError> {
        self.declare().await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(found) =
                self.docs.get(COLLECTION, id).await.map_err(|error| {
                    SubagentError::Persistence(format!("session store: {error}"))
                })?
            else {
                return Ok(false);
            };
            let precondition = found.unchanged();
            let mut session: DurableSubagentSession = serde_json::from_value(found.doc)
                .map_err(|error| SubagentError::Persistence(format!("session decode: {error}")))?;
            if !apply(&mut session) {
                return Ok(false);
            }
            let doc = serde_json::to_value(session)
                .map_err(|error| SubagentError::Persistence(format!("session encode: {error}")))?;
            match self.docs.put(COLLECTION, id, doc, precondition).await {
                Ok(_) => return Ok(true),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => {
                    return Err(SubagentError::Persistence(format!(
                        "session store: {error}"
                    )));
                }
            }
        }
        Err(SubagentError::Persistence(
            "session store: contention budget exhausted".into(),
        ))
    }
}

#[async_trait]
impl SubagentSessionStore for DriverSubagentSessionStore {
    async fn list_for_parent(
        &self,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<Vec<DurableSubagentSession>, SubagentError> {
        let this = self.clone();
        let parent_session = parent_session.to_owned();
        let parent_thread_id = parent_thread_id.map(str::to_owned);
        self.run(async move {
            let mut rows: Vec<_> = this
                .load_all()
                .await?
                .into_iter()
                .filter(|session| {
                    session.parent_session == parent_session
                        && parent_thread_id.as_deref().is_none_or(|thread| {
                            session.parent_thread_id.as_deref() == Some(thread)
                        })
                })
                .collect();
            rows.sort_by(|a, b| b.last_used_at.cmp(&a.last_used_at));
            Ok(rows)
        })
    }

    async fn get(&self, id: &str) -> Result<Option<DurableSubagentSession>, SubagentError> {
        let this = self.clone();
        let id = id.to_owned();
        self.run(async move {
            this.declare().await?;
            let found =
                this.docs.get(COLLECTION, &id).await.map_err(|error| {
                    SubagentError::Persistence(format!("session store: {error}"))
                })?;
            found
                .map(|document| {
                    serde_json::from_value(document.doc).map_err(|error| {
                        SubagentError::Persistence(format!("session decode: {error}"))
                    })
                })
                .transpose()
        })
    }

    async fn upsert_running(
        &self,
        upsert: SubagentSessionUpsert,
    ) -> Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        let this = self.clone();
        self.run(async move {
            let all = this.load_all().await?;
            let prior_selector = all
                .iter()
                .any(|session| session.same_selector(&upsert.selector));
            let reuse = if upsert.force_fresh {
                None
            } else {
                find_reusable_session(all, &upsert.selector)
            };
            let selected_owner = reuse.as_ref().map(|session| {
                (
                    session.current_task_id.clone(),
                    session.lifecycle_key.clone(),
                )
            });
            let decision = reuse_decision(reuse.as_ref(), upsert.force_fresh);
            let now = chrono::Utc::now().to_rfc3339();
            let mut session = reuse.clone().unwrap_or_else(|| DurableSubagentSession {
                subagent_session_id: if upsert.force_fresh || prior_selector {
                    format!("subsess-{}", uuid::Uuid::new_v4())
                } else {
                    session_id_for_selector(&upsert.selector)
                },
                parent_session: upsert.selector.parent_session.clone(),
                parent_thread_id: upsert.selector.parent_thread_id.clone(),
                worker_thread_id: upsert.worker_thread_id.clone(),
                agent_id: upsert.selector.agent_id.clone(),
                display_name: upsert.display_name.clone(),
                model: upsert.selector.model.clone(),
                sandbox_mode: upsert.selector.sandbox_mode.clone(),
                action_root: upsert.selector.action_root.clone(),
                task_key: upsert.selector.task_key.clone(),
                lifecycle_key: Some(upsert.lifecycle_key.clone()),
                task_title: upsert.task_title.clone(),
                current_task_id: None,
                status: DurableSubagentStatus::Running,
                reusable: true,
                latest_history: None,
                latest_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
                last_used_at: now.clone(),
            });
            session.lifecycle_key = Some(upsert.lifecycle_key);
            session.worker_thread_id = upsert.worker_thread_id.or(session.worker_thread_id);
            session.display_name = upsert.display_name.or(session.display_name);
            session.task_title = upsert.task_title;
            session.current_task_id = Some(upsert.task_id);
            session.status = DurableSubagentStatus::Running;
            session.reusable = true;
            session.latest_error = None;
            session.updated_at = now.clone();
            session.last_used_at = now;
            let session_id = session.subagent_session_id.clone();
            if reuse.is_some() {
                let update = this
                    .mutate(&session_id, |current| {
                        if !current.status.reusable()
                            || !current.reusable
                            || selected_owner.as_ref()
                                != Some(&(
                                    current.current_task_id.clone(),
                                    current.lifecycle_key.clone(),
                                ))
                        {
                            return false;
                        }
                        current.lifecycle_key = session.lifecycle_key.clone();
                        current.worker_thread_id = session.worker_thread_id.clone();
                        current.display_name = session.display_name.clone();
                        current.task_title = session.task_title.clone();
                        current.current_task_id = session.current_task_id.clone();
                        current.status = DurableSubagentStatus::Running;
                        current.reusable = true;
                        current.latest_error = None;
                        current.updated_at = session.updated_at.clone();
                        current.last_used_at = session.last_used_at.clone();
                        true
                    })
                    .await?;
                if !update {
                    return Err(SubagentError::Persistence(
                        "reusable session was concurrently closed or reassigned".into(),
                    ));
                }
            } else {
                let doc = serde_json::to_value(&session).map_err(|error| {
                    SubagentError::Persistence(format!("session encode: {error}"))
                })?;
                match this
                    .docs
                    .put(COLLECTION, &session_id, doc, Precondition::Absent)
                    .await
                {
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::Conflict && !upsert.force_fresh => {
                        let reused = this
                            .mutate(&session_id, |current| {
                                if !current.matches_selector(&upsert.selector) {
                                    return false;
                                }
                                current.lifecycle_key = session.lifecycle_key.clone();
                                current.worker_thread_id = session
                                    .worker_thread_id
                                    .clone()
                                    .or(current.worker_thread_id.clone());
                                current.current_task_id = session.current_task_id.clone();
                                current.task_title = session.task_title.clone();
                                current.status = DurableSubagentStatus::Running;
                                current.reusable = true;
                                current.updated_at = session.updated_at.clone();
                                current.last_used_at = session.last_used_at.clone();
                                true
                            })
                            .await?;
                        if !reused {
                            return Err(SubagentError::Persistence(
                                "matching session was concurrently closed".into(),
                            ));
                        }
                    }
                    Err(error) => {
                        return Err(SubagentError::Persistence(format!(
                            "session store: {error}"
                        )));
                    }
                }
            }
            let current = this
                .docs
                .get(COLLECTION, &session_id)
                .await
                .map_err(|error| SubagentError::Persistence(format!("session store: {error}")))?
                .ok_or_else(|| SubagentError::Persistence("claimed session disappeared".into()))?;
            let current = serde_json::from_value(current.doc)
                .map_err(|error| SubagentError::Persistence(format!("session decode: {error}")))?;
            Ok((current, decision))
        })
    }

    async fn close(&self, id: &str) -> Result<bool, SubagentError> {
        self.mutate(id, |session| {
            session.status = DurableSubagentStatus::Closed;
            session.reusable = false;
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }

    async fn mark_finished_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        let task_id = task_id.to_owned();
        let lifecycle_key = lifecycle_key.clone();
        let parent_session = parent_session.to_owned();
        let parent_thread_id = parent_thread_id.map(str::to_owned);
        self.mutate(id, move |session| {
            if session.status == DurableSubagentStatus::Closed
                || session.current_task_id.as_deref() != Some(task_id.as_str())
                || session.lifecycle_key.as_ref() != Some(&lifecycle_key)
                || session.parent_session != parent_session
                || session.parent_thread_id != parent_thread_id
            {
                return false;
            }
            session.status = status;
            session.reusable = status.reusable();
            session.latest_history = Some(history.clone());
            session.latest_error = None;
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }

    async fn close_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<bool, SubagentError> {
        let id = id.to_owned();
        let task_id = task_id.to_owned();
        let lifecycle_key = lifecycle_key.clone();
        let parent_session = parent_session.to_owned();
        let parent_thread_id = parent_thread_id.map(str::to_owned);
        self.mutate(&id, move |session| {
            if session.current_task_id.as_deref() != Some(task_id.as_str())
                || session.lifecycle_key.as_ref() != Some(&lifecycle_key)
                || session.parent_session != parent_session
                || session.parent_thread_id != parent_thread_id
            {
                return false;
            }
            session.status = DurableSubagentStatus::Closed;
            session.reusable = false;
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }

    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        self.mutate(id, |session| {
            if session.status == DurableSubagentStatus::Closed
                || session.current_task_id.as_deref() != Some(task_id)
            {
                return false;
            }
            session.status = status;
            session.reusable = status.reusable();
            session.latest_history = Some(history.clone());
            session.latest_error = None;
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }

    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> Result<bool, SubagentError> {
        self.mutate(id, |session| {
            if session.status == DurableSubagentStatus::Closed
                || session.current_task_id.as_deref() != Some(task_id)
            {
                return false;
            }
            session.status = DurableSubagentStatus::Failed;
            session.reusable = false;
            session.latest_error = Some(error.clone());
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }

    async fn mark_failed_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        error: String,
    ) -> Result<bool, SubagentError> {
        let task_id = task_id.to_owned();
        let lifecycle_key = lifecycle_key.clone();
        let parent_session = parent_session.to_owned();
        let parent_thread_id = parent_thread_id.map(str::to_owned);
        self.mutate(id, move |session| {
            if session.status == DurableSubagentStatus::Closed
                || session.current_task_id.as_deref() != Some(task_id.as_str())
                || session.lifecycle_key.as_ref() != Some(&lifecycle_key)
                || session.parent_session != parent_session
                || session.parent_thread_id != parent_thread_id
            {
                return false;
            }
            session.status = DurableSubagentStatus::Failed;
            session.reusable = false;
            session.latest_error = Some(error.clone());
            session.updated_at = chrono::Utc::now().to_rfc3339();
            session.last_used_at = session.updated_at.clone();
            true
        })
        .await
    }
}

/// Selects the most recently used reusable session matching `selector`.
pub fn find_reusable_session(
    sessions: impl IntoIterator<Item = DurableSubagentSession>,
    selector: &SubagentSessionSelector,
) -> Option<DurableSubagentSession> {
    sessions
        .into_iter()
        .filter(|session| session.matches_selector(selector))
        .max_by(|a, b| a.last_used_at.cmp(&b.last_used_at))
}

/// Classifies the selection result for telemetry or a tool response.
pub fn reuse_decision(reuse: Option<&DurableSubagentSession>, force_fresh: bool) -> ReuseDecision {
    if force_fresh {
        return ReuseDecision::ForcedFresh;
    }
    match reuse.map(|session| session.status) {
        Some(DurableSubagentStatus::Running) => ReuseDecision::ReusedRunning,
        Some(_) => ReuseDecision::ReusedIdle,
        None => ReuseDecision::SpawnedNew,
    }
}

/// Normalizes a user-authored task key into a bounded stable identifier.
pub fn normalize_task_key(input: &str) -> String {
    let mut output = String::new();
    let mut dash = false;
    for ch in input.trim().to_lowercase().chars() {
        if ch.is_alphanumeric() {
            output.push(ch);
            dash = false;
        } else if !dash && !output.is_empty() {
            output.push('-');
            dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        return if input.trim().is_empty() {
            "untitled-task".into()
        } else {
            format!(
                "task-{}",
                tinyagents_harness::driver_ids::hashed_id(&[input])
            )
        };
    }
    if output.len() > 96 {
        while output.len() > 72 {
            output.pop();
        }
        while output.ends_with('-') {
            output.pop();
        }
        output.push('-');
        output.push_str(&tinyagents_harness::driver_ids::hashed_id(&[input])[..16]);
    }
    output
}

/// Creates a concise display title from a delegated prompt.
pub fn task_title_from_prompt(prompt: &str) -> String {
    let collapsed = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= 80 {
        collapsed
    } else {
        let mut title: String = collapsed.chars().take(77).collect();
        title.push_str("...");
        title
    }
}

#[cfg(test)]
#[path = "sessions_tests.rs"]
mod tests;
