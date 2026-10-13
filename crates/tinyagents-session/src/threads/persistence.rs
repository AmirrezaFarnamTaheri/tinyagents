//! Async conversation persistence supplied by an admitted host/provider.
//!
//! These handles do not resolve identity, paths or ambient backends. The classic
//! file store remains compatible; unsupported authoritative operations refuse.

use super::{
    ConversationPurgeStats, ConversationStore, ConversationThread, CreateConversationThread,
    CrossThreadHit, ThreadMessage, ThreadMessagePatch,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::path::PathBuf;

/// Result of an explicitly selected conversation store.
pub type ConversationResult<T> = Result<T, ConversationError>;

/// Conversation failures retain implementation identity until the host wire boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConversationError {
    /// The unchanged classic JSONL API reports textual errors.
    #[error("{0}")]
    Legacy(String),
    /// An authoritative operation has no safe implementation in this store.
    #[error("conversation operation {operation} is unsupported")]
    Unsupported { operation: &'static str },
    /// A blocking classic operation could not complete.
    #[error(transparent)]
    Worker(#[from] tokio::task::JoinError),
    /// Native capability, conflict, fencing and backend detail remains available.
    #[error("{0}")]
    Driver(#[source] Box<tinystoragedrivers_core::StorageError>),
    /// A native write acknowledgment could not be resolved to this exact attempt.
    ///
    /// The original native failure remains the source. Ordinary-ID append is
    /// not silently retried, since repeated UUID calls preserve classic semantics.
    #[error("conversation operation {operation} has an unresolved acknowledgment")]
    Uncertain {
        /// The operation whose exact outcome could not be established.
        operation: &'static str,
        /// The original native write failure, retained without textual flattening.
        #[source]
        source: Box<tinystoragedrivers_core::StorageError>,
    },
}

impl From<tinystoragedrivers_core::StorageError> for ConversationError {
    fn from(error: tinystoragedrivers_core::StorageError) -> Self {
        Self::Driver(Box::new(error))
    }
}

/// Lease request. Scope/generation are bound by the store, never payload authority.
#[derive(Debug, Clone)]
pub struct ConversationTurnClaimRequest {
    pub thread_id: String,
    pub request_id: String,
    pub owner: String,
    /// Explicit eligibility boundary for deterministic admission.
    pub now: DateTime<Utc>,
    pub lease_until: DateTime<Utc>,
}

/// Exact ownership proof, issued by the store and invalid after takeover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationTurnClaim {
    pub thread_id: String,
    pub request_id: String,
    pub owner: String,
    pub epoch: u64,
    pub lease_until: DateTime<Utc>,
}

/// Execution admission differs from durable completed-request deduplication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationTurnClaimOutcome {
    Claimed(ConversationTurnClaim),
    Busy,
    Completed,
}

/// Object-safe async conversation API. Implementations preserve JSONL semantics.
///
/// A supplied backend refusal never permits local fallback. Authoritative turn
/// claims must exclude concurrent owners and fence every dependent commit in the
/// native atomic protocol; a lease precheck alone does not meet this contract.
#[async_trait]
pub trait ConversationPersistence: Send + Sync {
    async fn ensure_thread(
        &self,
        request: CreateConversationThread,
    ) -> ConversationResult<ConversationThread>;
    async fn get_thread(&self, thread_id: &str) -> ConversationResult<Option<ConversationThread>>;
    async fn list_threads(&self) -> ConversationResult<Vec<ConversationThread>>;
    async fn get_messages(&self, thread_id: &str) -> ConversationResult<Vec<ThreadMessage>>;
    async fn append_message(
        &self,
        thread_id: &str,
        message: ThreadMessage,
    ) -> ConversationResult<ThreadMessage>;
    async fn update_thread_title(
        &self,
        thread_id: &str,
        title: &str,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread>;
    async fn update_thread_labels(
        &self,
        thread_id: &str,
        labels: Vec<String>,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread>;
    /// The host validates endpoint/path authority before supplying this value.
    async fn update_thread_working_dir(
        &self,
        thread_id: &str,
        working_dir: Option<String>,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread>;
    async fn update_message(
        &self,
        thread_id: &str,
        message_id: &str,
        patch: ThreadMessagePatch,
    ) -> ConversationResult<ThreadMessage>;
    /// Remove the cut message and tail; an unknown ID returns None.
    async fn delete_messages_from(
        &self,
        thread_id: &str,
        message_id: &str,
    ) -> ConversationResult<Option<usize>>;
    async fn delete_thread(&self, thread_id: &str, deleted_at: &str) -> ConversationResult<bool>;
    async fn purge_threads(&self) -> ConversationResult<ConversationPurgeStats>;
    async fn search_cross_thread_messages(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> ConversationResult<Vec<CrossThreadHit>>;
    /// Acquire the thread's exclusive execution lease, even for differing request IDs.
    ///
    /// Completed request IDs must not expire with the lease. A claim-capable
    /// implementation also needs an atomic proof-bearing commit protocol for
    /// its transcript/messages; these lifecycle methods alone do not authorize
    /// ordinary append_message to bypass ownership. This initial seam implements
    /// neither claimed commits nor a distributed claim provider.
    async fn claim_turn(
        &self,
        _request: ConversationTurnClaimRequest,
    ) -> ConversationResult<ConversationTurnClaimOutcome> {
        unsupported("claim_turn")
    }
    /// Renew only the exact active owner/epoch, validating expiry atomically.
    async fn renew_turn(
        &self,
        _claim: &ConversationTurnClaim,
        _now: DateTime<Utc>,
        _lease_until: DateTime<Utc>,
    ) -> ConversationResult<ConversationTurnClaim> {
        unsupported("renew_turn")
    }
    /// Mark this request durably completed only under its exact active ownership.
    async fn complete_turn(
        &self,
        _claim: &ConversationTurnClaim,
        _now: DateTime<Utc>,
    ) -> ConversationResult<()> {
        unsupported("complete_turn")
    }
    /// Release only this exact ownership; never release a replacement owner.
    async fn release_turn(
        &self,
        _claim: &ConversationTurnClaim,
        _now: DateTime<Utc>,
    ) -> ConversationResult<()> {
        unsupported("release_turn")
    }
}

fn unsupported<T>(operation: &'static str) -> ConversationResult<T> {
    Err(ConversationError::Unsupported { operation })
}

/// Explicit classic JSONL adapter. Never selected implicitly after a backend error.
///
/// Operations offload the unchanged synchronous store to Tokio's blocking pool,
/// including on a current-thread runtime. Construct and call this adapter only
/// when the host explicitly selects classic workspace persistence. Reads may
/// repair legacy stats exactly as the underlying store does; this is not an
/// importer or a source-preserving snapshot reader. A dropped waiter does not
/// cancel an already-running classic filesystem operation. Turn leases remain
/// unsupported; process-local locks do not establish distributed ownership.
#[derive(Debug, Clone)]
pub struct FileConversationStore {
    store: ConversationStore,
}
impl FileConversationStore {
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(ConversationStore) -> Result<T, String> + Send + 'static,
    ) -> ConversationResult<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await?
            .map_err(ConversationError::Legacy)
    }
    /// Select the classic workspace explicitly; construction creates no files.
    pub fn new(workspace_dir: PathBuf) -> Self {
        Self {
            store: ConversationStore::new(workspace_dir),
        }
    }
}

#[async_trait]
impl ConversationPersistence for FileConversationStore {
    async fn ensure_thread(
        &self,
        request: CreateConversationThread,
    ) -> ConversationResult<ConversationThread> {
        self.run(move |store| store.ensure_thread(request)).await
    }
    async fn get_thread(&self, thread_id: &str) -> ConversationResult<Option<ConversationThread>> {
        let thread_id = thread_id.to_owned();
        self.run(move |store| {
            store
                .list_threads()
                .map(|threads| threads.into_iter().find(|thread| thread.id == thread_id))
        })
        .await
    }
    async fn list_threads(&self) -> ConversationResult<Vec<ConversationThread>> {
        self.run(move |store| store.list_threads()).await
    }
    async fn get_messages(&self, thread_id: &str) -> ConversationResult<Vec<ThreadMessage>> {
        let thread_id = thread_id.to_owned();
        self.run(move |store| store.get_messages(&thread_id)).await
    }
    async fn append_message(
        &self,
        thread_id: &str,
        message: ThreadMessage,
    ) -> ConversationResult<ThreadMessage> {
        let thread_id = thread_id.to_owned();
        self.run(move |store| store.append_message(&thread_id, message))
            .await
    }
    async fn update_thread_title(
        &self,
        thread_id: &str,
        title: &str,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        let thread_id = thread_id.to_owned();
        let title = title.to_owned();
        let updated_at = updated_at.to_owned();
        self.run(move |store| store.update_thread_title(&thread_id, &title, &updated_at))
            .await
    }
    async fn update_thread_labels(
        &self,
        thread_id: &str,
        labels: Vec<String>,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        let thread_id = thread_id.to_owned();
        let updated_at = updated_at.to_owned();
        self.run(move |store| store.update_thread_labels(&thread_id, labels, &updated_at))
            .await
    }
    async fn update_thread_working_dir(
        &self,
        thread_id: &str,
        working_dir: Option<String>,
        updated_at: &str,
    ) -> ConversationResult<ConversationThread> {
        let thread_id = thread_id.to_owned();
        let updated_at = updated_at.to_owned();
        self.run(move |store| store.update_thread_working_dir(&thread_id, working_dir, &updated_at))
            .await
    }
    async fn update_message(
        &self,
        thread_id: &str,
        message_id: &str,
        patch: ThreadMessagePatch,
    ) -> ConversationResult<ThreadMessage> {
        let thread_id = thread_id.to_owned();
        let message_id = message_id.to_owned();
        self.run(move |store| store.update_message(&thread_id, &message_id, patch))
            .await
    }
    async fn delete_messages_from(
        &self,
        thread_id: &str,
        message_id: &str,
    ) -> ConversationResult<Option<usize>> {
        let thread_id = thread_id.to_owned();
        let message_id = message_id.to_owned();
        self.run(move |store| store.delete_messages_from(&thread_id, &message_id))
            .await
    }
    async fn delete_thread(&self, thread_id: &str, deleted_at: &str) -> ConversationResult<bool> {
        let thread_id = thread_id.to_owned();
        let deleted_at = deleted_at.to_owned();
        self.run(move |store| store.delete_thread(&thread_id, &deleted_at))
            .await
    }
    async fn purge_threads(&self) -> ConversationResult<ConversationPurgeStats> {
        self.run(move |store| store.purge_threads()).await
    }
    async fn search_cross_thread_messages(
        &self,
        query: &str,
        limit: usize,
        exclude_thread_id: Option<&str>,
    ) -> ConversationResult<Vec<CrossThreadHit>> {
        let query = query.to_owned();
        let exclude_thread_id = exclude_thread_id.map(str::to_owned);
        self.run(move |store| {
            store.search_cross_thread_messages(&query, limit, exclude_thread_id.as_deref())
        })
        .await
    }
}
