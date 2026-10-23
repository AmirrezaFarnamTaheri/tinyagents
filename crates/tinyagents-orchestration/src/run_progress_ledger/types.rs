use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::AtomicUsize,
    },
};

use tinyagents_session::run_ledger::RunLedgerStore;
use tokio::sync::Mutex;

/// Stable identity assigned by the host to one observed turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProgressIdentity {
    /// Host or client that initiated the turn.
    pub client_id: String,
    /// Conversation containing the turn.
    pub thread_id: String,
    /// Stable request identifier, also used as the run id.
    pub request_id: String,
    /// Surface that initiated the turn (for example, `web_channel`).
    pub source: String,
    /// Agent definition executing the turn.
    pub agent_id: String,
}

/// Correlation data for projecting a progress event into a host surface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressLedgerContext {
    /// Parent tool call that spawned this event's subagent task, when known.
    pub parent_call_id: Option<String>,
}

#[derive(Default)]
pub(super) struct LedgerState {
    pub(super) subagent_parent_calls: HashMap<String, Option<String>>,
}

/// Records progress for one foreground turn in the shared TinyAgents run ledger.
///
/// Storage failures are logged and do not interrupt the live agent turn. The
/// observer also works without a store, retaining only in-memory correlation
/// needed by the host projection.
pub struct RunProgressLedger {
    pub(super) store: Option<Arc<dyn RunLedgerStore>>,
    pub(super) identity: RunProgressIdentity,
    pub(super) state: Mutex<LedgerState>,
    pub(super) events_seen: AtomicUsize,
}
