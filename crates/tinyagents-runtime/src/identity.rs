use std::sync::Arc;

use tinyagents_session::transcript::TranscriptMeta;

use crate::RuntimeError;
use crate::types::{ResolvedSessionStores, SessionIdentity, SessionIdentityRequest};

/// Resolves logical session naming inputs against host-authorized storage.
///
/// Implementations must reuse the actor-bound provider (and its pinned store
/// lease) for the lifetime of one admitted runtime session. A new admission
/// epoch must create a fresh fenced binding and runtime session; returning a
/// different tenant/backend locator during an existing admission is rejected.
/// Identity-changing preparation belongs in `before_resume`, which runs before
/// admission. `before_turn` is a trusted execution hook and must preserve the
/// admitted actor/backend authority for that turn.
pub trait SessionIdentityResolver<C: Send + Sync + 'static>: Send + Sync {
    /// Admits the current host context and returns a pinned transcript store lease.
    fn resolve(
        &self,
        request: &SessionIdentityRequest,
        context: &C,
    ) -> Result<ResolvedSessionStores, RuntimeError>;
}

pub(crate) struct IdentityConfig<C: Send + Sync + 'static> {
    pub(crate) resolver: Arc<dyn SessionIdentityResolver<C>>,
    pub(crate) session_key: String,
    pub(crate) parent: Option<tinyagents_session::transcript::SessionRef>,
    pub(crate) child_key: Option<String>,
    pub(crate) meta: TranscriptMeta,
}

pub(crate) fn same_admission(a: &SessionIdentity, b: &SessionIdentity) -> bool {
    if a.session.first_generation() != b.session.first_generation() || a.agent_id != b.agent_id {
        return false;
    }
    let left = a.stores.stores().transcripts.destination_key();
    let right = b.stores.stores().transcripts.destination_key();
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        (None, None) => Arc::ptr_eq(
            &a.stores.stores().transcripts,
            &b.stores.stores().transcripts,
        ),
        _ => false,
    }
}
