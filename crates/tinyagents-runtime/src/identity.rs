use std::sync::Arc;

use tinyagents_session::transcript::TranscriptMeta;

use crate::types::{ResolvedSessionStores, SessionIdentity, SessionIdentityRequest};
use crate::RuntimeError;

/// Resolves logical session naming inputs against host-authorized storage.
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
