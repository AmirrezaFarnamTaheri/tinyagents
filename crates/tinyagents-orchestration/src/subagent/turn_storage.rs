//! Scope-restricted, lease-conditioned storage for a parent/thread turn.

use std::sync::Arc;

use tinyagents_session::port::AgentStorageFactory;
use tinyagents_storage_kit::{Scope, ScopedStorage, StorageBackend, StorageError, WriteCondition};

/// Host storage restricted to the admitted tenant and conditioned on its
/// durable turn lease. The lease condition is checked atomically by the
/// backend for every mutation performed through the returned scoped handle.
#[derive(Clone)]
pub struct FencedSessionBackend {
    backend: Arc<dyn StorageBackend>,
    tenant: Scope,
    condition: WriteCondition,
}

impl std::fmt::Debug for FencedSessionBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FencedSessionBackend")
            .field("tenant", &self.tenant.as_str())
            .field("condition_clauses", &self.condition.clauses().len())
            .finish_non_exhaustive()
    }
}

impl FencedSessionBackend {
    /// Restricts storage to agent scopes under `tenant`, conditioned on the
    /// exact durable lease tenure supplied by the caller.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        tenant: Scope,
        condition: WriteCondition,
    ) -> Result<Self, StorageError> {
        if condition.clauses().is_empty() {
            return Err(StorageError::invalid_input(
                "a session write condition must contain a lease guard",
            ));
        }
        if tenant.as_str() == "cluster" || tenant.as_str().contains('/') {
            return Err(StorageError::invalid_input(
                "a conditioned session backend requires a tenant scope",
            ));
        }
        Ok(Self {
            backend,
            tenant,
            condition,
        })
    }

    /// Builds the session provider factory for an admitted actor. Each agent
    /// receives a distinct `tenant/agent:<id>` scope, and the backend opens
    /// that scope only with the lease condition. Backends without atomic
    /// conditional-write support fail closed.
    pub fn storage_factory(self: Arc<Self>) -> AgentStorageFactory {
        Arc::new(move |agent_id| self.for_agent(agent_id))
    }

    /// Opens one admitted agent's conditioned storage.
    pub fn for_agent(&self, agent_id: &str) -> Result<ScopedStorage, StorageError> {
        if agent_id.is_empty() {
            return Err(StorageError::invalid_input("agent id must not be empty"));
        }
        let scope = self.tenant.child(&format!("agent:{agent_id}"))?;
        self.backend.for_scope_conditioned(&scope, &self.condition)
    }

    /// Returns the tenant admitted by this backend.
    pub fn tenant(&self) -> &Scope {
        &self.tenant
    }
}

#[cfg(test)]
#[path = "turn_storage_tests.rs"]
mod tests;
