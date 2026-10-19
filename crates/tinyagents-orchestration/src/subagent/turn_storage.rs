//! Scope-restricted storage for a parent/thread turn holding a TSD lease.

use std::sync::Arc;

use tinystoragedrivers_core::{
    Capabilities, Capability, Fence, Principal, Scope, ScopeGeneration, ScopedStorage,
    StorageBackend, StorageError, Tenant,
};

/// A backend view that exposes only one tenant and its agent-principal scopes,
/// and applies one native TSD fence to every returned scope.
///
/// Hosts pass this restricted backend to `SessionStoreProvider::bind_actor`
/// while a durable turn lease is held. The wrapper never returns its raw
/// backend or an unfenced `ScopedStorage`. It rejects module, cluster, other
/// tenant, alternate-fence, and nonzero-generation bindings.
#[derive(Clone)]
pub struct FencedSessionBackend {
    backend: Arc<dyn StorageBackend>,
    tenant: Scope,
    tenant_kind: Tenant,
    fence: Fence,
}

impl std::fmt::Debug for FencedSessionBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FencedSessionBackend")
            .field("driver", &self.backend.document_driver())
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl FencedSessionBackend {
    /// Restricts `backend` to `tenant` and agent principals under it, fencing
    /// every scope's writes with `fence`.
    ///
    /// # Errors
    ///
    /// Refuses a cluster/principal tenant, an invalid scope, or a backend that
    /// does not advertise native fencing.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        tenant: Scope,
        fence: Fence,
    ) -> Result<Self, StorageError> {
        let parsed = Scope::parse(tenant.as_str())?;
        if parsed.principal.is_some() || parsed.tenant == Tenant::Cluster {
            return Err(StorageError::invalid_input(
                "a fenced session backend requires a tenant scope without a principal",
            ));
        }
        backend.capabilities().require(Capability::Fencing)?;
        fence.validate()?;
        Ok(Self {
            backend,
            tenant,
            tenant_kind: parsed.tenant,
            fence,
        })
    }

    /// Returns the tenant this adapter admits.
    pub fn tenant(&self) -> &Scope {
        &self.tenant
    }

    /// Returns the native fence retained by this adapter.
    pub fn fence(&self) -> &Fence {
        &self.fence
    }

    fn admits(&self, scope: &Scope) -> bool {
        let Ok(parsed) = Scope::parse(scope.as_str()) else {
            return false;
        };
        if parsed.tenant != self.tenant_kind {
            return false;
        }
        match parsed.principal {
            None => scope == &self.tenant,
            Some(Principal::Agent(_)) => scope.tenant_scope().as_ref() == Some(&self.tenant),
            Some(Principal::Module(_)) => false,
        }
    }

    fn bind(&self, scope: &Scope) -> Result<ScopedStorage, StorageError> {
        if !self.admits(scope) {
            return Err(StorageError::invalid_input(format!(
                "scope `{scope}` is outside this fenced session tenant"
            )));
        }
        self.backend.for_scope_fenced(scope, &self.fence)
    }
}

#[async_trait::async_trait]
impl StorageBackend for FencedSessionBackend {
    fn driver(&self) -> &'static str {
        self.backend.driver()
    }

    fn document_driver(&self) -> &'static str {
        self.backend.document_driver()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::none().with(Capability::Fencing)
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>, StorageError> {
        let backend = self.backend.existing_schema()?;
        let restricted = Self::new(backend, self.tenant.clone(), self.fence.clone())?;
        Ok(Arc::new(restricted))
    }

    async fn prepare_schema(&self) -> Result<(), StorageError> {
        Err(StorageError::unsupported(
            Capability::ExistingSchema,
            "turn-fenced session backends cannot bootstrap shared schema",
        ))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage, StorageError> {
        self.bind(scope)
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage, StorageError> {
        if fence != &self.fence {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "a second fence cannot be composed with the admitted thread lease",
            ));
        }
        self.bind(scope)
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<ScopedStorage, StorageError> {
        if generation != ScopeGeneration::default() {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "turn-fenced session backends do not admit alternate scope generations",
            ));
        }
        self.bind(scope)
    }

    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage, StorageError> {
        if generation != ScopeGeneration::default() || fence != &self.fence {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "turn-fenced session backends reject alternate fence or scope-generation bindings",
            ));
        }
        self.bind(scope)
    }
}

#[cfg(test)]
#[path = "turn_storage_tests.rs"]
mod tests;
