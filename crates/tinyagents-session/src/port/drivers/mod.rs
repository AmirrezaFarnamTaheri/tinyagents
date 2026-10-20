//! A [`SessionStoreProvider`] over `tinystoragedrivers` ports.
//!
//! A host opens one storage backend (SQLite on a desktop, MongoDB in the
//! cloud, memory in tests) and hands it, with the tenant [`Scope`] it serves,
//! to [`DriverSessionStores`]. Each agent's stores then live in that backend
//! under the agent's own principal scope (`tenant.agent(id)`), so one shared
//! database serves every tenant and agent and the driver, not this crate,
//! keeps them apart: a handle bound to one scope cannot name another scope's
//! records.
//!
//! # What goes where
//!
//! | Store | Backend shape |
//! | --- | --- |
//! | transcripts | `session.transcripts` (one index document per stem) and `session.transcript_entries` (an append-only log per stem) |
//! | generation reservations | `tsd.leases`, owner `session` |
//! | turn states | `session.turn_states`, one document per turn |
//! | key-value | `tsd.kv` owner `session`, through the harness [`DriverStore`] |
//! | journal | streams `harness.journal/<name>` (the run journal's family), through the harness [`DriverAppendStore`] |
//!
//! # Sync seams
//!
//! The transcript and turn-state traits are synchronous, so their
//! implementations here run each driver call on a [`Blocking`] bridge: a
//! dedicated runtime thread, which works from any caller (inside a tokio
//! runtime or not) and never needs `block_in_place`.
//!
//! # Scopes
//!
//! The provider is constructed with a **tenant** [`Scope`] (`local`, or
//! `u:<id>`; see [`Scope::tenant`]) and derives each agent's scope itself with
//! [`Scope::agent`]: `local/agent:<id>`. It never uses a raw agent id as a
//! scope. An id that is too long or holds characters outside the scope
//! grammar is hashed by [`Scope::agent`] (the segment only, so the agent
//! stays under its tenant and its fence); an empty id has no scope.

mod refused;
mod schema;
mod transcripts;
mod turn_states;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use tinyagents_harness::store::{DriverAppendStore, DriverStore, SchemaAdmission};
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, Scope, ScopedStorage, StorageBackend, StorageError,
};

use super::{AgentStores, SessionStoreProvider};

pub use schema::storage_schema;
pub use transcripts::{DriverTranscriptHistory, DriverTranscriptLocator};
pub use turn_states::DriverTurnStates;

/// The `tsd.kv` owner of the session key-value store, and the `tsd.leases`
/// owner of its generation reservations.
const KV_OWNER: &str = "session";

/// A [`SessionStoreProvider`] keeping every agent's stores in one
/// `tinystoragedrivers` backend, each agent in its own principal [`Scope`] of
/// one tenant.
pub struct DriverSessionStores {
    backend: Arc<dyn StorageBackend>,
    tenant: Scope,
    bridge: Blocking,
    agents: Mutex<HashMap<String, AgentStores>>,
    recover_on_open: bool,
    ledgers: Mutex<HashMap<String, Arc<dyn crate::run_ledger::RunLedgerStore>>>,
    schema_admission: SchemaAdmission,
    recovered: Mutex<HashSet<String>>,
    /// Names this provider in destination keys and handle paths.
    id: uuid::Uuid,
}

impl std::fmt::Debug for DriverSessionStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let agents = self.agents.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("DriverSessionStores")
            .field("driver", &self.backend.driver())
            .field("tenant", &self.tenant.as_str())
            .field("agents", &agents.len())
            .field("recover_on_open", &self.recover_on_open)
            .finish_non_exhaustive()
    }
}

impl DriverSessionStores {
    /// A provider over `backend` for `tenant`, with its own [`Blocking`] bridge.
    ///
    /// `tenant` is the tenant scope every agent of this provider lives under
    /// ([`Scope::local`] for the desktop operator, [`Scope::tenant`] for a
    /// user); an agent's stores are bound to `tenant.agent(id)`. The v1
    /// constructor took no tenant and used the raw agent id as the scope.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when `tenant` is not a tenant scope without a principal
    /// (`cluster`, or one that already names an agent or module), or when the
    /// bridge's runtime thread cannot start.
    pub fn new(backend: Arc<dyn StorageBackend>, tenant: Scope) -> Result<Self, StorageError> {
        check_tenant(&tenant)?;
        Ok(Self::with_bridge(backend, tenant, Blocking::new()?))
    }

    /// A provider over `backend` for `tenant` that runs its synchronous seams
    /// on `bridge`, so several providers (or other sync adapters) can share
    /// one thread.
    ///
    /// Unlike [`Self::new`] this does not check `tenant`: an unusable one makes
    /// every [`SessionStoreProvider::for_agent`] answer with refusing stores.
    pub fn with_bridge(backend: Arc<dyn StorageBackend>, tenant: Scope, bridge: Blocking) -> Self {
        Self::with_bridge_and_schema_admission(backend, tenant, bridge, SchemaAdmission::Declare)
    }

    /// Create a provider with an immutable document schema policy.
    ///
    /// Validation-only actors also need restricted native backend handles; this
    /// policy does not grant a lease or prevent native internal lazy initialization.
    ///
    /// # Errors
    /// `InvalidInput` for an unusable `tenant` (see [`Self::new`]), or failure
    /// to start the blocking bridge.
    pub fn new_with_schema_admission(
        backend: Arc<dyn StorageBackend>,
        tenant: Scope,
        schema_admission: SchemaAdmission,
    ) -> Result<Self, StorageError> {
        check_tenant(&tenant)?;
        Ok(Self::with_bridge_and_schema_admission(
            backend,
            tenant,
            Blocking::new()?,
            schema_admission,
        ))
    }

    /// Share a bridge while keeping schema policy, caches and recovery independent.
    pub fn with_bridge_and_schema_admission(
        backend: Arc<dyn StorageBackend>,
        tenant: Scope,
        bridge: Blocking,
        schema_admission: SchemaAdmission,
    ) -> Self {
        Self {
            backend,
            tenant,
            bridge,
            agents: Mutex::new(HashMap::new()),
            recover_on_open: false,
            ledgers: Mutex::new(HashMap::new()),
            schema_admission,
            recovered: Mutex::new(HashSet::new()),
            id: uuid::Uuid::new_v4(),
        }
    }

    /// The tenant scope every agent of this provider lives under.
    pub fn tenant(&self) -> &Scope {
        &self.tenant
    }

    /// Marks an agent's in-flight turns interrupted the first time this
    /// provider opens its stores.
    ///
    /// For a single-process host (the desktop app): there, a turn still in
    /// flight when the process starts can only be one an earlier process
    /// left behind. A host that runs several processes against one database
    /// must leave this off, since another process may own those turns.
    #[must_use]
    pub fn recover_on_open(mut self, enabled: bool) -> Self {
        self.recover_on_open = enabled;
        self
    }

    /// Core session declarations, excluding the optional run ledger.
    ///
    /// Prepare optional ledger declarations separately with
    /// [`crate::run_ledger::DriverRunLedger::prepare_schema`].
    /// Includes transcript entries and index, turn states, key-value
    /// records and the lease collection generation reservations use. The journal uses streams and has no document declaration.
    /// These are also used by each store's lazy initialization. Returning
    /// declarations does not initialize or authorize an actor.
    pub fn collection_specs() -> Vec<CollectionSpec> {
        let mut specs = transcripts::collection_specs().to_vec();
        specs.push(turn_states::collection_spec());
        specs.push(DriverStore::collection_spec());
        specs.push(transcripts::leases_spec());
        specs
    }

    /// Declare all provider collections using trusted bootstrap `documents`.
    ///
    /// Run against each addressed logical database before issuing actors.
    /// This does not prepare native internal schema, issue a lease, disable
    /// lazy initialization, or bind a provider to an actor. Declarations are
    /// idempotent. Earlier declarations may persist if a later one fails;
    /// this is not an atomic migration.
    ///
    /// # Errors
    ///
    /// The first native declaration failure, preserving its kind and cause.
    /// Handles without schema authority refuse this operation.
    pub async fn prepare_schema(documents: &dyn DocumentStore) -> Result<(), StorageError> {
        for spec in Self::collection_specs() {
            documents.ensure_collection(&spec).await?;
        }
        Ok(())
    }

    /// The concrete run ledger of `agent_id`, bound to the same scope as its
    /// other stores. Hosts use it where the object-safe
    /// [`SessionStoreProvider::run_ledger_for_agent`] handle is not enough, such
    /// as the legacy-database import.
    ///
    /// # Errors
    ///
    /// The native scope binding error.
    pub fn driver_run_ledger(
        &self,
        agent_id: &str,
    ) -> Result<crate::run_ledger::DriverRunLedger, StorageError> {
        let scoped = self.backend.for_scope(&self.scope_for(agent_id)?)?;
        Ok(match self.schema_admission {
            SchemaAdmission::Declare => crate::run_ledger::DriverRunLedger::new(scoped),
            SchemaAdmission::ValidateOnly => {
                crate::run_ledger::DriverRunLedger::new_existing_schema(scoped)
            }
        })
    }

    /// The storage of `agent_id`'s scope, and whether this provider may
    /// declare collections in it. Used by host-side one-time bookkeeping (the
    /// legacy import marker in `tsd.migrations`) that sits beside the session
    /// collections.
    #[cfg(feature = "legacy-import")]
    pub(crate) fn scoped_for(&self, agent_id: &str) -> Result<(ScopedStorage, bool), StorageError> {
        let scoped = self.backend.for_scope(&self.scope_for(agent_id)?)?;
        Ok((scoped, self.schema_admission == SchemaAdmission::Declare))
    }

    /// The bridge this provider runs synchronous seams on.
    #[cfg(feature = "legacy-import")]
    pub(crate) fn bridge(&self) -> &Blocking {
        &self.bridge
    }

    /// The scope agent `agent_id` is stored under: `tenant.agent(agent_id)`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an empty id, or an unusable tenant.
    pub fn scope_for(&self, agent_id: &str) -> Result<Scope, StorageError> {
        self.tenant.agent(agent_id)
    }

    /// The stores of `agent_id`, or the error that kept the backend from
    /// binding its scope.
    ///
    /// # Errors
    ///
    /// The native scope binding or recovery error, retaining its source.
    pub fn try_for_agent(&self, agent_id: &str) -> Result<AgentStores, StorageError> {
        if let Some(stores) = self
            .agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(agent_id)
        {
            return Ok(stores.clone());
        }
        let scoped = self.backend.for_scope(&self.scope_for(agent_id)?)?;
        if self.schema_admission == SchemaAdmission::ValidateOnly {
            let documents = Arc::clone(scoped.documents());
            self.bridge.run(async move {
                for spec in Self::collection_specs() {
                    SchemaAdmission::ValidateOnly
                        .admit(documents.as_ref(), &spec)
                        .await?;
                }
                Ok::<_, StorageError>(())
            })??;
        }
        let (stores, turn_states) = self.build(&scoped);
        if self.recover_on_open {
            // Fail closed: stores whose recovery did not run must not start
            // turns a retried sweep would then mistake for crash residue.
            // Nothing is cached, so the next open tries again.
            self.recover_agent(agent_id, &turn_states)?;
        }
        Ok(self
            .agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(agent_id.to_string())
            .or_insert(stores)
            .clone())
    }

    /// Interrupts `agent_id`'s in-flight turns unless this provider already
    /// did.
    ///
    /// The `recovered` lock is held across the sweep, so a concurrent first
    /// open of the same agent waits for it instead of handing out stores a
    /// still-running sweep could interrupt a new turn on. The agent is
    /// recorded only once the sweep succeeds.
    fn recover_agent(
        &self,
        agent_id: &str,
        turn_states: &DriverTurnStates,
    ) -> Result<(), StorageError> {
        let mut recovered = self
            .recovered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if recovered.contains(agent_id) {
            return Ok(());
        }
        let now = chrono::Utc::now().to_rfc3339();
        match turn_states.mark_all_interrupted_checked(&now) {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(
                        target: "tinyagents_session::port::drivers",
                        agent_id,
                        count,
                        "[session-store] marked turns left in flight interrupted"
                    );
                }
                recovered.insert(agent_id.to_string());
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    target: "tinyagents_session::port::drivers",
                    agent_id,
                    %error,
                    "[session-store] could not recover in-flight turns; retried on the next open"
                );
                Err(error)
            }
        }
    }

    /// Builds public stores and retains their exact typed recovery handle.
    fn build(&self, scoped: &ScopedStorage) -> (AgentStores, Arc<DriverTurnStates>) {
        let docs = Arc::clone(scoped.documents());
        // The provider's own id keeps two providers with the same driver and
        // scope from claiming one destination; unlike an address, it is
        // never reused.
        let label = format!("{}://{}/{}", scoped.driver(), self.id, scoped.scope());
        let turn_states = Arc::new(DriverTurnStates::new_with_schema_admission(
            Arc::clone(&docs),
            self.bridge.clone(),
            self.schema_admission,
        ));
        let stores = AgentStores {
            transcripts: Arc::new(DriverTranscriptLocator::new_with_schema_admission(
                Arc::clone(&docs),
                self.bridge.clone(),
                label,
                self.schema_admission,
            )),
            turn_states: turn_states.clone(),
            kv: Arc::new(DriverStore::with_owner_and_schema_admission(
                docs,
                KV_OWNER,
                self.schema_admission,
            )),
            journal: Arc::new(DriverAppendStore::journal(Arc::clone(scoped.streams()))),
        };
        (stores, turn_states)
    }
}

/// A provider's tenant must be a tenant scope without a principal.
fn check_tenant(tenant: &Scope) -> Result<(), StorageError> {
    tenant.agent("probe").map(drop)
}

impl SessionStoreProvider for DriverSessionStores {
    fn run_ledger_for_agent(
        &self,
        agent_id: &str,
    ) -> crate::run_ledger::LedgerResult<Option<Arc<dyn crate::run_ledger::RunLedgerStore>>> {
        let mut ledgers = self.ledgers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ledger) = ledgers.get(agent_id) {
            return Ok(Some(ledger.clone()));
        }
        let scoped = self.backend.for_scope(&self.scope_for(agent_id)?)?;
        let ledger: Arc<dyn crate::run_ledger::RunLedgerStore> =
            Arc::new(match self.schema_admission {
                SchemaAdmission::Declare => crate::run_ledger::DriverRunLedger::new(scoped),
                SchemaAdmission::ValidateOnly => {
                    crate::run_ledger::DriverRunLedger::new_existing_schema(scoped)
                }
            });
        ledgers.insert(agent_id.to_owned(), ledger.clone());
        Ok(Some(ledger))
    }

    /// Opens through the driver accessor without erasing native refusal errors.
    fn try_for_agent(&self, agent_id: &str) -> Result<AgentStores, StorageError> {
        DriverSessionStores::try_for_agent(self, agent_id)
    }

    fn bind_actor(
        &self,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Option<Arc<dyn SessionStoreProvider>>, StorageError> {
        self.bind_actor_with_schema_admission(backend, self.schema_admission)
    }

    fn bind_actor_with_schema_admission(
        &self,
        backend: Arc<dyn StorageBackend>,
        admission: SchemaAdmission,
    ) -> Result<Option<Arc<dyn SessionStoreProvider>>, StorageError> {
        let actor = Self::with_bridge_and_schema_admission(
            backend,
            self.tenant.clone(),
            self.bridge.clone(),
            admission,
        )
        .recover_on_open(self.recover_on_open);
        Ok(Some(Arc::new(actor)))
    }

    /// The stores of `agent_id`.
    ///
    /// Fails closed: when the backend cannot bind the agent's scope, or
    /// [`Self::recover_on_open`]'s sweep fails, the stores returned refuse
    /// every call with that error rather than fall
    /// back to somewhere the agent's data does not belong. They are not
    /// cached, so the next call tries the backend again.
    fn for_agent(&self, agent_id: &str) -> AgentStores {
        self.try_for_agent(agent_id).unwrap_or_else(|error| {
            tracing::error!(
                target: "tinyagents_session::port::drivers",
                agent_id,
                %error,
                "[session-store] backend refused the agent's scope"
            );
            refused::stores(&error, &self.bridge)
        })
    }

    /// Marks in-flight turns interrupted for every agent this provider has
    /// opened. A backend cannot enumerate its scopes, so agents not yet
    /// opened are recovered by [`Self::recover_on_open`] instead.
    fn recover(&self) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let agents: Vec<AgentStores> = self
            .agents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for stores in agents {
            stores
                .turn_states
                .mark_all_interrupted(&now)
                .map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }

    fn destination_key(&self) -> Option<String> {
        Some(format!("{}://{}", self.backend.driver(), self.id))
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "mod_actor_tests.rs"]
mod actor_tests;

#[cfg(test)]
#[path = "schema_admission_tests.rs"]
mod schema_admission_tests;
