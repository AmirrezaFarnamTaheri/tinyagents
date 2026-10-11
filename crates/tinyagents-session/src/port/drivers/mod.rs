//! A [`SessionStoreProvider`] over `tinystoragedrivers` ports.
//!
//! A host opens one storage backend (SQLite on a desktop, MongoDB in the
//! cloud, memory in tests) and hands it to [`DriverSessionStores`]. Each
//! agent's stores then live in that backend under the agent's own
//! [`Scope`], so one shared database serves every agent and the driver, not
//! this crate, keeps them apart: a handle bound to one scope cannot name
//! another scope's records.
//!
//! # What goes where
//!
//! | Store | Backend shape |
//! | --- | --- |
//! | transcripts | `session_transcripts` (one index document per stem) and `session_transcript_entries` (an append-only log per stem) |
//! | turn states | `session_turn_states`, one document per turn |
//! | key-value | `session_kv`, through the harness [`DriverStore`] |
//! | journal | streams `16:session_journal/<name>`, through the harness [`DriverAppendStore`] |
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
//! An agent id that is a valid [`Scope`] is used as is. Any other id (empty,
//! over-long, holding whitespace) and any id that itself starts with
//! `sha256:` maps to `sha256:<hex>` of the id, so every agent gets a scope of
//! its own, no raw id can name a hashed scope, and the mapping never changes.

mod refused;
mod transcripts;
mod turn_states;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use sha2::{Digest, Sha256};
use tinyagents_harness::store::{DriverAppendStore, DriverStore};
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, Scope, ScopedStorage, StorageBackend, StorageError,
};

use super::{AgentStores, SessionStoreProvider};

pub use tinyagents_harness::store::SchemaAdmission;
pub use transcripts::{DriverTranscriptHistory, DriverTranscriptLocator};
pub use turn_states::DriverTurnStates;

/// Prefix of the scopes agent ids are hashed into. Reserved: an agent id
/// starting with it is hashed too, so it cannot name another agent's scope.
const HASHED_SCOPE: &str = "sha256:";

/// Collection holding each agent's key-value records.
const KV_COLLECTION: &str = "session_kv";
/// Prefix of each agent's journal streams.
const JOURNAL_PREFIX: &str = "session_journal/";

/// A [`SessionStoreProvider`] keeping every agent's stores in one
/// `tinystoragedrivers` backend, each agent in its own [`Scope`].
pub struct DriverSessionStores {
    backend: Arc<dyn StorageBackend>,
    bridge: Blocking,
    agents: Mutex<HashMap<String, AgentStores>>,
    recover_on_open: bool,
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
            .field("agents", &agents.len())
            .field("recover_on_open", &self.recover_on_open)
            .finish_non_exhaustive()
    }
}

impl DriverSessionStores {
    /// A provider over `backend`, with its own [`Blocking`] bridge.
    ///
    /// # Errors
    ///
    /// When the bridge's runtime thread cannot start.
    pub fn new(backend: Arc<dyn StorageBackend>) -> Result<Self, StorageError> {
        Ok(Self::with_bridge(backend, Blocking::new()?))
    }

    /// A provider over `backend` that runs its synchronous seams on `bridge`,
    /// so several providers (or other sync adapters) can share one thread.
    pub fn with_bridge(backend: Arc<dyn StorageBackend>, bridge: Blocking) -> Self {
        Self::with_bridge_and_schema_admission(backend, bridge, SchemaAdmission::Declare)
    }

    /// Create a provider with an immutable document schema policy.
    ///
    /// Validation-only actors also need restricted native backend handles; this
    /// policy does not grant a lease or prevent native internal lazy initialization.
    ///
    /// # Errors
    /// Failure to start the blocking bridge.
    pub fn new_with_schema_admission(
        backend: Arc<dyn StorageBackend>,
        schema_admission: SchemaAdmission,
    ) -> Result<Self, StorageError> {
        Ok(Self::with_bridge_and_schema_admission(
            backend,
            Blocking::new()?,
            schema_admission,
        ))
    }

    /// Share a bridge while keeping schema policy, caches and recovery independent.
    pub fn with_bridge_and_schema_admission(
        backend: Arc<dyn StorageBackend>,
        bridge: Blocking,
        schema_admission: SchemaAdmission,
    ) -> Self {
        Self {
            backend,
            bridge,
            agents: Mutex::new(HashMap::new()),
            recover_on_open: false,
            schema_admission,
            recovered: Mutex::new(HashSet::new()),
            id: uuid::Uuid::new_v4(),
        }
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

    /// All document declarations required by this provider.
    ///
    /// Includes transcript entries and index, turn states, and key-value
    /// records. The journal uses streams and has no document declaration.
    /// These are also used by each store's lazy initialization. Returning
    /// declarations does not initialize or authorize an actor.
    pub fn collection_specs() -> Vec<CollectionSpec> {
        let mut specs = transcripts::collection_specs().to_vec();
        specs.push(turn_states::collection_spec());
        specs.push(DriverStore::collection_spec(KV_COLLECTION));
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

    /// The scope agent `agent_id` is stored under.
    pub fn scope_for(agent_id: &str) -> Scope {
        if !agent_id.starts_with(HASHED_SCOPE)
            && let Ok(scope) = Scope::new(agent_id)
        {
            return scope;
        }
        let digest = Sha256::digest(agent_id.as_bytes());
        Scope::new(format!("{HASHED_SCOPE}{}", hex::encode(digest)))
            .expect("a sha256 hex scope is always valid")
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
        let scoped = self.backend.for_scope(&Self::scope_for(agent_id))?;
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
            kv: Arc::new(DriverStore::with_collection_and_schema_admission(
                docs,
                KV_COLLECTION,
                self.schema_admission,
            )),
            journal: Arc::new(DriverAppendStore::with_prefix(
                Arc::clone(scoped.streams()),
                JOURNAL_PREFIX,
            )),
        };
        (stores, turn_states)
    }
}

impl SessionStoreProvider for DriverSessionStores {
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
        let actor = Self::with_bridge_and_schema_admission(backend, self.bridge.clone(), admission)
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
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "mod_actor_tests.rs"]
mod actor_tests;

#[cfg(test)]
#[path = "schema_admission_tests.rs"]
mod schema_admission_tests;
