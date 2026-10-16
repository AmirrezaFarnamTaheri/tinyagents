//! Rebindable session-store providers with stable handles for live agents.
//!
//! A handle operation snapshots its current store immediately before the
//! operation and releases the binding lock before doing I/O. Operations that
//! already captured a backend finish there; later calls through retained or
//! cloned `AgentStores` follow the new backend after `rebind` completes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};

use anyhow::Result;
use serde_json::Value;
use tinyagents_harness::store::{AppendStore, Store};
use tinystoragedrivers_core::{StorageBackend, StorageError};

use super::{AgentStores, SessionStoreProvider, TurnStates};
use crate::port::types::TurnLifecycle;
use crate::port::TurnState;
use crate::transcript::{
    SessionAdoption, SessionRef, ThreadTranscripts, TranscriptHistory, TranscriptLocator,
    TranscriptMeta, TranscriptPartial, TranscriptRead,
};
use crate::DriverSessionStores;

/// Scopes live provider handles so one test or runtime can rebind its own
/// stores without moving unrelated providers.
#[derive(Default)]
pub struct SessionStoreRegistry {
    live: Mutex<Vec<Weak<RebindableSessionStores>>>,
}

impl SessionStoreRegistry {
    fn register(&self, stores: &Arc<RebindableSessionStores>) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        live.retain(|weak| weak.strong_count() > 0);
        live.push(Arc::downgrade(stores));
    }

    fn live(&self) -> Vec<Arc<RebindableSessionStores>> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }

    /// Rebinds every provider registered here, returning the number moved.
    pub fn rebind(&self, backend: Arc<dyn StorageBackend>) -> Result<usize, StorageError> {
        let stores = self.live();
        for store in &stores {
            store.rebind(Arc::clone(&backend))?;
        }
        Ok(stores.len())
    }
}

/// Driver-backed stores whose retained agent handles follow backend rebinds.
pub struct RebindableSessionStores {
    current: RwLock<Arc<DriverSessionStores>>,
    live: Mutex<HashMap<String, Weak<LiveAgentStores>>>,
    recover_on_open: bool,
}

impl RebindableSessionStores {
    /// Creates a provider registered in the process-wide registry.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        recover_on_open: bool,
    ) -> Result<Arc<Self>, StorageError> {
        static PROCESS: std::sync::LazyLock<SessionStoreRegistry> =
            std::sync::LazyLock::new(SessionStoreRegistry::default);
        Self::new_in(&PROCESS, backend, recover_on_open)
    }

    /// Creates a provider registered only in `registry`.
    pub fn new_in(
        registry: &SessionStoreRegistry,
        backend: Arc<dyn StorageBackend>,
        recover_on_open: bool,
    ) -> Result<Arc<Self>, StorageError> {
        let driver = DriverSessionStores::new(backend)?.recover_on_open(recover_on_open);
        let this = Arc::new(Self {
            current: RwLock::new(Arc::new(driver)),
            live: Mutex::new(HashMap::new()),
            recover_on_open,
        });
        registry.register(&this);
        Ok(this)
    }

    fn current(&self) -> Arc<DriverSessionStores> {
        Arc::clone(&self.current.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Replaces the backend and updates every retained agent handle.
    ///
    /// Existing method calls retain the provider snapshot they started with;
    /// subsequent calls through a live handle resolve against the new backend.
    pub fn rebind(&self, backend: Arc<dyn StorageBackend>) -> Result<(), StorageError> {
        let current = self.current();
        let next = Arc::new(
            DriverSessionStores::with_bridge(backend, current.bridge().clone())
                .recover_on_open(self.recover_on_open),
        );
        let live = self
            .live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(agent_id, weak)| weak.upgrade().map(|binding| (agent_id.clone(), binding)))
            .collect::<Vec<_>>();

        // Prepare every replacement before publishing any, so a failed scope
        // bind cannot strand some live handles on each side of the rebind.
        let replacements = live
            .iter()
            .map(|(agent_id, binding)| {
                next.try_for_agent(agent_id)
                    .map(|stores| (Arc::clone(binding), stores))
            })
            .collect::<Result<Vec<_>, _>>()?;
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = next;
        for (binding, stores) in replacements {
            *binding.current.write().unwrap_or_else(PoisonError::into_inner) = stores;
        }
        Ok(())
    }
}

struct LiveAgentStores {
    current: RwLock<AgentStores>,
}

impl LiveAgentStores {
    fn snapshot(&self) -> AgentStores {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn handles(self: &Arc<Self>) -> AgentStores {
        AgentStores {
            transcripts: Arc::new(LiveTranscriptLocator(Arc::clone(self))),
            turn_states: Arc::new(LiveTurnStates(Arc::clone(self))),
            kv: Arc::new(LiveStore(Arc::clone(self))),
            journal: Arc::new(LiveAppendStore(Arc::clone(self))),
        }
    }
}

#[async_trait::async_trait]
impl Store for LiveStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        let store = self.0.snapshot().kv;
        store.get(namespace, key).await
    }
    async fn put(&self, namespace: &str, key: &str, value: Value) -> Result<()> {
        let store = self.0.snapshot().kv;
        store.put(namespace, key, value).await
    }
    async fn delete(&self, namespace: &str, key: &str) -> Result<()> {
        let store = self.0.snapshot().kv;
        store.delete(namespace, key).await
    }
    async fn list(&self, namespace: &str) -> Result<Vec<String>> {
        let store = self.0.snapshot().kv;
        store.list(namespace).await
    }
}

#[async_trait::async_trait]
impl AppendStore for LiveAppendStore {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        let store = self.0.snapshot().journal;
        store.append(stream, value).await
    }
    async fn read_from(&self, stream: &str, offset: u64) -> Result<Vec<(u64, Value)>> {
        let store = self.0.snapshot().journal;
        store.read_from(stream, offset).await
    }
    async fn read_window(&self, stream: &str, offset: u64, limit: usize) -> Result<Vec<(u64, Value)>> {
        let store = self.0.snapshot().journal;
        store.read_window(stream, offset, limit).await
    }
    async fn len(&self, stream: &str) -> Result<u64> {
        let store = self.0.snapshot().journal;
        store.len(stream).await
    }
}

struct LiveStore(Arc<LiveAgentStores>);
struct LiveAppendStore(Arc<LiveAgentStores>);
struct LiveTurnStates(Arc<LiveAgentStores>);
struct LiveTranscriptLocator(Arc<LiveAgentStores>);

impl TurnStates for LiveTurnStates {
    fn put(&self, state: &TurnState) -> Result<(), String> { self.0.snapshot().turn_states.put(state) }
    fn put_unless_completed(&self, state: &TurnState) -> Result<bool, String> { self.0.snapshot().turn_states.put_unless_completed(state) }
    fn get(&self, thread_id: &str) -> Result<Option<TurnState>, String> { self.0.snapshot().turn_states.get(thread_id) }
    fn get_turn(&self, thread_id: &str, request_id: &str) -> Result<Option<TurnState>, String> { self.0.snapshot().turn_states.get_turn(thread_id, request_id) }
    fn delete(&self, thread_id: &str) -> Result<bool, String> { self.0.snapshot().turn_states.delete(thread_id) }
    fn delete_turn(&self, thread_id: &str, request_id: &str) -> Result<bool, String> { self.0.snapshot().turn_states.delete_turn(thread_id, request_id) }
    fn list(&self) -> Result<Vec<TurnState>, String> { self.0.snapshot().turn_states.list() }
    fn list_thread(&self, thread_id: &str) -> Result<Vec<TurnState>, String> { self.0.snapshot().turn_states.list_thread(thread_id) }
    fn clear_all(&self) -> Result<usize, String> { self.0.snapshot().turn_states.clear_all() }
    fn mark_all_interrupted(&self, now: &str) -> Result<usize, String> { self.0.snapshot().turn_states.mark_all_interrupted(now) }
    fn settle_turn(&self, thread_id: &str, request_id: &str, lifecycle: TurnLifecycle, now: &str) -> Result<bool, String> { self.0.snapshot().turn_states.settle_turn(thread_id, request_id, lifecycle, now) }
}

impl TranscriptLocator for LiveTranscriptLocator {
    fn destination_key(&self) -> Option<String> { self.0.snapshot().transcripts.destination_key() }
    fn latest_for_agent(&self, agent: &str) -> Option<Arc<dyn TranscriptRead>> { self.0.snapshot().transcripts.latest_for_agent(agent) }
    fn root_for_thread(&self, thread: &str) -> Option<Arc<dyn TranscriptRead>> { self.0.snapshot().transcripts.root_for_thread(thread) }
    fn root_for_thread_scoped(&self, thread: &str, agent: Option<&str>) -> Option<Arc<dyn TranscriptRead>> { self.0.snapshot().transcripts.root_for_thread_scoped(thread, agent) }
    fn thread_transcripts(&self, thread: &str, agent: Option<&str>) -> Option<ThreadTranscripts> { self.0.snapshot().transcripts.thread_transcripts(thread, agent) }
    fn open_stem(&self, stem: &str, seed: TranscriptMeta) -> anyhow::Result<Arc<dyn TranscriptHistory>> { self.0.snapshot().transcripts.open_stem(stem, seed) }
    fn adopt_legacy(&self, session: &SessionRef, thread: &str, seed: &TranscriptMeta) -> anyhow::Result<Option<SessionAdoption>> { self.0.snapshot().transcripts.adopt_legacy(session, thread, seed) }
    fn append_interrupted_partial(&self, thread: &str, agent: Option<&str>, partial: &TranscriptPartial, request: Option<&str>) -> anyhow::Result<bool> { self.0.snapshot().transcripts.append_interrupted_partial(thread, agent, partial, request) }
}

/// Each resolved trait object is stable, while its backing stores are replaced
/// atomically when the owning provider rebinds.
impl SessionStoreProvider for RebindableSessionStores {
    fn for_agent(&self, agent_id: &str) -> AgentStores {
        if let Some(binding) = self.live.lock().unwrap_or_else(PoisonError::into_inner).get(agent_id).and_then(Weak::upgrade) {
            return binding.handles();
        }
        let stores = self.current().for_agent(agent_id);
        let binding = Arc::new(LiveAgentStores { current: RwLock::new(stores) });
        self.live.lock().unwrap_or_else(PoisonError::into_inner).insert(agent_id.to_owned(), Arc::downgrade(&binding));
        binding.handles()
    }

    fn try_for_agent(&self, agent_id: &str) -> Result<AgentStores, StorageError> {
        if let Some(binding) = self.live.lock().unwrap_or_else(PoisonError::into_inner).get(agent_id).and_then(Weak::upgrade) {
            return Ok(binding.handles());
        }
        let stores = self.current().try_for_agent(agent_id)?;
        let binding = Arc::new(LiveAgentStores { current: RwLock::new(stores) });
        self.live.lock().unwrap_or_else(PoisonError::into_inner).insert(agent_id.to_owned(), Arc::downgrade(&binding));
        Ok(binding.handles())
    }

    fn bind_actor(&self, backend: Arc<dyn StorageBackend>) -> Result<Option<Arc<dyn SessionStoreProvider>>, StorageError> {
        self.current().bind_actor(backend)
    }
    fn bind_actor_with_schema_admission(&self, backend: Arc<dyn StorageBackend>, admission: tinyagents_harness::store::SchemaAdmission) -> Result<Option<Arc<dyn SessionStoreProvider>>, StorageError> {
        self.current().bind_actor_with_schema_admission(backend, admission)
    }
    fn run_ledger_for_agent(&self, agent_id: &str) -> crate::run_ledger::LedgerResult<Option<Arc<dyn crate::run_ledger::RunLedgerStore>>> {
        self.current().run_ledger_for_agent(agent_id)
    }
    fn conversations_for_agent(&self, agent_id: &str) -> crate::threads::ConversationResult<Option<Arc<dyn crate::threads::ConversationPersistence>>> {
        self.current().conversations_for_agent(agent_id)
    }
    fn recover(&self) -> anyhow::Result<()> { self.current().recover() }
}
