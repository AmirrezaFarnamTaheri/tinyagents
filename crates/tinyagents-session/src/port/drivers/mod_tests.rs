use super::*;
use crate::testkit::conformance::{session_store_conformance, session_store_isolation_conformance};
use crate::turn_state::{TurnLifecycle, TurnState};
use tinystoragedrivers_core::MemoryStorage;
use tinystoragedrivers_sqlite::SqliteStorage;

fn memory() -> DriverSessionStores {
    DriverSessionStores::new(Arc::new(MemoryStorage::new())).unwrap()
}

#[test]
fn bootstrap_manifest_contains_all_session_document_indexes() {
    use tinystoragedrivers_core::IndexSpec;
    let specs = DriverSessionStores::collection_specs();
    assert_eq!(specs.len(), 4);
    assert_eq!(specs[0].name, "session_transcript_entries");
    assert_eq!(
        specs[0].indexes,
        vec![
            IndexSpec::new("by_stem", ["stem", "seq"]),
            IndexSpec::new("by_stem_written", ["stem", "written"]),
        ]
    );
    assert_eq!(specs[1].name, "session_transcripts");
    assert_eq!(
        specs[1].indexes,
        vec![
            IndexSpec::new("by_thread", ["thread_id", "created_at"]),
            IndexSpec::new("by_agent_name", ["agent_name", "created_at"]),
            IndexSpec::new("by_agent_id", ["agent_id", "created_at"]),
        ]
    );
    assert_eq!(specs[2].name, "session_turn_states");
    assert_eq!(
        specs[2].indexes,
        vec![IndexSpec::new("by_thread", ["thread_id"])]
    );
    assert_eq!(specs[3], DriverStore::collection_spec("session_kv"));
    for spec in specs {
        spec.validate().unwrap();
        assert!(spec.ttl_field.is_none());
        assert!(spec.search.is_none());
    }
}

#[tokio::test]
async fn bootstrap_preparation_is_idempotent_and_preserves_native_session_data() {
    let directory = tempfile::tempdir().unwrap();
    let backends: Vec<Arc<dyn StorageBackend>> = vec![
        Arc::new(MemoryStorage::new()),
        Arc::new(SqliteStorage::open(directory.path().join("prepared.db")).unwrap()),
    ];
    for backend in backends {
        let scoped = backend.for_scope(&Scope::local()).unwrap();
        DriverSessionStores::prepare_schema(scoped.documents().as_ref())
            .await
            .unwrap();
        scoped
            .documents()
            .put(
                "session_kv",
                "bootstrap-control",
                serde_json::json!({"ns": "control", "value": "retained"}),
                tinystoragedrivers_core::Precondition::Absent,
            )
            .await
            .unwrap();
        DriverSessionStores::prepare_schema(scoped.documents().as_ref())
            .await
            .unwrap();
        assert_eq!(
            scoped
                .documents()
                .get("session_kv", "bootstrap-control")
                .await
                .unwrap()
                .unwrap()
                .doc["value"],
            "retained"
        );
        let provider = DriverSessionStores::new(backend).unwrap();
        session_store_conformance(&provider).await;
        session_store_isolation_conformance(&provider).await;
    }
}

fn sqlite(dir: &tempfile::TempDir) -> DriverSessionStores {
    let storage = SqliteStorage::open(dir.path().join("sessions.db")).unwrap();
    DriverSessionStores::new(Arc::new(storage)).unwrap()
}

#[tokio::test]
async fn memory_backed_stores_meet_the_session_store_contract() {
    session_store_conformance(&memory()).await;
}

#[tokio::test]
async fn memory_backed_stores_keep_agents_apart() {
    session_store_isolation_conformance(&memory()).await;
}

#[tokio::test]
async fn sqlite_backed_stores_meet_the_session_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    session_store_conformance(&sqlite(&dir)).await;
}

#[tokio::test]
async fn sqlite_backed_stores_keep_agents_apart() {
    let dir = tempfile::tempdir().unwrap();
    session_store_isolation_conformance(&sqlite(&dir)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stores_work_from_a_multi_threaded_runtime() {
    session_store_conformance(&memory()).await;
}

#[test]
fn stores_work_outside_any_runtime() {
    let stores = memory().for_agent("plain");
    let turn = TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z");
    stores.turn_states.put(&turn).unwrap();
    assert_eq!(stores.turn_states.get("t").unwrap(), Some(turn));
}

#[test]
fn a_valid_agent_id_is_its_own_scope() {
    assert_eq!(
        DriverSessionStores::scope_for("agent-7").as_str(),
        "agent-7"
    );
}

#[test]
fn any_other_agent_id_maps_to_a_stable_hashed_scope() {
    let spaced = DriverSessionStores::scope_for("has a space");
    assert!(spaced.as_str().starts_with("sha256:"));
    assert_eq!(spaced, DriverSessionStores::scope_for("has a space"));
    assert_ne!(spaced, DriverSessionStores::scope_for("has  a space"));
    assert!(
        DriverSessionStores::scope_for("")
            .as_str()
            .starts_with("sha256:")
    );
    let long = "a".repeat(300);
    assert!(
        DriverSessionStores::scope_for(&long)
            .as_str()
            .starts_with("sha256:")
    );
}

#[test]
fn a_raw_id_can_never_name_a_hashed_scope() {
    let hashed = DriverSessionStores::scope_for("has a space");
    let impostor = DriverSessionStores::scope_for(hashed.as_str());
    assert_ne!(
        impostor, hashed,
        "ids in the reserved prefix are hashed too"
    );
    assert!(impostor.as_str().starts_with("sha256:"));
}

#[test]
fn two_backends_never_share_a_destination() {
    let one = memory().for_agent("a");
    let two = memory().for_agent("a");
    assert_ne!(
        one.transcripts.destination_key(),
        two.transcripts.destination_key()
    );
}

#[test]
fn an_agent_gets_the_same_stores_every_time() {
    let provider = memory();
    let first = provider.for_agent("a");
    let again = provider.for_agent("a");
    assert!(Arc::ptr_eq(&first.turn_states, &again.turn_states));
    assert_eq!(
        first.transcripts.destination_key(),
        again.transcripts.destination_key()
    );
    assert_ne!(
        first.transcripts.destination_key(),
        provider.for_agent("b").transcripts.destination_key()
    );
    let debug = format!("{provider:?}");
    assert!(debug.contains("agents: 2"), "{debug}");
    assert!(debug.contains("memory"), "{debug}");
    assert!(
        provider
            .destination_key()
            .is_some_and(|key| key.starts_with("memory://"))
    );
}

#[test]
fn data_survives_a_new_provider_on_the_same_backend() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let turn = TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z");
    DriverSessionStores::new(Arc::clone(&backend))
        .unwrap()
        .for_agent("a")
        .turn_states
        .put(&turn)
        .unwrap();
    let reopened = DriverSessionStores::new(backend).unwrap().for_agent("a");
    assert_eq!(reopened.turn_states.get("t").unwrap(), Some(turn));
}

#[test]
fn recover_marks_open_agents_turns_interrupted() {
    let provider = memory();
    let stores = provider.for_agent("a");
    stores
        .turn_states
        .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
        .unwrap();
    provider.recover().unwrap();
    assert_eq!(
        stores.turn_states.get("t").unwrap().unwrap().lifecycle,
        TurnLifecycle::Interrupted
    );
}

#[test]
fn recover_on_open_interrupts_turns_an_earlier_process_left() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let earlier = DriverSessionStores::new(Arc::clone(&backend)).unwrap();
    earlier
        .for_agent("a")
        .turn_states
        .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
        .unwrap();

    let plain = DriverSessionStores::new(Arc::clone(&backend)).unwrap();
    assert_eq!(
        plain
            .for_agent("a")
            .turn_states
            .get("t")
            .unwrap()
            .unwrap()
            .lifecycle,
        TurnLifecycle::Started,
        "without the option nothing is touched"
    );

    let recovering = DriverSessionStores::new(backend)
        .unwrap()
        .recover_on_open(true);
    assert!(format!("{recovering:?}").contains("recover_on_open: true"));
    let stores = recovering.for_agent("a");
    assert_eq!(
        stores.turn_states.get("t").unwrap().unwrap().lifecycle,
        TurnLifecycle::Interrupted
    );

    // Only the first open recovers: a turn started afterwards stays live.
    stores
        .turn_states
        .put(&TurnState::started("t2", "r", 8, "2026-01-01T00:01:00Z"))
        .unwrap();
    recovering.agents.lock().unwrap().clear();
    assert_eq!(
        recovering
            .for_agent("a")
            .turn_states
            .get("t2")
            .unwrap()
            .unwrap()
            .lifecycle,
        TurnLifecycle::Started
    );
}

/// A backend that cannot bind any scope.
#[derive(Debug)]
struct Unbindable;

impl StorageBackend for Unbindable {
    fn driver(&self) -> &'static str {
        "unbindable"
    }

    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        tinystoragedrivers_core::Capabilities::default()
    }

    fn for_scope(&self, _scope: &Scope) -> Result<ScopedStorage, StorageError> {
        Err(StorageError::unavailable("database is down"))
    }

}

#[tokio::test]
async fn an_unbindable_scope_fails_closed() {
    let provider = DriverSessionStores::new(Arc::new(Unbindable)).unwrap();
    assert!(provider.try_for_agent("a").is_err());
    let stores = provider.for_agent("a");

    let error = stores
        .turn_states
        .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
        .unwrap_err();
    assert!(error.contains("database is down"), "{error}");
    assert!(stores.turn_states.list().is_err());
    assert!(stores.kv.get("ns", "k").await.is_err());
    assert!(
        stores
            .journal
            .append("s", serde_json::json!(1))
            .await
            .is_err()
    );
    assert!(stores.journal.len("s").await.is_err());

    let session = crate::transcript::SessionRef::scoped("t", "a");
    assert!(!stores.transcripts.session_exists(&session));
    assert!(stores.transcripts.root_for_thread("t").is_none());
    assert!(stores.transcripts.latest_for_agent("a").is_none());
    let handle = stores
        .transcripts
        .open_stem("stem", transcripts::tests::meta("t"))
        .unwrap();
    assert!(handle.read_session().is_err());
    assert!(handle.messages().is_err());
    assert!(
        stores
            .transcripts
            .begin_generation(&session, transcripts::tests::meta("t"))
            .is_err()
    );

    assert!(
        provider.agents.lock().unwrap().is_empty(),
        "refusals are not cached"
    );
    provider.recover().unwrap();
}

/// A memory backend whose documents fail every query while `down` is set.
#[derive(Debug)]
struct Flaky {
    storage: MemoryStorage,
    down: Arc<std::sync::atomic::AtomicBool>,
    reject_declaration: bool,
}

#[derive(Debug)]
struct FlakyDocs {
    inner: Arc<dyn tinystoragedrivers_core::DocumentStore>,
    down: Arc<std::sync::atomic::AtomicBool>,
    reject_declaration: bool,
}

impl FlakyDocs {
    /// Injects a typed refusal while the recovery backend is unavailable.
    fn check(&self) -> Result<(), StorageError> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            Err(StorageError::unsupported(
                tinystoragedrivers_core::Capability::Fencing,
                "flaky backend is down",
            )
            .with_source(NativeFailure))
        } else {
            Ok(())
        }
    }
}

#[tinystoragedrivers_core::async_trait]
impl tinystoragedrivers_core::DocumentStore for FlakyDocs {
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        self.inner.capabilities()
    }
    /// Fails declaration when selected, otherwise delegates the native schema operation.
    async fn ensure_collection(
        &self,
        spec: &tinystoragedrivers_core::CollectionSpec,
    ) -> Result<(), StorageError> {
        if self.reject_declaration {
            self.check()?;
        }
        self.inner.ensure_collection(spec).await
    }
    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<tinystoragedrivers_core::Versioned<serde_json::Value>>, StorageError> {
        self.inner.get(collection, id).await
    }
    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> Result<tinystoragedrivers_core::Version, StorageError> {
        self.inner.put(collection, id, doc, precondition).await
    }
    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> Result<bool, StorageError> {
        self.inner.delete(collection, id, precondition).await
    }
    async fn query(
        &self,
        collection: &str,
        query: &tinystoragedrivers_core::Query,
    ) -> Result<
        tinystoragedrivers_core::Page<tinystoragedrivers_core::Versioned<serde_json::Value>>,
        StorageError,
    > {
        self.check()?;
        self.inner.query(collection, query).await
    }
    async fn count(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> Result<u64, StorageError> {
        self.inner.count(collection, filter).await
    }
    async fn delete_where(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> Result<u64, StorageError> {
        self.inner.delete_where(collection, filter).await
    }
    async fn claim(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
        sort: &[tinystoragedrivers_core::Sort],
        patch: &serde_json::Value,
    ) -> Result<Option<tinystoragedrivers_core::Versioned<serde_json::Value>>, StorageError> {
        self.inner.claim(collection, filter, sort, patch).await
    }
    async fn drop_collection(&self, collection: &str) -> Result<(), StorageError> {
        self.inner.drop_collection(collection).await
    }
}

impl StorageBackend for Flaky {
    fn driver(&self) -> &'static str {
        "flaky"
    }
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        self.storage.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage, StorageError> {
        let inner = self.storage.for_scope(scope)?;
        Ok(ScopedStorage::new(
            scope.clone(),
            "flaky",
            Arc::new(FlakyDocs {
                inner: Arc::clone(inner.documents()),
                down: Arc::clone(&self.down),
                reject_declaration: self.reject_declaration,
            }),
            Arc::clone(inner.streams()),
            Arc::clone(inner.blobs()),
        ))
    }
}

#[test]
fn a_failed_recovery_fails_closed_and_is_retried() {
    let down = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let provider = DriverSessionStores::new(Arc::new(Flaky {
        storage: MemoryStorage::new(),
        down: Arc::clone(&down),
        reject_declaration: false,
    }))
    .unwrap()
    .recover_on_open(true);

    let error = provider.try_for_agent("a").unwrap_err();
    assert!(error.message().contains("flaky backend is down"), "{error}");
    let refused = provider.for_agent("a");
    assert!(
        refused
            .turn_states
            .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
            .is_err(),
        "no turn can start before recovery ran"
    );
    assert!(provider.agents.lock().unwrap().is_empty());

    down.store(false, std::sync::atomic::Ordering::SeqCst);
    let stores = provider.for_agent("a");
    stores
        .turn_states
        .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
        .unwrap();
    assert_eq!(
        provider
            .for_agent("a")
            .turn_states
            .get("t")
            .unwrap()
            .unwrap()
            .lifecycle,
        TurnLifecycle::Started,
        "a recovered agent is never swept again"
    );
}

#[derive(Debug)]
struct NativeFailure;
impl std::fmt::Display for NativeFailure {
    /// Displays the marker without changing its native error identity.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native failure marker")
    }
}
impl std::error::Error for NativeFailure {}

#[derive(Debug)]
struct NativeRefusal {
    marker: Arc<NativeFailure>,
}
impl StorageBackend for NativeRefusal {
    /// Names the scope-refusing fixture backend.
    fn driver(&self) -> &'static str {
        "native-refusal"
    }
    /// Advertises no optional native capabilities.
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        Default::default()
    }
    /// Refuses admission with the same source Arc supplied by the test.
    fn for_scope(&self, _: &Scope) -> Result<ScopedStorage, StorageError> {
        Err(StorageError::fenced("scope rejected").with_source(self.marker.clone()))
    }
}

#[test]
/// Pins native scope error and source identity through object-safe checked opening.
fn checked_open_preserves_native_scope_refusal_through_dyn_provider() {
    use std::error::Error;
    let marker = Arc::new(NativeFailure);
    let provider: Arc<dyn SessionStoreProvider> = Arc::new(
        DriverSessionStores::new(Arc::new(NativeRefusal {
            marker: marker.clone(),
        }))
        .unwrap(),
    );
    // Existing unusual keys retain the owner's hashed scope mapping.
    for key in ["agent", "", "unusual key", "sha256:reserved"] {
        let error = provider.try_for_agent(key).unwrap_err();
        assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Fenced);
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<Arc<NativeFailure>>()
            .unwrap();
        assert!(Arc::ptr_eq(source, &marker));
    }
}

#[test]
/// Pins native query errors, uncached failure and successful recovery retry.
fn checked_open_preserves_native_recovery_query_refusal_and_retries() {
    use std::error::Error;
    let down = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let provider = DriverSessionStores::new(Arc::new(Flaky {
        storage: MemoryStorage::new(),
        down: down.clone(),
        reject_declaration: false,
    }))
    .unwrap()
    .recover_on_open(true);
    let error = provider.try_for_agent("agent").unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(
            tinystoragedrivers_core::Capability::Fencing
        )
    );
    assert!(error.source().unwrap().is::<NativeFailure>());
    assert!(provider.agents.lock().unwrap().is_empty());
    assert!(provider.recovered.lock().unwrap().is_empty());
    down.store(false, std::sync::atomic::Ordering::SeqCst);
    let opened = provider.try_for_agent("agent").unwrap();
    let cached = provider.try_for_agent("agent").unwrap();
    assert!(Arc::ptr_eq(&opened.turn_states, &cached.turn_states));
}

#[test]
/// Pins native declaration errors before turn recovery can proceed.
fn checked_open_preserves_native_recovery_declaration_refusal() {
    use std::error::Error;
    let provider = DriverSessionStores::new(Arc::new(Flaky {
        storage: MemoryStorage::new(),
        down: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        reject_declaration: true,
    }))
    .unwrap()
    .recover_on_open(true);
    let error = provider.try_for_agent("agent").unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(
            tinystoragedrivers_core::Capability::Fencing
        )
    );
    assert!(error.source().unwrap().is::<NativeFailure>());
    assert!(provider.agents.lock().unwrap().is_empty());
}

#[test]
/// Pins cache reuse, actor cache independence and durable scope isolation.
fn checked_open_caches_success_but_actor_rebinding_has_independent_handles() {
    use std::error::Error;
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let provider: Arc<dyn SessionStoreProvider> =
        Arc::new(DriverSessionStores::new(backend.clone()).unwrap());
    let first = provider.try_for_agent("agent").unwrap();
    let cached = provider.try_for_agent("agent").unwrap();
    assert!(Arc::ptr_eq(&first.turn_states, &cached.turn_states));
    let rebound = provider.bind_actor(backend).unwrap().unwrap();
    let independent = rebound.try_for_agent("agent").unwrap();
    assert!(!Arc::ptr_eq(&first.turn_states, &independent.turn_states));
    first
        .turn_states
        .put(&TurnState::started("t", "r", 8, "2026-01-01T00:00:00Z"))
        .unwrap();
    assert!(independent.turn_states.get("t").unwrap().is_some());
    assert!(
        rebound
            .try_for_agent("other")
            .unwrap()
            .turn_states
            .get("t")
            .unwrap()
            .is_none()
    );

    let marker = Arc::new(NativeFailure);
    let refused = provider
        .bind_actor(Arc::new(NativeRefusal {
            marker: marker.clone(),
        }))
        .unwrap()
        .unwrap();
    let error = refused.try_for_agent("agent").unwrap_err();
    assert!(Arc::ptr_eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<Arc<NativeFailure>>()
            .unwrap(),
        &marker
    ));
    assert!(Arc::ptr_eq(
        &first.turn_states,
        &provider.try_for_agent("agent").unwrap().turn_states
    ));
}

#[tokio::test]
async fn native_provider_returns_stable_bound_ledger_and_rebinds_actor_independently() {
    let backend = Arc::new(tinystoragedrivers_core::MemoryStorage::new());
    let provider = DriverSessionStores::new(backend).unwrap();
    let one = provider.run_ledger_for_agent("alice").unwrap().unwrap();
    let same = provider.run_ledger_for_agent("alice").unwrap().unwrap();
    assert!(Arc::ptr_eq(&one, &same));
    one.append_run_event(crate::run_ledger::RunEventAppend {
        run_id: "run".into(),
        event_type: "one".into(),
        payload: serde_json::json!({}),
    })
    .await
    .unwrap();
    let bob = provider.run_ledger_for_agent("bob").unwrap().unwrap();
    assert_eq!(
        bob.list_recent_run_events(&crate::run_ledger::RunEventListRequest {
            run_id: "run".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .count,
        0
    );
    let actor = provider
        .bind_actor(Arc::new(tinystoragedrivers_core::MemoryStorage::new()))
        .unwrap()
        .unwrap();
    let rebound = actor.run_ledger_for_agent("alice").unwrap().unwrap();
    assert!(!Arc::ptr_eq(&one, &rebound));
    assert_eq!(
        rebound
            .list_recent_run_events(&crate::run_ledger::RunEventListRequest {
                run_id: "run".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .count,
        0
    );
}

async fn prepared_restricted_actor_contract(backend: Arc<dyn StorageBackend>) {
    backend.prepare_schema().await.unwrap();
    let bootstrap = backend.for_scope(&Scope::local()).unwrap();
    DriverSessionStores::prepare_schema(bootstrap.documents().as_ref())
        .await
        .unwrap();
    let restricted = backend.existing_schema().unwrap();
    let actor =
        DriverSessionStores::new_with_schema_admission(restricted, SchemaAdmission::ValidateOnly)
            .unwrap()
            .recover_on_open(true);
    let opened = actor.try_for_agent("prepared-actor");
    assert!(
        opened.is_ok(),
        "prepared restricted actor recovery refused: {:?}",
        opened.err()
    );
    session_store_conformance(&actor).await;
    session_store_isolation_conformance(&actor).await;
}

#[tokio::test]
async fn prepared_restricted_actor_recovers_and_uses_memory_stores() {
    prepared_restricted_actor_contract(Arc::new(MemoryStorage::new())).await;
}

#[tokio::test]
async fn prepared_restricted_actor_recovers_and_uses_sqlite_stores() {
    let directory = tempfile::tempdir().unwrap();
    prepared_restricted_actor_contract(Arc::new(
        SqliteStorage::open(directory.path().join("prepared-actor.db")).unwrap(),
    ))
    .await;
}
