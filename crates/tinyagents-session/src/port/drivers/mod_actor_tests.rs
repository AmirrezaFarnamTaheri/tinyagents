use super::*;
use tinystoragedrivers_api::StorageBackend;
use crate::testkit::MemoryStorage;
use crate::turn_state::TurnState;
use serde_json::json;
use tinyagents_storage_kit::{Capabilities, Fence, Filter};

#[test]
fn actor_factory_uses_supplied_backend_with_independent_stable_agent_cache() {
    let original_backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let admitted_backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let original =
        DriverSessionStores::new(original_backend, Scope::new("local").unwrap())
            .unwrap();
    let old = original.for_agent("alice~planner");
    old.turn_states
        .put(&TurnState::started(
            "old",
            "old-r",
            1,
            "2026-01-01T00:00:00Z",
        ))
        .unwrap();
    let actor = original
        .bind_actor(admitted_backend)
        .unwrap()
        .expect("driver providers support admitted actors");
    let fresh = actor.for_agent("alice~planner");
    assert!(fresh.turn_states.get("old").unwrap().is_none());
    fresh
        .turn_states
        .put(&TurnState::started("fresh", "r", 1, "2026-01-01T00:00:00Z"))
        .unwrap();
    assert!(old.turn_states.get("fresh").unwrap().is_none());
    assert!(Arc::ptr_eq(
        &fresh.turn_states,
        &actor.for_agent("alice~planner").turn_states
    ));
    assert!(!Arc::ptr_eq(&fresh.turn_states, &old.turn_states));
}

#[derive(Debug)]
struct AdmittedBackend {
    inner: Arc<dyn StorageBackend>,
    fence: Fence,
}
impl StorageBackend for AdmittedBackend {
    fn driver(&self) -> &'static str {
        self.inner.driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage, StorageError> {
        self.inner.for_scope_fenced(scope, &self.fence)
    }
}

fn admitted(inner: &Arc<dyn StorageBackend>, epoch: u64) -> Arc<dyn StorageBackend> {
    Arc::new(AdmittedBackend {
        inner: Arc::clone(inner),
        fence: Fence::new(
            Scope::new("cluster").unwrap(),
            "leases",
            "alice",
            Filter::eq("epoch", epoch),
        ),
    })
}

fn set_epoch(backend: &Arc<dyn StorageBackend>, epoch: u64) {
    let scoped = backend.for_scope(&Scope::new("cluster").unwrap()).unwrap();
    Blocking::new()
        .unwrap()
        .run(async move {
            scoped
                .documents()
                .put(
                    "leases",
                    "alice",
                    json!({"epoch": epoch}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await
        })
        .unwrap()
        .unwrap();
}

#[test]
fn fresh_actor_after_native_takeover_preserves_namespace_and_refuses_retained_old_handles() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    set_epoch(&inner, 1);
    let original = DriverSessionStores::new(
        admitted(&inner, 1),
        Scope::new("local").unwrap(),
    )
    .unwrap();
    let old = original.for_agent("alice~planner");
    let saved = TurnState::started("persisted", "r", 1, "2026-01-01T00:00:00Z");
    old.turn_states.put(&saved).unwrap();
    set_epoch(&inner, 2);
    let actor = original
        .bind_actor(admitted(&inner, 2))
        .unwrap()
        .expect("driver providers support admitted actors");
    let new = actor.for_agent("alice~planner");
    assert_eq!(new.turn_states.get("persisted").unwrap(), Some(saved));
    new.turn_states
        .put(&TurnState::started("new", "r", 1, "2026-01-01T00:00:00Z"))
        .unwrap();
    assert!(
        old.turn_states
            .put(&TurnState::started("stale", "r", 1, "2026-01-01T00:00:00Z"))
            .is_err()
    );
    assert!(new.turn_states.get("stale").unwrap().is_none());
    let session = crate::transcript::SessionRef::scoped("stable-thread", "alice~planner");
    let error = old
        .transcripts
        .begin_generation(&session, super::transcripts::tests::meta("stable-thread"))
        .err()
        .expect("retained transcript writes are fenced");
    assert_eq!(
        error.downcast_ref::<StorageError>().unwrap().kind(),
        tinystoragedrivers_types::ErrorKind::Fenced
    );
    assert!(
        new.transcripts
            .begin_generation(&session, super::transcripts::tests::meta("stable-thread"))
            .is_ok()
    );
}

#[test]
fn arbitrary_provider_reports_actor_factory_absence_without_fallback() {
    let provider = crate::port::InMemorySessionStores::default();
    let admitted: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    assert!(
        provider
            .bind_actor(Arc::clone(&admitted))
            .unwrap()
            .is_none()
    );
    let shared: Arc<dyn SessionStoreProvider> = Arc::new(provider);
    assert!(shared.bind_actor(admitted).unwrap().is_none());
}
