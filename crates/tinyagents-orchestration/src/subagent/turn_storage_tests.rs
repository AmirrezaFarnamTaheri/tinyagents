use super::*;

use serde_json::json;
use tinyagents_harness::store::SchemaAdmission;
use tinyagents_session::port::{DriverSessionStores, SessionStoreProvider};
use tinyagents_session::transcript::{
    SessionRef, TranscriptHistory, TranscriptMessage, TranscriptMeta, TranscriptTurn,
};
use tinyagents_tasks::DurableThreadDeliveryLeases;
use tinystoragedrivers_core::{Filter, MemoryStorage, Precondition, Scope, StorageBackend};

fn meta(thread_id: &str) -> TranscriptMeta {
    TranscriptMeta {
        agent_name: "planner".into(),
        agent_id: Some("planner".into()),
        agent_type: Some("root".into()),
        dispatcher: "native".into(),
        provider: None,
        model: None,
        created: "2026-01-01T00:00:00Z".into(),
        updated: "2026-01-01T00:00:00Z".into(),
        turn_count: 0,
        prefix_message_count: None,
        input_tokens: 0,
        output_tokens: 0,
        cached_input_tokens: 0,
        charged_amount_usd: 0.0,
        thread_id: Some(thread_id.into()),
        task_id: None,
        session_id: None,
        parent_session_id: None,
    }
}

fn append(
    history: &dyn TranscriptHistory,
    prev: &[TranscriptMessage],
    next: &[TranscriptMessage],
    metadata: &TranscriptMeta,
) -> anyhow::Result<()> {
    history.append_turn(TranscriptTurn {
        prev,
        next,
        meta: metadata,
        turn_usage: None,
        request_id: None,
        tools: None,
    })
}

#[test]
fn fenced_session_backend_admits_only_typed_tenant_and_agent_scopes() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let tenant = Scope::tenant("alice").unwrap();
    let cluster = Scope::cluster();
    let lease_docs = Arc::clone(backend.for_scope(&cluster).unwrap().documents());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            lease_docs
                .put(
                    "tsd.leases",
                    "turn-lease",
                    json!({"holder":"a", "epoch":1}),
                    Precondition::Absent,
                )
                .await
                .unwrap();
        });
    let fence = tinystoragedrivers_core::Fence::new(
        cluster,
        "tsd.leases",
        "turn-lease",
        Filter::eq("epoch", 1),
    );
    let scoped = FencedSessionBackend::new(backend, tenant.clone(), fence).unwrap();

    assert!(scoped.for_scope(&tenant).is_ok());
    assert!(scoped.for_scope(&tenant.agent("worker").unwrap()).is_ok());
    assert!(
        scoped
            .for_scope(
                &Scope::tenant("alice-other")
                    .unwrap()
                    .agent("worker")
                    .unwrap()
            )
            .is_err()
    );
    assert!(scoped.for_scope(&tenant.module("module").unwrap()).is_err());
    assert!(scoped.for_scope(&Scope::cluster()).is_err());
    // Legacy strings that merely share a textual prefix do not inherit the
    // typed tenant's authority.
    assert!(
        scoped
            .for_scope(&Scope::new("u:alice-evil").unwrap())
            .is_err()
    );
}

#[tokio::test]
async fn takeover_fences_a_retained_driver_session_transcript_writer() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    let tenant = Scope::tenant("writer-test").unwrap();
    let bootstrap = backend.for_scope(&tenant).unwrap();
    DriverSessionStores::prepare_schema(bootstrap.documents().as_ref())
        .await
        .unwrap();
    let provider = DriverSessionStores::new(Arc::clone(&backend), tenant.clone()).unwrap();
    let leases_a = DurableThreadDeliveryLeases::new(
        backend.as_ref(),
        Scope::cluster(),
        "writer-test-domain",
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let leases_b = DurableThreadDeliveryLeases::new(
        backend.as_ref(),
        Scope::cluster(),
        "writer-test-domain",
        std::time::Duration::from_secs(30),
    )
    .unwrap();

    let lease_a = leases_a
        .try_acquire(&tenant, "thread-1")
        .await
        .unwrap()
        .unwrap();
    let actor_a = provider
        .bind_actor_with_schema_admission(
            Arc::new(
                FencedSessionBackend::new(Arc::clone(&backend), tenant.clone(), lease_a.fence())
                    .unwrap(),
            ),
            SchemaAdmission::ValidateOnly,
        )
        .unwrap()
        .unwrap();
    let stores_a = actor_a.try_for_agent("planner").unwrap();
    let session = SessionRef::root("thread-1");
    let metadata = meta("thread-1");
    let history_a = stores_a
        .transcripts
        .open_session(&session, metadata.clone())
        .unwrap();
    let first = vec![TranscriptMessage::new("assistant", "owner A")];
    append(history_a.as_ref(), &[], &first, &metadata).unwrap();

    assert!(
        leases_b
            .try_acquire(&tenant, "thread-1")
            .await
            .unwrap()
            .is_none()
    );
    let epoch_a = lease_a.epoch();
    assert!(lease_a.release().await.unwrap());
    let lease_b = leases_b
        .try_acquire(&tenant, "thread-1")
        .await
        .unwrap()
        .unwrap();
    assert!(lease_b.epoch() > epoch_a);
    let actor_b = provider
        .bind_actor_with_schema_admission(
            Arc::new(
                FencedSessionBackend::new(Arc::clone(&backend), tenant, lease_b.fence()).unwrap(),
            ),
            SchemaAdmission::ValidateOnly,
        )
        .unwrap()
        .unwrap();
    let stores_b = actor_b.try_for_agent("planner").unwrap();
    let history_b = stores_b
        .transcripts
        .open_session(&session, metadata.clone())
        .unwrap();
    let second = vec![
        TranscriptMessage::new("assistant", "owner A"),
        TranscriptMessage::new("assistant", "owner B"),
    ];
    append(history_b.as_ref(), &first, &second, &metadata).unwrap();

    let stale = vec![
        TranscriptMessage::new("assistant", "owner A"),
        TranscriptMessage::new("assistant", "stale A write"),
    ];
    assert!(append(history_a.as_ref(), &first, &stale, &metadata).is_err());
    let transcript = history_b.read_session().unwrap().unwrap();
    assert_eq!(transcript.messages, second);
    assert!(lease_b.release().await.unwrap());
}
