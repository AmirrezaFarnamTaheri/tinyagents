//! Opt-in full session-store and run-ledger conformance against a real local
//! MongoDB replica set. This excluded workspace keeps BSON's serde_json feature
//! unification out of the ordinary TinyAgents workspace graph.
#![cfg(feature = "mongodb-storage-tests")]

use std::sync::Arc;

use tinyagents_session::{
    port::{AgentStorageFactory, ConditionedAgentStorageFactory, DriverSessionStores},
    run_ledger::{AgentRunListRequest, DriverRunLedger, RunLedgerStore},
    testkit::conformance::{
        run_ledger_conformance, session_store_conformance, session_store_isolation_conformance,
    },
};
use tinyagents_storage_kit::{Scope, ScopedStorage, WriteCondition};
use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend, StreamStore};
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_document::mongodb::{MongoDocuments, MongoStreams};
use tinystoragedrivers_testkit::MemoryBackend;

/// Require an explicit, credential-free loopback URL for the local fixture.
fn local_mongo_url() -> String {
    let value = std::env::var("TSD_MONGO_URL")
        .expect("set TSD_MONGO_URL to run ignored MongoDB conformance tests");
    let url = reqwest::Url::parse(&value).expect("TSD_MONGO_URL must be a valid URI");
    assert_eq!(url.scheme(), "mongodb", "MongoDB SRV URLs are not accepted");
    assert!(
        matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1")) && url.port().is_some(),
        "TSD_MONGO_URL must point to a local MongoDB server with an explicit port"
    );
    assert!(
        url.username().is_empty() && url.password().is_none(),
        "credentials are not accepted for local conformance tests"
    );
    value
}

async fn local_connection(label: &str) -> MongoConnection {
    let url = local_mongo_url();
    let database = format!("ta_{label}_{}", uuid::Uuid::new_v4().simple());
    MongoConnection::connect(&url, &database)
        .await
        .expect("local MongoDB replica set is reachable")
}

fn scoped_storage(
    connection: &MongoConnection,
    scope: &Scope,
    condition: Option<&WriteCondition>,
) -> Result<ScopedStorage, tinystoragedrivers_types::StorageError> {
    let documents = match condition {
        Some(condition) => Arc::new(MongoDocuments::conditioned(
            connection.clone(),
            scope.clone(),
            condition.clone(),
        )?) as Arc<dyn DocumentStore>,
        None => Arc::new(MongoDocuments::new(connection.clone(), scope.clone()))
            as Arc<dyn DocumentStore>,
    };
    let streams =
        Arc::new(MongoStreams::new(connection.clone(), scope.clone())) as Arc<dyn StreamStore>;
    let memory = MemoryBackend::new().for_scope(scope).unwrap();
    Ok(ScopedStorage::new(
        Storage::new(
            documents,
            streams,
            memory.kv().clone(),
            memory.blobs().clone(),
            memory.queues().clone(),
            memory.secrets().clone(),
        ),
        scope.clone(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local MongoDB; run explicitly with TSD_MONGO_URL and --ignored"]
async fn mongodb_session_store_round_trip_list_delete_and_agent_isolation() {
    let scope = Scope::new("session_store_conformance").unwrap();
    let connection = local_connection("session_store").await;
    let factory_scope = scope.clone();
    let factory_connection = connection.clone();
    let factory: AgentStorageFactory = Arc::new(move |agent_id| {
        let agent_scope = factory_scope.child(agent_id)?;
        scoped_storage(&factory_connection, &agent_scope, None)
    });
    let conditioned_scope = scope.clone();
    let conditioned_connection = connection.clone();
    let conditioned: ConditionedAgentStorageFactory = Arc::new(move |agent_id, condition| {
        let agent_scope = conditioned_scope.child(agent_id)?;
        scoped_storage(&conditioned_connection, &agent_scope, Some(condition))
    });
    let stores = DriverSessionStores::new(factory)
        .unwrap()
        .with_conditioned_storage_factory(conditioned);
    session_store_conformance(&stores).await;
    session_store_isolation_conformance(&stores).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local MongoDB; run explicitly with TSD_MONGO_URL and --ignored"]
async fn mongodb_run_ledger_contract_and_scope_isolation() {
    let scope = Scope::new("run_ledger_conformance").unwrap();
    let connection = local_connection("run_ledger").await;
    let storage = scoped_storage(&connection, &scope, None).unwrap();
    let ledger = Arc::new(DriverRunLedger::new(storage)) as Arc<dyn RunLedgerStore>;
    run_ledger_conformance(ledger.clone()).await;
    let peer_scope = Scope::new("run_ledger_peer").unwrap();
    let peer_storage = scoped_storage(&connection, &peer_scope, None).unwrap();
    let peer = Arc::new(DriverRunLedger::new(peer_storage)) as Arc<dyn RunLedgerStore>;
    assert!(
        peer.list_agent_runs(&AgentRunListRequest::default())
            .await
            .unwrap()
            .runs
            .is_empty()
    );
}
