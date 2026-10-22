//! Harness store conformance: the same contract suite applied to every
//! built-in `Store` / `NamespacedStore` backend, proving they behave
//! interchangeably (gap #17, harness half — see `tests/conformance.rs` and
//! `tests/persistence_conformance.rs` for the graph checkpointer/task-store
//! side and `tests/session_conformance.rs` for the session side).

use tinyagents_harness::store::conformance::{
    run_namespaced_store_conformance, run_store_conformance,
};
use tinyagents_harness::store::namespaced::{DriverNamespacedStore, InMemoryNamespacedStore};
use tinyagents_harness::store::{DriverStore, InMemoryStore};

mod common;
use common::storage;

#[tokio::test]
async fn in_memory_store_satisfies_the_conformance_suite() {
    run_store_conformance(&InMemoryStore::new()).await;
}

#[tokio::test]
async fn memory_driver_store_satisfies_the_conformance_suite() {
    run_store_conformance(&DriverStore::new(storage::docs(&storage::memory()))).await;
}

#[tokio::test]
async fn sqlite_driver_store_satisfies_the_conformance_suite() {
    let dir = tempfile::tempdir().unwrap();
    let docs = storage::docs(&storage::sqlite(dir.path(), "kv.sqlite"));
    run_store_conformance(&DriverStore::new(docs)).await;
}

#[tokio::test]
async fn in_memory_namespaced_store_satisfies_the_conformance_suite() {
    run_namespaced_store_conformance(&InMemoryNamespacedStore::new()).await;
}

#[tokio::test]
async fn driver_namespaced_stores_satisfy_the_conformance_suite() {
    run_namespaced_store_conformance(&DriverNamespacedStore::new(storage::docs(
        &storage::memory(),
    )))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let docs = storage::docs(&storage::sqlite(dir.path(), "ns.sqlite"));
    run_namespaced_store_conformance(&DriverNamespacedStore::new(docs)).await;
}
