//! Checkpointer conformance for the lineage and pending-writes contracts.
//!
//! The pre-existing `tests/conformance.rs` covers put/get/list/delete_thread/
//! prune. These two suites cover what it did not — `get_tuple`,
//! `state_history`, `copy_thread`, `delete_checkpoints` and the whole
//! pending-writes protocol — against every bundled backend, so a defect in
//! any one of them cannot hide behind the others.

mod common;

use common::storage;
use tinyagents_graph::checkpoint::{DriverCheckpointer, InMemoryCheckpointer};
use tinyagents_graph::testkit::conformance::{
    checkpointer_lineage_contract, checkpointer_writes_contract,
};

#[tokio::test]
async fn in_memory_checkpointer_satisfies_the_writes_contract() {
    checkpointer_writes_contract(InMemoryCheckpointer::<i32>::new()).await;
}

#[tokio::test]
async fn in_memory_checkpointer_satisfies_the_lineage_contract() {
    checkpointer_lineage_contract(InMemoryCheckpointer::<i32>::new()).await;
}

#[tokio::test]
async fn memory_driver_checkpointer_satisfies_the_writes_contract() {
    checkpointer_writes_contract(DriverCheckpointer::<i32>::new(storage::docs(
        &storage::memory(),
    )))
    .await;
}

#[tokio::test]
async fn memory_driver_checkpointer_satisfies_the_lineage_contract() {
    checkpointer_lineage_contract(DriverCheckpointer::<i32>::new(storage::docs(
        &storage::memory(),
    )))
    .await;
}

#[tokio::test]
async fn sqlite_driver_checkpointer_satisfies_the_writes_contract() {
    let dir = tempfile::tempdir().unwrap();
    checkpointer_writes_contract(storage::checkpointer::<i32>(dir.path())).await;
}

#[tokio::test]
async fn sqlite_driver_checkpointer_satisfies_the_lineage_contract() {
    let dir = tempfile::tempdir().unwrap();
    checkpointer_lineage_contract(storage::checkpointer::<i32>(dir.path())).await;
}
