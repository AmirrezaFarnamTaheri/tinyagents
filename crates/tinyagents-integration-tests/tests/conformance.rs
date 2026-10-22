//! Storage conformance: the same contract suite applied to every built-in
//! backend, proving they behave interchangeably (gap #17).

mod common;

use common::storage;
use tinyagents_graph::checkpoint::{DriverCheckpointer, InMemoryCheckpointer};
use tinyagents_graph::orchestration::{DriverTaskStore, InMemoryTaskStore};
use tinyagents_graph::testkit::conformance::{
    checkpointer_concurrent_contract, checkpointer_contract, task_cache_contract,
    taskstore_concurrent_contract, taskstore_contract, taskstore_replay_contract,
};
use tinyagents_graph::{DriverTaskCache, InMemoryTaskCache};

#[tokio::test]
async fn in_memory_checkpointer_satisfies_contract() {
    checkpointer_contract(InMemoryCheckpointer::<i32>::new()).await;
}

#[tokio::test]
async fn memory_driver_checkpointer_satisfies_contract() {
    checkpointer_contract(DriverCheckpointer::<i32>::new(storage::docs(
        &storage::memory(),
    )))
    .await;
}

#[tokio::test]
async fn sqlite_driver_checkpointer_satisfies_contract() {
    let dir = tempfile::tempdir().unwrap();
    checkpointer_contract(storage::checkpointer::<i32>(dir.path())).await;
}

#[test]
fn in_memory_task_store_satisfies_contract() {
    taskstore_contract(InMemoryTaskStore::new());
}

#[test]
fn memory_driver_task_store_satisfies_contract() {
    taskstore_contract(DriverTaskStore::new(storage::docs(&storage::memory())).unwrap());
}

#[test]
fn sqlite_driver_task_store_satisfies_contract() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        DriverTaskStore::new(storage::docs(&storage::sqlite(dir.path(), "tasks.sqlite"))).unwrap();
    taskstore_contract(store);
}

#[tokio::test]
async fn in_memory_checkpointer_handles_concurrent_writes() {
    checkpointer_concurrent_contract(std::sync::Arc::new(InMemoryCheckpointer::<i32>::new())).await;
}

#[tokio::test]
async fn sqlite_driver_checkpointer_handles_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    checkpointer_concurrent_contract(std::sync::Arc::new(storage::checkpointer::<i32>(
        dir.path(),
    )))
    .await;
}

#[test]
fn in_memory_task_store_handles_concurrent_writes() {
    taskstore_concurrent_contract(std::sync::Arc::new(InMemoryTaskStore::new()));
}

#[test]
fn sqlite_driver_task_store_handles_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        DriverTaskStore::new(storage::docs(&storage::sqlite(dir.path(), "tasks.sqlite"))).unwrap();
    taskstore_concurrent_contract(std::sync::Arc::new(store));
}

#[test]
fn sqlite_driver_task_store_replays_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    taskstore_replay_contract(|| {
        DriverTaskStore::new(storage::docs(&storage::sqlite(dir.path(), "tasks.sqlite"))).unwrap()
    });
}

#[tokio::test]
async fn task_caches_satisfy_the_contract() {
    task_cache_contract(InMemoryTaskCache::new()).await;
    task_cache_contract(DriverTaskCache::new(storage::docs(&storage::memory()))).await;
    let dir = tempfile::tempdir().unwrap();
    task_cache_contract(DriverTaskCache::new(storage::docs(&storage::sqlite(
        dir.path(),
        "cache.sqlite",
    ))))
    .await;
}
