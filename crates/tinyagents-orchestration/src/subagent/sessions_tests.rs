use super::*;
use std::sync::Arc;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn key(task_id: &str) -> SubagentTaskKey {
    SubagentTaskKey {
        root_run_id: "root".into(),
        parent_run_id: "parent".into(),
        thread_id: Some("thread".into()),
        task_id: task_id.into(),
    }
}

fn upsert(task_id: &str, lifecycle_key: SubagentTaskKey) -> SubagentSessionUpsert {
    SubagentSessionUpsert {
        selector: SubagentSessionSelector {
            parent_session: "parent-session".into(),
            parent_thread_id: Some("thread".into()),
            agent_id: "worker".into(),
            model: None,
            sandbox_mode: "none".into(),
            action_root: None,
            task_key: "research".into(),
        },
        lifecycle_key,
        display_name: None,
        task_title: "research".into(),
        worker_thread_id: None,
        task_id: task_id.into(),
        force_fresh: false,
    }
}

#[tokio::test]
async fn stale_session_owner_cannot_close_a_reused_lifecycle() {
    let backend = MemoryStorage::new();
    let docs = Arc::clone(backend.for_scope(&Scope::local()).unwrap().documents());
    let store = DriverSubagentSessionStore::new(docs).unwrap();
    let first_key = key("task-a");
    let (first, _) = store
        .upsert_running(upsert("task-a", first_key.clone()))
        .await
        .unwrap();
    let second_key = key("task-b");
    let (second, _) = store
        .upsert_running(upsert("task-b", second_key.clone()))
        .await
        .unwrap();
    assert_eq!(first.subagent_session_id, second.subagent_session_id);

    assert!(
        !store
            .close_if_current(&second.subagent_session_id, "task-a", &first_key)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .get(&second.subagent_session_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        DurableSubagentStatus::Running
    );
    assert!(
        store
            .close_if_current(&second.subagent_session_id, "task-b", &second_key)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn scoped_session_store_rejects_other_parent_identity() {
    let backend = MemoryStorage::new();
    let docs = Arc::clone(backend.for_scope(&Scope::local()).unwrap().documents());
    let inner = Arc::new(DriverSubagentSessionStore::new(docs).unwrap());
    let scoped =
        ScopedSubagentSessionStore::new(inner, "parent-session", Some("thread".to_owned()));
    assert!(
        scoped
            .list_for_parent("other-session", Some("thread"))
            .await
            .is_err()
    );
    assert!(
        scoped
            .list_for_parent("parent-session", None)
            .await
            .is_err()
    );
    assert!(scoped.get("not-owned").await.unwrap().is_none());
}
