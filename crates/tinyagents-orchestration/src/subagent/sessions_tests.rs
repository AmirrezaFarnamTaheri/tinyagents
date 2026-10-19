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
            .close_if_current(
                &second.subagent_session_id,
                "task-a",
                &first_key,
                "parent-session",
                Some("thread"),
            )
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
            .close_if_current(
                &second.subagent_session_id,
                "task-b",
                &second_key,
                "parent-session",
                Some("thread"),
            )
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
    let mut foreign = upsert("task", key("task"));
    foreign.selector.parent_session = "other-session".into();
    assert!(scoped.upsert_running(foreign).await.is_err());
}

#[test]
fn durable_session_decodes_legacy_transcript_message_history() {
    let value = serde_json::json!({
        "subagentSessionId": "session",
        "parentSession": "parent",
        "parentThreadId": "thread",
        "workerThreadId": null,
        "agentId": "worker",
        "displayName": null,
        "model": null,
        "sandboxMode": "none",
        "actionRoot": null,
        "taskKey": "research",
        "taskTitle": "research",
        "currentTaskId": "task",
        "status": "awaiting_user",
        "reusable": true,
        "latestHistory": [
            { "role": "user", "content": "need help" },
            {
                "role": "assistant",
                "content": "checking",
                "tool_calls": [{ "id": "call-1", "name": "lookup", "arguments": "{\"q\":\"x\"}" }]
            },
            { "role": "tool", "content": "found", "tool_call_id": "call-1" }
        ],
        "latestError": null,
        "createdAt": "now",
        "updatedAt": "now",
        "lastUsedAt": "now"
    });

    let session: DurableSubagentSession = serde_json::from_value(value).unwrap();
    let history = session.latest_history.unwrap();
    assert_eq!(history[0], Message::user("need help"));
    assert_eq!(history[1].text(), "checking");
    assert_eq!(history[2], Message::tool("call-1", "found"));
    let Message::Assistant(assistant) = &history[1] else {
        panic!("legacy assistant row must remain an assistant message")
    };
    assert_eq!(assistant.tool_calls.len(), 1);
    assert_eq!(assistant.tool_calls[0].name, "lookup");
}

#[derive(Clone)]
struct PauseAfterSessionRead {
    inner: Arc<DriverSubagentSessionStore>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    pause_once: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl SubagentSessionStore for PauseAfterSessionRead {
    async fn list_for_parent(
        &self,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<Vec<DurableSubagentSession>, SubagentError> {
        self.inner
            .list_for_parent(parent_session, parent_thread_id)
            .await
    }

    async fn get(&self, id: &str) -> Result<Option<DurableSubagentSession>, SubagentError> {
        let session = self.inner.get(id).await?;
        if self
            .pause_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(session)
    }

    async fn upsert_running(
        &self,
        upsert: SubagentSessionUpsert,
    ) -> Result<(DurableSubagentSession, ReuseDecision), SubagentError> {
        self.inner.upsert_running(upsert).await
    }

    async fn close(&self, id: &str) -> Result<bool, SubagentError> {
        self.inner.close(id).await
    }

    async fn close_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
    ) -> Result<bool, SubagentError> {
        self.inner
            .close_if_current(id, task_id, lifecycle_key, parent_session, parent_thread_id)
            .await
    }

    async fn mark_finished(
        &self,
        id: &str,
        task_id: &str,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        self.inner.mark_finished(id, task_id, status, history).await
    }

    async fn mark_finished_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        status: DurableSubagentStatus,
        history: Vec<Message>,
    ) -> Result<bool, SubagentError> {
        self.inner
            .mark_finished_if_current(
                id,
                task_id,
                lifecycle_key,
                parent_session,
                parent_thread_id,
                status,
                history,
            )
            .await
    }

    async fn mark_failed(
        &self,
        id: &str,
        task_id: &str,
        error: String,
    ) -> Result<bool, SubagentError> {
        self.inner.mark_failed(id, task_id, error).await
    }

    async fn mark_failed_if_current(
        &self,
        id: &str,
        task_id: &str,
        lifecycle_key: &SubagentTaskKey,
        parent_session: &str,
        parent_thread_id: Option<&str>,
        error: String,
    ) -> Result<bool, SubagentError> {
        self.inner
            .mark_failed_if_current(
                id,
                task_id,
                lifecycle_key,
                parent_session,
                parent_thread_id,
                error,
            )
            .await
    }
}

#[tokio::test]
async fn scoped_mutations_recheck_reused_task_owner_after_a_paused_read() {
    for mutation in ["close", "finish", "fail"] {
        let backend = MemoryStorage::new();
        let docs = Arc::clone(backend.for_scope(&Scope::local()).unwrap().documents());
        let driver = Arc::new(DriverSubagentSessionStore::new(docs).unwrap());
        let old_key = key("same-task");
        let (old, _) = driver
            .upsert_running(upsert("same-task", old_key.clone()))
            .await
            .unwrap();
        let barrier = PauseAfterSessionRead {
            inner: Arc::clone(&driver),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            pause_once: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        let scoped = Arc::new(ScopedSubagentSessionStore::new(
            Arc::new(barrier.clone()),
            "parent-session",
            Some("thread".into()),
        ));
        let waiting_for_read = barrier.entered.notified();
        let pending = {
            let scoped = Arc::clone(&scoped);
            let id = old.subagent_session_id.clone();
            let old_key = old_key.clone();
            tokio::spawn(async move {
                match mutation {
                    "close" => scoped.close(&id).await,
                    "finish" => {
                        scoped
                            .mark_finished(
                                &id,
                                "same-task",
                                DurableSubagentStatus::Idle,
                                vec![Message::user("stale")],
                            )
                            .await
                    }
                    "fail" => scoped.mark_failed(&id, "same-task", "stale".into()).await,
                    _ => unreachable!(),
                }
            })
        };
        waiting_for_read.await;

        let new_key = SubagentTaskKey {
            root_run_id: "replacement-root".into(),
            parent_run_id: "replacement-parent".into(),
            thread_id: Some("thread".into()),
            task_id: "same-task".into(),
        };
        let (replacement, _) = driver
            .upsert_running(upsert("same-task", new_key.clone()))
            .await
            .unwrap();
        assert_eq!(replacement.subagent_session_id, old.subagent_session_id);
        barrier.release.notify_one();
        assert!(
            !pending.await.unwrap().unwrap(),
            "stale {mutation} must be rejected"
        );

        assert!(match mutation {
            "close" => driver
                .close_if_current(
                    &replacement.subagent_session_id,
                    "same-task",
                    &new_key,
                    "parent-session",
                    Some("thread"),
                )
                .await
                .unwrap(),
            "finish" => driver
                .mark_finished_if_current(
                    &replacement.subagent_session_id,
                    "same-task",
                    &new_key,
                    "parent-session",
                    Some("thread"),
                    DurableSubagentStatus::Idle,
                    vec![Message::user("current")],
                )
                .await
                .unwrap(),
            "fail" => driver
                .mark_failed_if_current(
                    &replacement.subagent_session_id,
                    "same-task",
                    &new_key,
                    "parent-session",
                    Some("thread"),
                    "current failure".into(),
                )
                .await
                .unwrap(),
            _ => unreachable!(),
        });
    }
}
