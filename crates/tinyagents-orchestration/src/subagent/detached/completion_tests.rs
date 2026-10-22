use std::sync::Arc;
use std::time::Duration;

use tinyagents_harness::run_queue::{QueueLane, RunQueue, RunQueueHandle};
use tinyagents_tasks::{
    CompletionRouter, CompletionStatus, InMemoryCompletionStore, InMemoryTaskStore, NotifyMode,
    OrchestrationTaskStatus, TaskStore,
};
use tinyinference_llm::message::Message;
use tokio::sync::watch;

use super::*;

fn router() -> Arc<CompletionRouter> {
    Arc::new(CompletionRouter::new(Arc::new(
        InMemoryCompletionStore::new(),
    )))
}

fn target(router: &Arc<CompletionRouter>) -> DetachedCompletionTarget {
    DetachedCompletionTarget::try_new(router.clone(), "p", "worker")
        .expect("test parent starts open")
        .with_label("indexer")
}

async fn settle(router: &CompletionRouter, parent: &str, task_id: &str) {
    for _ in 0..100 {
        if router
            .pending_for(parent)
            .iter()
            .any(|record| record.task_id == task_id)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spawned_store(task: &str) -> Arc<dyn TaskStore> {
    let store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());
    record_spawned(
        store.as_ref(),
        &SpawnedSubagent {
            task_id: task,
            agent_id: "worker",
            parent_session: "p",
            session_parent_prefix: None,
            subagent_session_id: None,
            workspace_dir: "/w",
            parent_thread_id: None,
        },
    )
    .unwrap();
    store
}

#[test]
fn only_final_statuses_are_completions() {
    let router = router();
    let target = target(&router);
    assert!(
        target
            .record_for("t", &DetachedSubagentStatus::Running)
            .is_none()
    );
    assert!(
        target
            .record_for(
                "t",
                &DetachedSubagentStatus::AwaitingUser {
                    question: "which?".into()
                }
            )
            .is_none()
    );
    let done = target
        .record_for(
            "t",
            &DetachedSubagentStatus::Completed {
                output: "ok".into(),
                iterations: 2,
            },
        )
        .unwrap();
    assert_eq!(done.status, CompletionStatus::Success);
    assert_eq!(done.label.as_deref(), Some("indexer"));
    let failed = target
        .record_for(
            "t",
            &DetachedSubagentStatus::Failed {
                error: "bad".into(),
            },
        )
        .unwrap();
    assert_eq!(failed.status, CompletionStatus::Failed);
    assert_eq!(failed.result.text, "bad");
}

#[tokio::test]
async fn pause_notices_use_independent_ids_and_leave_terminal_task_id_free() {
    let router = router();
    let target = target(&router);
    let first = target.pause_record_for("task:1", "question one");
    let second = target.pause_record_for("task:1", "question two");
    let prefix = pause_record_prefix("task:1");
    assert!(first.task_id.starts_with(&prefix));
    assert!(second.task_id.starts_with(&prefix));
    assert_ne!(first.task_id, second.task_id);
    assert_eq!(first.status, CompletionStatus::Incomplete);
    assert_eq!(first.parent_key, "p");
    assert_eq!(first.result.text, "question one");

    assert!(target.record_pause("task:1", "question one").await);
    assert!(
        record_detached_completion(
            &target,
            "task:1",
            &DetachedSubagentStatus::Completed {
                output: "done".into(),
                iterations: 1,
            },
        )
        .await
    );
    let pending = router.pending_for("p");
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().any(|record| record.task_id == "task:1"));
    assert!(
        pending
            .iter()
            .any(|record| record.task_id.starts_with(&prefix))
    );
}

#[tokio::test]
async fn the_watcher_mirrors_the_status_and_records_the_completion() {
    let router = router();
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(store.clone(), "t1".into(), rx, target(&router));
    tx.send(DetachedSubagentStatus::Completed {
        output: "found it".into(),
        iterations: 1,
    })
    .unwrap();
    settle(&router, "p", "t1").await;
    let pending = router.pending_for("p");
    assert_eq!(pending[0].result.text, "found it");
    let record = store.get(&"t1".into()).unwrap();
    assert_eq!(record.status, OrchestrationTaskStatus::Completed);
}

#[tokio::test]
async fn a_dropped_sender_is_recorded_as_a_failure() {
    let router = router();
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(store, "t1".into(), rx, target(&router));
    drop(tx);
    settle(&router, "p", "t1").await;
    assert_eq!(router.pending_for("p")[0].status, CompletionStatus::Failed);
}

#[tokio::test]
async fn the_watcher_pushes_to_a_live_parent_by_notify_mode() {
    let router = router();
    let queue: RunQueueHandle = Arc::new(RunQueue::<Message>::new());
    router.attach_parent("p", queue.clone());
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(
        store,
        "t1".into(),
        rx,
        target(&router).with_notify_mode(NotifyMode::Collect),
    );
    tx.send(DetachedSubagentStatus::Completed {
        output: "x".into(),
        iterations: 1,
    })
    .unwrap();
    for _ in 0..100 {
        if queue.status().await.total > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let admitted = queue
        .take_admitted(
            QueueLane::Collect,
            tinyagents_harness::run_queue::QueueMode::All,
        )
        .await;
    assert_eq!(admitted.len(), 1);
}

#[tokio::test]
async fn a_paused_child_records_a_separate_notice_not_its_terminal_task_id() {
    let router = router();
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(store, "t1".into(), rx, target(&router));
    tx.send(DetachedSubagentStatus::AwaitingUser {
        question: "which?".into(),
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let pauses = router.pending_for("p");
    assert_eq!(pauses.len(), 1);
    assert!(pauses[0].task_id.starts_with(&pause_record_prefix("t1")));
    assert_eq!(pauses[0].result.text, "which?");
}

#[tokio::test]
async fn a_cancelled_child_is_not_pushed_back_to_its_parent() {
    let router = router();
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    record_cancelled(store.as_ref(), "t1").unwrap();
    spawn_status_watcher_with_completions(store, "t1".into(), rx, target(&router));
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(router.pending_for("p").is_empty());
}

#[tokio::test]
async fn a_resumed_child_is_recorded_when_it_finally_finishes() {
    let router = router();
    let store = spawned_store("t1");
    let (tx, rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(store, "t1".into(), rx, target(&router));
    tx.send(DetachedSubagentStatus::AwaitingUser {
        question: "which?".into(),
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(router.pending_for("p").len(), 1);
    tx.send(DetachedSubagentStatus::Completed {
        output: "answered".into(),
        iterations: 2,
    })
    .unwrap();
    settle(&router, "p", "t1").await;
    let pending = router.pending_for("p");
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .any(|record| record.result.text == "answered")
    );
}

#[tokio::test]
async fn watcher_targets_keep_the_admitted_parent_generation() {
    let router = router();
    let old_target = DetachedCompletionTarget::try_new(router.clone(), "p", "worker").unwrap();
    let old_epoch = old_target.parent_epoch;
    router.cancel_parent("p").unwrap();
    router.resume_parent("p");

    assert!(old_target.record_pause("late-pause", "old question").await);
    let old_store = spawned_store("old-child");
    let (old_tx, old_rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(old_store, "old-child".into(), old_rx, old_target);
    old_tx
        .send(DetachedSubagentStatus::Completed {
            output: "old answer".into(),
            iterations: 1,
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(router.pending_for("p").is_empty());

    let new_target = DetachedCompletionTarget::try_new(router.clone(), "p", "worker").unwrap();
    assert!(new_target.parent_epoch > old_epoch);
    let new_epoch = new_target.parent_epoch;
    let new_store = spawned_store("new-child");
    let (new_tx, new_rx) = watch::channel(DetachedSubagentStatus::Running);
    spawn_status_watcher_with_completions(new_store, "new-child".into(), new_rx, new_target);
    new_tx
        .send(DetachedSubagentStatus::AwaitingUser {
            question: "new question".into(),
        })
        .unwrap();
    let mut saw_pause = false;
    for _ in 0..100 {
        if router.pending_for("p").iter().any(|record| {
            record
                .task_id
                .starts_with(&pause_record_prefix("new-child"))
        }) {
            saw_pause = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        saw_pause,
        "watcher must persist the pause before it resumes"
    );
    new_tx
        .send(DetachedSubagentStatus::Completed {
            output: "new answer".into(),
            iterations: 1,
        })
        .unwrap();
    for _ in 0..100 {
        if router
            .pending_for("p")
            .iter()
            .any(|record| record.task_id == "new-child")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = router.pending_for("p");
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .all(|record| record.parent_epoch == new_epoch)
    );
    assert!(
        pending
            .iter()
            .any(|record| record.result.text == "new question")
    );
    assert!(pending.iter().any(|record| record.task_id == "new-child"));
}
