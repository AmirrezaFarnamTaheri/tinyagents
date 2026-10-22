use super::*;
use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::ids::TaskId;

#[tokio::test]
async fn a_bad_id_in_a_batch_wait_prunes_nothing() {
    let registry = DetachedTaskRegistry::new(SteeringRegistry::new(), 8, |status: &bool| *status);
    let done = TaskId::new("done");
    let (_status_tx, status_rx) = tokio::sync::watch::channel(true);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            done.clone(),
            "owner",
            (),
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();
    let error = registry
        .wait_many(
            &[done.clone(), TaskId::new("missing")],
            "owner",
            Some(std::time::Duration::ZERO),
        )
        .await
        .unwrap_err();
    assert_eq!(error.task_id, TaskId::new("missing"));
    // The finished task's result was not handed back, so it must still be
    // there for the next wait.
    assert_eq!(registry.len().unwrap(), 1);
    // A repeated id gets the same result instead of an error after the first
    // wait pruned it.
    let items = registry
        .wait_many(
            &[done.clone(), done],
            "owner",
            Some(std::time::Duration::ZERO),
        )
        .await
        .unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], items[1]);
    task.abort();
}

#[tokio::test]
async fn hard_cancel_notifies_observer_before_aborting_live_future() {
    let (events, observed) = std::sync::mpsc::channel();
    let registry = DetachedTaskRegistry::new(SteeringRegistry::new(), 8, |status: &bool| *status)
        .with_abort_observer(move |event| events.send(event).unwrap());
    let task_id = TaskId::new("abort-me");
    let (status_tx, status_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            task_id.clone(),
            "owner",
            "worker".to_string(),
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();

    let cancelled = registry.cancel(&task_id, "owner").unwrap();
    let event = observed.recv().unwrap();
    assert_eq!(event.task_id, task_id);
    assert_eq!(event.owner_id, "owner");
    assert_eq!(event.metadata, "worker");
    assert!(!event.status);
    assert!(!cancelled.status);
    drop(status_tx);
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn hard_cancel_does_not_emit_abort_for_an_already_terminal_task() {
    let (events, observed) = std::sync::mpsc::channel();
    let registry = DetachedTaskRegistry::new(SteeringRegistry::new(), 8, |status: &bool| *status)
        .with_abort_observer(move |event| events.send(event).unwrap());
    let task_id = TaskId::new("already-done");
    let (status_tx, status_rx) = tokio::sync::watch::channel(true);
    let task = tokio::spawn(std::future::pending::<()>());
    registry
        .register(
            task_id.clone(),
            "owner",
            (),
            status_rx,
            CancellationToken::new(),
            task.abort_handle(),
        )
        .unwrap();
    registry.cancel(&task_id, "owner").unwrap();
    assert!(observed.try_recv().is_err());
    drop(status_tx);
    assert!(task.await.unwrap_err().is_cancelled());
}
