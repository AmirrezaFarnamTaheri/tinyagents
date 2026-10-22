use super::*;
use tinyagents_harness::cancel::CancellationToken;
use tinyagents_harness::ids::TaskId;

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

#[tokio::test]
async fn a_task_whose_status_sender_is_gone_is_settled() {
    let registry = DetachedTaskRegistry::new(SteeringRegistry::new(), 8, |status: &bool| *status);
    let task_id = TaskId::new("dropped");
    let (status_tx, status_rx) = tokio::sync::watch::channel(false);
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
    // The executor went away without publishing a terminal status: nothing
    // will ever drain a steer or settle the task.
    drop(status_tx);
    assert!(matches!(
        registry.steering_handle(&task_id, "owner"),
        Err(DetachedTaskRegistryError::AlreadyDone)
    ));
    assert!(matches!(
        registry.cancel_cooperative(&task_id, "owner"),
        Err(DetachedTaskRegistryError::AlreadyDone)
    ));
    assert_eq!(registry.sweep_terminal().unwrap(), 1);
    task.abort();
}
