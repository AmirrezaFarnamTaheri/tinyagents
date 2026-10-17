use super::*;

#[test]
fn stale_generation_cannot_cancel_its_resumed_successor() {
    let supervisor = WorkflowRunSupervisor::default();
    let pending = Arc::new(std::sync::Mutex::new(Vec::<
        Pin<Box<dyn Future<Output = ()> + Send>>,
    >::new()));
    let first_pending = pending.clone();
    let second_pending = pending.clone();
    let first = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            first_pending.lock().unwrap().push(Box::pin(future));
        },
    );
    let second = supervisor.resume(
        "run",
        |_| async {},
        move |future| {
            second_pending.lock().unwrap().push(Box::pin(future));
        },
    );

    assert!(first.cancellation_token().is_cancelled());
    assert!(!supervisor.stop_generation(&first));
    assert!(!second.cancellation_token().is_cancelled());
    drop(pending.lock().unwrap().remove(0));
    assert!(supervisor.stop("run"));
    assert!(second.cancellation_token().is_cancelled());
}

#[test]
fn finish_only_removes_the_generation_that_finished() {
    let supervisor = WorkflowRunSupervisor::default();
    let pending = Arc::new(std::sync::Mutex::new(Vec::<
        Pin<Box<dyn Future<Output = ()> + Send>>,
    >::new()));
    let old_pending = pending.clone();
    let current_pending = pending.clone();
    let old = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            old_pending.lock().unwrap().push(Box::pin(future));
        },
    );
    let current = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            current_pending.lock().unwrap().push(Box::pin(future));
        },
    );

    assert!(!supervisor.finish(&old));
    assert!(supervisor.stop("run"));
    assert!(current.cancellation_token().is_cancelled());
    pending.lock().unwrap().clear();
    assert!(!supervisor.stop("run"));
}

#[test]
fn dropping_unpolled_drive_clears_its_generation() {
    let supervisor = WorkflowRunSupervisor::default();
    let handle = supervisor.start("run", |_| async {}, drop);

    assert!(!supervisor.stop_generation(&handle));
    assert!(!supervisor.stop("run"));
}

#[test]
fn handle_aware_start_installs_current_handle_before_spawning_fast_future() {
    let supervisor = WorkflowRunSupervisor::default();
    let mut observed = None;
    let returned = supervisor.start_scoped_with_handle(
        "run",
        |_| async {},
        |future| future,
        |future| future,
        |handle, future| {
            assert!(supervisor.is_current(&handle));
            drop(future);
            observed = Some(handle);
        },
    );

    assert_eq!(observed.unwrap().generation, returned.generation);
    assert!(!supervisor.is_current(&returned));
}

#[test]
fn old_handle_is_not_current_after_replacement_and_stop_keeps_current_identity() {
    let supervisor = WorkflowRunSupervisor::default();
    let old = supervisor.start("run", |_| async {}, drop);
    let pending = Arc::new(std::sync::Mutex::new(
        None::<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ));
    let pending_slot = pending.clone();
    let current = supervisor.resume_scoped_with_handle(
        "run",
        |_| async {},
        |future| future,
        |future| future,
        |handle, future| {
            assert!(supervisor.is_current(&handle));
            *pending_slot.lock().unwrap() = Some(Box::pin(future));
        },
    );

    assert!(!supervisor.is_current(&old));
    assert!(supervisor.is_current(&current));
    assert!(supervisor.stop_generation(&current));
    assert!(supervisor.is_current(&current));
    pending.lock().unwrap().take();
    assert!(!supervisor.is_current(&current));
}

#[test]
fn current_handle_snapshot_cannot_cancel_a_replacement() {
    let supervisor = WorkflowRunSupervisor::default();
    let pending = Arc::new(std::sync::Mutex::new(Vec::<
        Pin<Box<dyn Future<Output = ()> + Send>>,
    >::new()));
    let first_pending = pending.clone();
    let second_pending = pending.clone();

    assert!(supervisor.current_handle("run").is_none());
    supervisor.start(
        "run",
        |_| async {},
        move |future| {
            first_pending.lock().unwrap().push(Box::pin(future));
        },
    );
    let snapshot = supervisor.current_handle("run").unwrap();
    assert!(supervisor.is_current(&snapshot));

    let replacement = supervisor.resume(
        "run",
        |_| async {},
        move |future| {
            second_pending.lock().unwrap().push(Box::pin(future));
        },
    );
    assert!(!supervisor.stop_generation(&snapshot));
    assert!(!replacement.cancellation_token().is_cancelled());
    assert!(supervisor.is_current(&replacement));

    pending.lock().unwrap().clear();
    assert!(supervisor.current_handle("run").is_none());
}

#[test]
fn current_handle_tracks_stopped_and_finished_generations() {
    let supervisor = WorkflowRunSupervisor::default();
    let pending = Arc::new(std::sync::Mutex::new(
        None::<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ));
    let slot = pending.clone();
    let handle = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            *slot.lock().unwrap() = Some(Box::pin(future));
        },
    );

    let snapshot = supervisor.current_handle("run").unwrap();
    assert_eq!(snapshot.generation, handle.generation);
    assert!(supervisor.current_handle("other-run").is_none());
    assert!(supervisor.stop_generation(&snapshot));
    let stopped = supervisor.current_handle("run").unwrap();
    assert!(stopped.cancellation_token().is_cancelled());
    assert!(supervisor.is_current(&stopped));
    assert!(supervisor.finish(&snapshot));
    assert!(supervisor.current_handle("run").is_none());
    pending.lock().unwrap().take();
}

#[test]
fn ready_drive_clears_its_generation_while_future_is_retained() {
    let supervisor = WorkflowRunSupervisor::default();
    let pending = Arc::new(std::sync::Mutex::new(
        None::<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ));
    let slot = pending.clone();
    let handle = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            *slot.lock().unwrap() = Some(Box::pin(future));
        },
    );
    let mut future = pending.lock().unwrap().take().unwrap();
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(())
    ));
    assert!(!supervisor.stop_generation(&handle));
    drop(future);
}

#[test]
fn completed_drive_clears_its_generation_and_stale_drop_preserves_successor() {
    let supervisor = WorkflowRunSupervisor::default();
    let old_pending = Arc::new(std::sync::Mutex::new(
        None::<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ));
    let slot = old_pending.clone();
    let old = supervisor.start(
        "run",
        |_| async {},
        move |future| {
            *slot.lock().unwrap() = Some(Box::pin(future));
        },
    );
    let successor_pending = Arc::new(std::sync::Mutex::new(
        None::<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ));
    let successor_slot = successor_pending.clone();
    let successor = supervisor.resume(
        "run",
        |_| futures::future::pending(),
        move |future| {
            *successor_slot.lock().unwrap() = Some(Box::pin(future));
        },
    );
    assert!(old.cancellation_token().is_cancelled());
    futures::executor::block_on(old_pending.lock().unwrap().take().unwrap());
    assert!(supervisor.stop_generation(&successor));

    let completed = supervisor.start("completed", |_| async {}, futures::executor::block_on);
    assert!(!supervisor.stop_generation(&completed));
}

#[test]
fn scoped_start_wraps_drive_with_root_parent_then_origin_before_spawning() {
    let supervisor = WorkflowRunSupervisor::default();
    let scopes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let root_scopes = scopes.clone();
    let origin_scopes = scopes.clone();

    supervisor.start_scoped(
        "run",
        |_| async {},
        move |future| {
            root_scopes.lock().unwrap().push("root-parent");
            future
        },
        move |future| {
            origin_scopes.lock().unwrap().push("origin");
            future
        },
        futures::executor::block_on,
    );

    assert_eq!(*scopes.lock().unwrap(), ["root-parent", "origin"]);
}
