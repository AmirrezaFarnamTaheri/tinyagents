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

#[test]
fn failed_transactional_start_keeps_previous_generation_and_does_not_spawn() {
    let supervisor = WorkflowRunSupervisor::default();
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_old = held.clone();
    let old = supervisor.start(
        "run",
        |_| futures::future::pending(),
        move |future| held_old.lock().unwrap().push(future),
    );
    let spawned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let spawned_flag = spawned.clone();

    let result = supervisor.start_scoped_with_commit(
        "run",
        || Err::<(), _>("durable claim rejected"),
        |_| async {},
        |future| future,
        |future| future,
        move |_, future| {
            spawned_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(future);
        },
    );

    assert_eq!(result.unwrap_err(), "durable claim rejected");
    assert!(!spawned.load(std::sync::atomic::Ordering::SeqCst));
    assert!(supervisor.is_current(&old));
    assert!(!old.cancellation_token().is_cancelled());
}

#[test]
fn transactional_commit_serializes_generation_install_until_commit_finishes() {
    let supervisor = WorkflowRunSupervisor::default();
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_old = held.clone();
    let old = supervisor.start(
        "run",
        |_| futures::future::pending(),
        move |future| held_old.lock().unwrap().push(future),
    );
    let commit_entered = Arc::new(std::sync::Barrier::new(2));
    let release_commit = Arc::new(std::sync::Barrier::new(2));
    let supervisor_for_start = supervisor.clone();
    let entered_for_commit = commit_entered.clone();
    let release_for_commit = release_commit.clone();
    let start_thread = std::thread::spawn(move || {
        supervisor_for_start.start_scoped_with_commit(
            "run",
            move || {
                entered_for_commit.wait();
                release_for_commit.wait();
                Ok::<_, ()>(())
            },
            |_| futures::future::pending(),
            |future| future,
            |future| future,
            |_, future| drop(future),
        )
    });

    commit_entered.wait();
    let snapshot_started = Arc::new(std::sync::Barrier::new(2));
    let snapshot_started_thread = snapshot_started.clone();
    let supervisor_for_snapshot = supervisor.clone();
    let snapshot_thread = std::thread::spawn(move || {
        snapshot_started_thread.wait();
        supervisor_for_snapshot.current_handle("run")
    });
    snapshot_started.wait();

    // The commit owns the registry lock, so a current-generation snapshot
    // cannot observe a partially committed durable transition.
    assert!(snapshot_thread.is_finished() == false);
    release_commit.wait();
    let (new, ()) = start_thread.join().unwrap().unwrap();
    let snapshot = snapshot_thread.join().unwrap();

    assert!(old.cancellation_token().is_cancelled());
    assert_eq!(snapshot.unwrap().generation, new.generation);
    assert!(supervisor.is_current(&new));
}

#[test]
fn losing_transactional_commit_does_not_cancel_a_concurrent_winner() {
    let supervisor = WorkflowRunSupervisor::default();
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_old = held.clone();
    let old = supervisor.start(
        "run",
        |_| futures::future::pending(),
        move |future| held_old.lock().unwrap().push(future),
    );
    let gate = Arc::new(std::sync::Barrier::new(2));
    let thread_gate = gate.clone();
    let supervisor_for_loser = supervisor.clone();
    let loser = std::thread::spawn(move || {
        supervisor_for_loser.start_scoped_with_commit(
            "run",
            move || {
                thread_gate.wait();
                Err::<(), _>("compare-and-swap lost")
            },
            |_| futures::future::pending(),
            |future| future,
            |future| future,
            |_, future| drop(future),
        )
    });
    gate.wait();
    let winner = supervisor.resume_scoped_with_commit(
        "run",
        || Ok::<_, ()>(()),
        |_| futures::future::pending(),
        |future| future,
        |future| future,
        |_, future| drop(future),
    );

    assert_eq!(loser.join().unwrap().unwrap_err(), "compare-and-swap lost");
    let (winner_handle, ()) = winner.unwrap();
    assert!(old.cancellation_token().is_cancelled());
    assert!(!winner_handle.cancellation_token().is_cancelled());
    assert!(supervisor.is_current(&winner_handle));
}

#[test]
fn with_current_skips_stale_callback_and_serializes_current_callback() {
    let supervisor = WorkflowRunSupervisor::default();
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let held_old = held.clone();
    let old = supervisor.start(
        "run",
        |_| futures::future::pending(),
        move |future| held_old.lock().unwrap().push(future),
    );
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let called_flag = called.clone();
    let held_current = held.clone();
    let current = supervisor.resume(
        "run",
        |_| futures::future::pending(),
        move |future| held_current.lock().unwrap().push(future),
    );

    assert_eq!(
        supervisor.with_current(&old, move || {
            called_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }),
        None
    );
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));

    let callback_entered = Arc::new(std::sync::Barrier::new(2));
    let release_callback = Arc::new(std::sync::Barrier::new(2));
    let entered = callback_entered.clone();
    let release = release_callback.clone();
    let supervisor_for_callback = supervisor.clone();
    let handle = current.clone();
    let callback_thread = std::thread::spawn(move || {
        supervisor_for_callback.with_current(&handle, move || {
            entered.wait();
            release.wait();
        })
    });
    callback_entered.wait();

    let replacement_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = replacement_done.clone();
    let supervisor_for_replacement = supervisor.clone();
    let replacement_thread = std::thread::spawn(move || {
        supervisor_for_replacement.start(
            "run",
            |_| futures::future::pending(),
            move |future| held.lock().unwrap().push(future),
        );
        done.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    assert!(!replacement_done.load(std::sync::atomic::Ordering::SeqCst));
    release_callback.wait();
    assert_eq!(callback_thread.join().unwrap(), Some(()));
    let replacement = replacement_thread.join().unwrap();
    assert!(current.cancellation_token().is_cancelled());
    assert!(supervisor.is_current(&replacement));
}
