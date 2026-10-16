use super::*;

#[test]
fn stale_generation_cannot_cancel_its_resumed_successor() {
    let supervisor = WorkflowRunSupervisor::default();
    let first = supervisor.start("run", |_| async {}, |_| {});
    let second = supervisor.resume("run", |_| async {}, |_| {});

    assert!(first.cancellation_token().is_cancelled());
    assert!(!supervisor.stop_generation(&first));
    assert!(!second.cancellation_token().is_cancelled());
    assert!(supervisor.stop_generation(&second));
    assert!(second.cancellation_token().is_cancelled());
}

#[test]
fn finish_only_removes_the_generation_that_finished() {
    let supervisor = WorkflowRunSupervisor::default();
    let old = supervisor.start("run", |_| async {}, |_| {});
    let current = supervisor.start("run", |_| async {}, |_| {});

    assert!(!supervisor.finish(&old));
    assert!(supervisor.stop("run"));
    assert!(current.cancellation_token().is_cancelled());
    assert!(supervisor.finish(&current));
    assert!(!supervisor.stop("run"));
}
