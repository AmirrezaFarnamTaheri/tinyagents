use tinyagents_harness::cancel::CancellationToken;
use tinyagents_orchestration::subagent::{
    DriverJobRegistration, ParentJobScope, SubAgentJobRegistry, format_parent_roster_for_owner,
};
use tinytools::WorkspaceDescriptor;

#[test]
fn parent_job_scope_is_available_from_the_public_subagent_facade() {
    let constructor: fn(WorkspaceDescriptor, String) -> ParentJobScope = ParentJobScope::new;
    let _ = constructor;
    let registry = SubAgentJobRegistry::new();
    let scope = ParentJobScope::new(WorkspaceDescriptor::new("/workspace"), "thread");
    let rendered =
        format_parent_roster_for_owner(&registry, "owner", &scope, &[], |_| "empty".to_owned())
            .unwrap();
    assert_eq!(rendered, "empty");
}

#[test]
fn parent_scope_can_cancel_only_its_jobs_through_the_public_facade() {
    let registry = SubAgentJobRegistry::new();
    let workspace_a = WorkspaceDescriptor::new("/workspace/a");
    let workspace_b = WorkspaceDescriptor::new("/workspace/b");
    let token_a = CancellationToken::new();
    let token_b = CancellationToken::new();
    for (task, thread, workspace, cancellation) in [
        (
            "task-a",
            "same-thread",
            workspace_a.clone(),
            token_a.clone(),
        ),
        ("task-b", "same-thread", workspace_b, token_b.clone()),
    ] {
        registry
            .register_driver_job(DriverJobRegistration {
                key: tinyagents_orchestration::subagent::SubagentTaskKey {
                    root_run_id: "root".into(),
                    parent_run_id: "parent".into(),
                    thread_id: Some(thread.into()),
                    task_id: task.into(),
                },
                agent: "worker".into(),
                owner: "owner".into(),
                workspace: Some(workspace),
                cancellation,
                steering: None,
                run_queue: None,
                continuing: false,
            })
            .unwrap();
    }

    assert_eq!(
        registry
            .cancel_for_thread(&ParentJobScope::new(workspace_a, "same-thread"))
            .unwrap(),
        1
    );
    assert!(token_a.is_cancelled());
    assert!(!token_b.is_cancelled());
}
