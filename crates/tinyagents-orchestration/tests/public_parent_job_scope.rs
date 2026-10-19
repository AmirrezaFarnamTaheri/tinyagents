use tinyagents_orchestration::subagent::ParentJobScope;
use tinytools::WorkspaceDescriptor;

#[test]
fn parent_job_scope_is_available_from_the_public_subagent_facade() {
    let constructor: fn(WorkspaceDescriptor, String) -> ParentJobScope = ParentJobScope::new;
    let _ = constructor;
}
