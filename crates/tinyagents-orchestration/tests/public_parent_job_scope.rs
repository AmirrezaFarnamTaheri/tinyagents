use tinyagents_orchestration::subagent::{
    ParentJobScope, SubAgentJobRegistry, format_parent_roster_for_owner,
};
use tinytools::WorkspaceDescriptor;

#[test]
fn parent_job_scope_is_available_from_the_public_subagent_facade() {
    let constructor: fn(WorkspaceDescriptor, String) -> ParentJobScope = ParentJobScope::new;
    let _ = constructor;
    let registry = SubAgentJobRegistry::new();
    let scope = ParentJobScope::new(WorkspaceDescriptor::new("/workspace"), "thread");
    let rendered = format_parent_roster_for_owner(&registry, "owner", &scope, &[], |_| {
        "empty".to_owned()
    })
    .unwrap();
    assert_eq!(rendered, "empty");
}
