use tinyagents_orchestration::subagent::{
    ParentJobScope, SubAgentJobRegistry, format_parent_roster_for_owner,
};
use tinytools::WorkspaceDescriptor;

#[test]
fn parent_job_scope_is_available_from_the_public_subagent_facade() {
    let constructor: fn(WorkspaceDescriptor, String) -> ParentJobScope = ParentJobScope::new;
    let _ = constructor;
    let _public_roster = format_parent_roster_for_owner;
    let _registry_api = SubAgentJobRegistry::list_for_parent_scope;
}
