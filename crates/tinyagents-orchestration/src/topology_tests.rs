use super::*;

#[test]
fn all_topologies_includes_member_delegation_and_workflow_scheduler() {
    let reports = all_graph_topologies();
    for name in [
        "agent_teams:member",
        "delegation",
        "workflow_runs:scheduler_preview",
    ] {
        let report = reports
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("the {name} graph should be exported"));
        assert!(
            report.ok,
            "{name} graph should validate structurally: {:?}",
            report.errors
        );
        assert!(
            report.mermaid.contains("flowchart"),
            "{name} mermaid should render: {}",
            report.mermaid
        );
    }
}
