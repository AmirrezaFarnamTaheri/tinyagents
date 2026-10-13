use super::*;

#[test]
fn describe_renders_mermaid_json_and_validation() {
    let topology = crate::delegation::delegation_graph_topology().expect("delegation topology");
    let report = GraphTopologyReport::describe("delegation", &topology);
    assert_eq!(report.name, "delegation");
    assert!(report.ok, "{:?}", report.errors);
    assert!(report.mermaid.contains("flowchart"));
    assert!(!report.json.is_empty());
    assert_eq!(report.errors, topology.validation.errors);
}
