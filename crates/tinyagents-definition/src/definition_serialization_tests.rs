use super::*;

#[test]
fn an_absent_routing_role_remains_absent_in_definition_payloads() {
    let definition = AgentDefinition::new("worker", "A specialist");
    let payload = serde_json::to_value(&definition).unwrap();
    assert!(
        payload.get("role").is_none(),
        "an unconfigured role must not become a new null field"
    );
    let mut configured = definition;
    configured.role = Some("worker".into());
    assert_eq!(serde_json::to_value(&configured).unwrap()["role"], "worker");
    let restored: AgentDefinition = serde_json::from_value(payload).unwrap();
    assert_eq!(restored.role, None);
}
