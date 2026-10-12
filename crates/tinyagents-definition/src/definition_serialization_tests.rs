use super::*;

#[test]
fn an_absent_routing_role_remains_absent_in_definition_payloads() {
    let definition = AgentDefinition::new("worker", "Worker", "A specialist");
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

#[test]
fn authored_deny_globs_apply_to_the_resolved_named_tool_scope() {
    let mut definition = AgentDefinition::new("worker", "Worker", "A specialist");
    definition.tools = ToolScope::Named(vec![
        "file_write".into(),
        "FILE_read".into(),
        "web_search".into(),
    ]);
    definition.disallowed_tools = vec!["file_*".into()];
    assert_eq!(
        definition.resolve_tool_scope(None),
        ToolScope::Named(vec!["web_search".into()]),
        "definition deny patterns must preserve the shared case-insensitive glob contract"
    );
}
