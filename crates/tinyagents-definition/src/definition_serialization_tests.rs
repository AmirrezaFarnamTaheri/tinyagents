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

#[test]
fn replacing_product_extensions_preserves_all_generic_definition_fields() {
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct Product {
        enabled: bool,
    }
    let mut definition = AgentDefinition::new("worker", "Worker", "Specialist")
        .with_extensions(Product { enabled: true });
    definition.system_prompt = PromptSource::Verbatim("prompt bytes\n".into());
    definition.model = ModelSpec::Hint("coding".into());
    definition.temperature = 0.9;
    definition.role = Some("routing role".into());
    definition.tool_rules = Some(tinytools::ToolRules::default());
    definition.tools = ToolScope::Named(vec!["second".into(), "first".into()]);
    definition.disallowed_tools = vec!["blocked*".into()];
    definition.skill_filter = Some("skill".into());
    definition.extra_tools = vec!["extra".into()];
    definition.deferred_tools = vec!["second".into()];
    definition.max_iterations = 200;
    definition.iteration_policy = IterationPolicy::Extended;
    definition.max_result_chars = Some(42);
    definition.max_turn_output_tokens = Some(100);
    definition.timeout_secs = Some(60);
    definition.sandbox_mode = SandboxMode::Sandboxed;
    definition.background = true;
    definition.subagents = vec![SubagentEntry::AgentId("leaf".into())];
    definition.delegate_name = Some("specialist".into());
    definition.agent_tier = AgentTier::Reasoning;
    definition.source = DefinitionSource::CustomRegistry;
    let mut original = serde_json::to_value(&definition).unwrap();
    assert_eq!(
        original.as_object_mut().unwrap().remove("enabled"),
        Some(true.into())
    );
    let portable = definition.with_extensions(Extensions::default());
    assert_eq!(serde_json::to_value(&portable).unwrap(), original);
    assert_eq!(portable.source, DefinitionSource::CustomRegistry);
}

#[test]
fn a_host_prompt_marker_is_inert_and_cannot_be_authored() {
    let marker = PromptSource::Dynamic(());
    assert_eq!(
        serde_json::to_value(marker).unwrap(),
        serde_json::json!({"dynamic": null})
    );
    assert!(serde_json::from_value::<PromptSource>(serde_json::json!({"dynamic": null})).is_err());
}

#[test]
fn explicit_materialization_and_deferred_options_do_not_invent_tool_authority() {
    let mut definition = AgentDefinition::new("worker", "Worker", "Specialist");
    let options = ToolScopeResolution {
        materialize_wildcard: true,
        include_extra: false,
        include_deferred: false,
    };
    assert_eq!(
        definition.resolve_tool_scope_with(None, options),
        ToolScope::Named(vec![])
    );
    definition.extra_tools = vec!["unregistered_extra".into()];
    let catalogue = vec!["second".into(), "first".into(), "second".into()];
    assert_eq!(
        definition.resolve_tool_scope_with(Some(&catalogue), options),
        ToolScope::Named(vec!["second".into(), "first".into()])
    );
    definition.tools = ToolScope::Named(vec![]);
    definition.extra_tools.clear();
    definition.deferred_tools = vec!["hidden".into()];
    assert_eq!(
        definition.resolve_tool_scope_with(Some(&catalogue), options),
        ToolScope::Named(vec![])
    );
    assert_eq!(
        definition.resolve_tool_scope(None),
        ToolScope::Named(vec!["hidden".into()])
    );
}
