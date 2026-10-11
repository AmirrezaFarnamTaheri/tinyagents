use super::*;

fn make_def(id: &str) -> AgentDefinition {
    AgentDefinition::new(id, id, "test")
}

#[test]
fn model_spec_resolve_inherit_uses_parent() {
    let spec = ModelSpec::Inherit;
    assert_eq!(spec.resolve("parent-model"), "parent-model");
}

#[test]
fn model_spec_resolve_exact_uses_name() {
    let spec = ModelSpec::Exact("kimi-k2".into());
    assert_eq!(spec.resolve("parent-model"), "kimi-k2");
}

#[test]
fn model_spec_resolve_hint_yields_the_role_alias() {
    let spec = ModelSpec::Hint("coding".into());
    assert_eq!(spec.resolve("parent-model"), "hint:coding");
}

#[test]
fn display_name_falls_back_to_id() {
    let def = make_def("alpha");
    assert_eq!(def.display_name(), "alpha");
    let mut def2 = make_def("beta");
    def2.display_name = Some("Beta Specialist".into());
    assert_eq!(def2.display_name(), "Beta Specialist");
}

// ── subagents parsing ─────────────────────────────────────────────

/// Parses a minimal TOML document with a `subagents` list containing
/// both a bare agent-id string and an inline `{ skills = "*" }` table.
/// Ensures the `#[serde(untagged)]` enum routes each shape to the
/// correct variant without the TOML needing explicit tags.
///
/// NOTE: `subagents = [...]` must appear **before** the `[tools]`
/// table header in the TOML — once you open a TOML table section,
/// every subsequent top-level key is consumed by that table, so
/// `subagents` placed after `[tools]` would be parsed as
/// `tools.subagents` and fail because `ToolScope` is an enum, not
/// a struct with a `subagents` field.
#[test]
fn subagents_parses_mixed_string_and_table_entries() {
    let toml_src = r#"
id = "orchestrator"
when_to_use = "Routes work to the right specialist"
temperature = 0.4
max_iterations = 15

subagents = [
"researcher",
"code_executor",
{ skills = "*" },
]

[tools]
named = ["query_memory"]
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert_eq!(def.subagents.len(), 3);
    assert_eq!(
        def.subagents[0],
        SubagentEntry::AgentId("researcher".into())
    );
    assert_eq!(
        def.subagents[1],
        SubagentEntry::AgentId("code_executor".into())
    );
    assert_eq!(
        def.subagents[2],
        SubagentEntry::Skills(SkillsWildcard { skills: "*".into() })
    );
}

#[test]
fn subagents_section_parses_allowlist_entries() {
    let toml_src = r#"
id = "orchestrator"
when_to_use = "Routes work to the right specialist"
temperature = 0.4
max_iterations = 15

[subagents]
allowlist = [
    "researcher",
    "code_executor",
    { skills = "*" },
]

[tools]
named = ["query_memory"]
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert_eq!(
        def.subagents,
        vec![
            SubagentEntry::AgentId("researcher".into()),
            SubagentEntry::AgentId("code_executor".into()),
            SubagentEntry::Skills(SkillsWildcard { skills: "*".into() }),
        ]
    );
}

/// `subagents` is optional — omitting it should yield an empty Vec
/// rather than a deserialization error. Most non-delegating agents
/// (critic, summarizer, etc.) will not list any.
#[test]
fn subagents_defaults_to_empty_when_omitted() {
    let toml_src = r#"
id = "code_executor"
when_to_use = "Runs code and shell commands"
temperature = 0.7
max_iterations = 6

[tools]
named = ["shell", "file_read"]
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert!(def.subagents.is_empty());
    assert!(def.delegate_name.is_none());
}

/// The `delegate_name` field lets an agent expose itself under a
/// shorter / more natural tool name than `delegate_{id}`. For example
/// the `researcher` agent is exposed as `research` in the
/// orchestrator's tool list.
#[test]
fn delegate_name_overrides_default() {
    let toml_src = r#"
id = "researcher"
when_to_use = "Web & docs crawler"
delegate_name = "research"
temperature = 0.4
max_iterations = 8
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert_eq!(def.delegate_name.as_deref(), Some("research"));
}

/// `SkillsWildcard::matches_all` is the predicate the tool builder
/// checks before expanding a wildcard into per-toolkit tools. Only
/// the literal `"*"` should be accepted today — any other pattern
/// (reserved for future specific-toolkit lists) must not match.
#[test]
fn skills_wildcard_only_star_matches_all() {
    let star = SkillsWildcard { skills: "*".into() };
    assert!(star.matches_all());

    let specific = SkillsWildcard {
        skills: "gmail".into(),
    };
    assert!(!specific.matches_all());
}

// ── iteration policy ─────────────────────────────────────────────

#[test]
fn strict_policy_returns_max_iterations_unchanged() {
    let mut def = make_def("summarizer");
    def.max_iterations = 2;
    def.iteration_policy = IterationPolicy::Strict;
    assert_eq!(def.effective_max_iterations(), 2);
}

#[test]
fn extended_policy_raises_cap_to_at_least_extended_constant() {
    let mut def = make_def("code_executor");
    def.max_iterations = 10;
    def.iteration_policy = IterationPolicy::Extended;
    assert_eq!(
        def.effective_max_iterations(),
        super::EXTENDED_MAX_TOOL_ITERATIONS
    );
    assert!(def.effective_max_iterations() > def.max_iterations);
}

#[test]
fn extended_policy_preserves_custom_cap_when_higher_than_constant() {
    let mut def = make_def("custom_agent");
    def.max_iterations = 100;
    def.iteration_policy = IterationPolicy::Extended;
    assert_eq!(def.effective_max_iterations(), 100);
}

#[test]
fn iteration_policy_defaults_to_strict() {
    let def = make_def("test");
    assert_eq!(def.iteration_policy, IterationPolicy::Strict);
}

#[test]
fn iteration_policy_parses_from_toml() {
    let toml_src = r#"
id = "code_executor"
when_to_use = "Runs code"
max_iterations = 10
iteration_policy = "extended"
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert_eq!(def.iteration_policy, IterationPolicy::Extended);
    assert_eq!(
        def.effective_max_iterations(),
        super::EXTENDED_MAX_TOOL_ITERATIONS
    );
}

#[test]
fn iteration_policy_omitted_defaults_strict() {
    let toml_src = r#"
id = "summarizer"
when_to_use = "Summarizes"
max_iterations = 1
"#;
    let def: AgentDefinition = toml::from_str(toml_src).expect("toml parse");
    assert_eq!(def.iteration_policy, IterationPolicy::Strict);
    assert_eq!(def.effective_max_iterations(), 1);
}

// ── Spawn-hierarchy tier rule (validate_tier_transition) ────────────────────
// Single source of truth shared by the boot loader walk and the runtime
// spawn gate in `run_subagent` (issue #4098).

#[test]
fn tier_transition_allows_legal_descending_handoffs() {
    // chat → reasoning, chat → worker, reasoning → worker are the only
    // legal hops; each strictly descends the hierarchy.
    assert!(validate_tier_transition(AgentTier::Chat, AgentTier::Reasoning).is_ok());
    assert!(validate_tier_transition(AgentTier::Chat, AgentTier::Worker).is_ok());
    assert!(validate_tier_transition(AgentTier::Reasoning, AgentTier::Worker).is_ok());
}

#[test]
fn tier_transition_rejects_chat_to_chat() {
    let err = validate_tier_transition(AgentTier::Chat, AgentTier::Chat)
        .expect_err("chat→chat must be rejected");
    // Wording must carry the substrings the loader diagnostics rely on.
    assert!(err.contains("chat") && err.contains("leaf"), "got: {err}");
}

#[test]
fn tier_transition_rejects_reasoning_to_reasoning() {
    let err = validate_tier_transition(AgentTier::Reasoning, AgentTier::Reasoning)
        .expect_err("reasoning→reasoning must be rejected");
    assert!(err.contains("reasoning"), "got: {err}");
}

#[test]
fn tier_transition_allows_upward_reasoning_to_chat() {
    // Upward delegation is intentionally legal: a reasoning agent may hand
    // follow-ups back to the `orchestrator` chat agent. Only same-tier
    // and worker-as-parent hops are forbidden.
    assert!(validate_tier_transition(AgentTier::Reasoning, AgentTier::Chat).is_ok());
}

#[test]
fn tier_transition_rejects_worker_as_parent() {
    // A worker is a leaf executor — it may not spawn any tier, including
    // another worker.
    for child in [AgentTier::Chat, AgentTier::Reasoning, AgentTier::Worker] {
        let err = validate_tier_transition(AgentTier::Worker, child)
            .expect_err("worker must not spawn any tier");
        assert!(
            err.contains("worker") && err.contains("leaf"),
            "worker→{} reason should call out the worker-leaf rule, got: {err}",
            child.as_str()
        );
    }
}

#[test]
fn tier_display_matches_as_str() {
    assert_eq!(AgentTier::Chat.to_string(), "chat");
    assert_eq!(AgentTier::Reasoning.to_string(), "reasoning");
    assert_eq!(AgentTier::Worker.to_string(), "worker");
}

#[test]
fn legacy_catalogue_json_can_still_be_read() {
    let def: AgentDefinition = serde_json::from_value(serde_json::json!({"id":"worker", "name":"Worker", "description":"does work", "model":"exact-model", "tools":["read"], "subagents":["helper"]})).unwrap();
    assert_eq!(def.display_name(), "Worker");
    assert_eq!(def.model.pin().as_deref(), Some("exact-model"));
    assert_eq!(def.tools, ToolScope::Named(vec!["read".into()]));
    assert_eq!(def.allowed_subagent_ids(), ["helper"]);
}

#[test]
fn extensions_retain_authored_host_fields_and_skip_runtime_only_members() {
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct HostExtension {
        #[serde(default)]
        omit_identity: bool,
        #[serde(default)]
        searches_connected_mcp: bool,
        #[serde(skip)]
        dynamic_prompt: Option<fn() -> String>,
    }
    let mut def: AgentDefinition<HostExtension> = toml::from_str(
        "id='agent'\nwhen_to_use='test'\nomit_identity=true\nsearches_connected_mcp=true\n",
    )
    .unwrap();
    def.extensions.dynamic_prompt = Some(|| "built-in".into());
    let encoded = toml::to_string(&def).unwrap();
    assert!(!encoded.contains("dynamic_prompt"));
    let parsed: AgentDefinition<HostExtension> = toml::from_str(&encoded).unwrap();
    assert!(parsed.extensions.omit_identity && parsed.extensions.searches_connected_mcp);
    assert!(parsed.extensions.dynamic_prompt.is_none());
    let replaced = def.with_extensions(Extensions::default());
    assert_eq!(replaced.id, "agent");
}

#[test]
fn named_empty_and_denied_wildcards_never_become_unrestricted() {
    let mut def = AgentDefinition::new("agent", "Agent", "test").with_tools(Vec::<String>::new());
    assert_eq!(def.resolve_tool_scope(None), ToolScope::Named(vec![]));
    def.tools = ToolScope::Wildcard;
    def.disallowed_tools = vec!["write".into()];
    assert_eq!(def.resolve_tool_scope(None), ToolScope::Named(vec![]));
    def.extra_tools = vec!["read".into(), "write".into()];
    assert_eq!(
        def.resolve_tool_scope(Some(&["read".into(), "write".into()])),
        ToolScope::Named(vec!["read".into()])
    );
}

#[test]
fn portable_prompt_sources_roundtrip_and_dynamic_sources_are_not_authored() {
    for prompt in [
        PromptSource::Inline("inline".into()),
        PromptSource::File {
            path: "prompt.md".into(),
        },
        PromptSource::Verbatim("verbatim".into()),
    ] {
        let mut def = make_def("agent");
        def.system_prompt = prompt.clone();
        let parsed: AgentDefinition = toml::from_str(&toml::to_string(&def).unwrap()).unwrap();
        assert_eq!(parsed.system_prompt, prompt);
    }
    let unknown = serde_json::json!({"dynamic": null});
    assert!(serde_json::from_value::<PromptSource>(unknown).is_err());
}

#[test]
fn generic_loader_skips_bad_files_and_preserves_source_for_valid_custom_agents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("valid.toml");
    std::fs::write(
        &path,
        "id='custom'\nwhen_to_use='custom task'\n[system_prompt]\ninline='custom prompt'\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("bad.toml"), "not = [valid").unwrap();
    std::fs::write(dir.path().join("note.md"), "not TOML").unwrap();
    std::fs::create_dir(dir.path().join("nested.toml")).unwrap();
    let mut definitions: Vec<AgentDefinition> = vec![];
    load_dir(dir.path(), &mut definitions).unwrap();
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].source, DefinitionSource::File(path));
    let missing = dir.path().join("missing.toml");
    std::fs::write(&missing, "id='custom'\nwhen_to_use='custom task'\n").unwrap();
    assert!(
        load_file::<Extensions>(&missing)
            .unwrap_err()
            .to_string()
            .contains("missing `system_prompt`")
    );
    assert!(load_file::<Extensions>(&dir.path().join("absent.toml")).is_err());
    assert!(load_dir::<Extensions>(&dir.path().join("missing"), &mut definitions).is_err());
}

#[test]
fn deferred_names_join_named_scope_before_denials_and_deduplication() {
    let mut def = make_def("agent").with_tools(vec!["read"]);
    def.extra_tools = vec!["read".into(), "extra".into()];
    def.deferred_tools = vec!["deferred".into(), "read".into(), "denied".into()];
    def.disallowed_tools = vec!["denied".into()];
    assert_eq!(
        def.resolve_tool_scope(None),
        ToolScope::Named(vec!["read".into(), "extra".into(), "deferred".into()])
    );
}

#[test]
fn wildcard_additions_materialize_known_catalogues_and_remain_fail_closed_without_one() {
    for deferred in [false, true] {
        let mut def = make_def("agent");
        if deferred {
            def.deferred_tools = vec!["added".into()];
        } else {
            def.extra_tools = vec!["added".into()];
        }
        assert_eq!(
            def.resolve_tool_scope(Some(&["read".into()])),
            ToolScope::Named(vec!["read".into(), "added".into()])
        );
        assert_eq!(def.resolve_tool_scope(None), ToolScope::Named(vec![]));
    }
}

#[test]
fn model_hints_are_separate_from_exact_pins() {
    assert_eq!(ModelSpec::Hint("reasoning".into()).pin(), None);
    assert_eq!(
        ModelSpec::Hint("reasoning".into()).hint().as_deref(),
        Some("reasoning")
    );
    assert_eq!(
        ModelSpec::Exact("model".into()).pin().as_deref(),
        Some("model")
    );
    assert_eq!(ModelSpec::Exact("model".into()).hint(), None);
    assert_eq!(ModelSpec::Inherit.pin(), None);
    assert_eq!(ModelSpec::Inherit.hint(), None);
}

#[test]
fn loader_rejects_semantically_invalid_authored_definitions() {
    let dir = tempfile::tempdir().unwrap();
    for (name, authored) in [
        (
            "blank-id",
            "id=' '\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\n",
        ),
        (
            "blank-purpose",
            "id='agent'\nwhen_to_use=' '\nsystem_prompt={inline='prompt'}\n",
        ),
        (
            "blank-tool",
            "id='agent'\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\n[tools]\nnamed=['']\n",
        ),
        (
            "duplicate-tool",
            "id='agent'\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\n[tools]\nnamed=['noop','noop']\n",
        ),
        (
            "blank-subagent",
            "id='agent'\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\nsubagents=['']\n",
        ),
        (
            "duplicate-subagent",
            "id='agent'\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\nsubagents=['worker','worker']\n",
        ),
    ] {
        let path = dir.path().join(format!("{name}.toml"));
        std::fs::write(&path, authored).unwrap();
        assert!(
            load_file::<Extensions>(&path).is_err(),
            "{name} must be rejected"
        );
    }
    let valid = dir.path().join("valid.toml");
    std::fs::write(
        &valid,
        "id='valid'\nwhen_to_use='task'\nsystem_prompt={inline='prompt'}\n",
    )
    .unwrap();
    let mut loaded: Vec<AgentDefinition> = Vec::new();
    load_dir(dir.path(), &mut loaded).unwrap();
    assert_eq!(
        loaded
            .iter()
            .map(|definition| definition.id.as_str())
            .collect::<Vec<_>>(),
        ["valid"]
    );
    assert_eq!(loaded[0].source, DefinitionSource::File(valid));
}
