use super::*;

#[cfg(feature = "json-schema")]
#[test]
fn context_config_exposes_a_json_schema_for_host_configuration() {
    fn accepts_schema_type<T: schemars::JsonSchema>() {}
    accepts_schema_type::<ContextConfig>();
    accepts_schema_type::<CompactionStrategy>();

    let schema = schemars::schema_for!(ContextConfig);
    let value = serde_json::to_value(schema).unwrap();
    let properties = value["properties"].as_object().unwrap();

    for field in [
        "enabled",
        "microcompact_enabled",
        "autocompact_enabled",
        "microcompact_keep_recent",
        "tool_result_budget_bytes",
        "summarizer_payload_threshold_tokens",
        "summarizer_max_payload_tokens",
        "summarizer_model",
        "prefer_markdown_tool_output",
        "compaction_enabled",
        "compaction_trigger_tokens",
        "compaction_strategy",
    ] {
        assert!(properties.contains_key(field), "missing {field}");
    }

    for field in [
        "enabled",
        "microcompact_enabled",
        "autocompact_enabled",
        "prefer_markdown_tool_output",
        "compaction_enabled",
    ] {
        assert_eq!(properties[field]["type"], "boolean", "{field}");
    }
    for field in [
        "microcompact_keep_recent",
        "tool_result_budget_bytes",
        "summarizer_payload_threshold_tokens",
        "summarizer_max_payload_tokens",
    ] {
        assert_eq!(properties[field]["type"], "integer", "{field}");
    }
    assert_eq!(
        properties["compaction_trigger_tokens"]["type"],
        serde_json::json!(["integer", "null"])
    );
    assert_eq!(
        properties["summarizer_model"]["type"],
        serde_json::json!(["string", "null"])
    );
    assert_eq!(
        properties["compaction_strategy"]["$ref"],
        "#/$defs/CompactionStrategy"
    );
    assert_eq!(
        value["$defs"]["CompactionStrategy"]["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .map(|variant| variant["const"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["task_state", "summary"]
    );

    let defaults = [
        ("enabled", serde_json::json!(true)),
        ("microcompact_enabled", serde_json::json!(true)),
        ("autocompact_enabled", serde_json::json!(true)),
        ("microcompact_keep_recent", serde_json::json!(5)),
        ("tool_result_budget_bytes", serde_json::json!(16_384)),
        (
            "summarizer_payload_threshold_tokens",
            serde_json::json!(4_000),
        ),
        ("summarizer_max_payload_tokens", serde_json::json!(64_000)),
        ("summarizer_model", serde_json::Value::Null),
        ("prefer_markdown_tool_output", serde_json::json!(true)),
        ("compaction_enabled", serde_json::json!(true)),
        ("compaction_trigger_tokens", serde_json::Value::Null),
        ("compaction_strategy", serde_json::json!("task_state")),
    ];
    for (field, expected) in defaults {
        assert_eq!(properties[field]["default"], expected, "{field}");
    }
}

#[test]
fn context_config_preserves_defaults_aliases_and_strategy_spellings() {
    let default = ContextConfig::default();
    let parsed: ContextConfig = toml::from_str("").unwrap();
    assert_eq!(
        serde_json::to_value(parsed).unwrap(),
        serde_json::to_value(&default).unwrap()
    );

    let canonical = r#"
enabled = false
microcompact_enabled = false
autocompact_enabled = true
microcompact_keep_recent = 9
tool_result_budget_bytes = 1234
summarizer_payload_threshold_tokens = 4000
summarizer_max_payload_tokens = 64000
summarizer_model = "test-model"
prefer_markdown_tool_output = false
compaction_enabled = true
compaction_trigger_tokens = 123
compaction_strategy = "summary"
"#;
    let value: ContextConfig = toml::from_str(canonical).unwrap();
    assert!(!value.enabled);
    assert!(!value.microcompact_enabled);
    assert!(value.autocompact_enabled);
    assert_eq!(value.microcompact_keep_recent, 9);
    assert_eq!(value.tool_result_budget_bytes, 1234);
    assert_eq!(value.summarizer_payload_threshold_tokens, 4000);
    assert_eq!(value.summarizer_max_payload_tokens, 64000);
    assert_eq!(value.summarizer_model.as_deref(), Some("test-model"));
    assert!(!value.prefer_markdown_tool_output);
    assert!(value.compaction_enabled);
    assert_eq!(value.compaction_trigger_tokens, Some(123));
    assert_eq!(value.compaction_settings().trigger_tokens, Some(123));
    assert_eq!(
        value.compaction_settings().strategy,
        CompactionStrategy::Summary
    );

    let aliases: ContextConfig =
        toml::from_str("summarizer_payload_threshold_bytes=41\nsummarizer_max_payload_bytes=42")
            .unwrap();
    assert_eq!(aliases.summarizer_payload_threshold_tokens, 41);
    assert_eq!(aliases.summarizer_max_payload_tokens, 42);
    for spelling in ["task_state", "typed", "s4"] {
        assert_eq!(
            CompactionStrategy::parse(spelling),
            Some(CompactionStrategy::TaskState)
        );
    }
    for spelling in ["summary", "free_form", "s0"] {
        assert_eq!(
            CompactionStrategy::parse(spelling),
            Some(CompactionStrategy::Summary)
        );
    }
    assert_eq!(
        CompactionStrategy::parse("  TASK-STATE  "),
        Some(CompactionStrategy::TaskState)
    );
    assert_eq!(CompactionStrategy::parse("unknown"), None);
    let mut zero = value.clone();
    zero.compaction_trigger_tokens = Some(0);
    assert_eq!(zero.compaction_settings().trigger_tokens, None);
    let encoded = toml::to_string(&default).unwrap();
    let round_trip: ContextConfig = toml::from_str(&encoded).unwrap();
    assert_eq!(
        serde_json::to_value(round_trip).unwrap(),
        serde_json::to_value(default).unwrap()
    );
}
