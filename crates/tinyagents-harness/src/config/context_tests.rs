use super::*;

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
