use tinyagents_definition::AgentDefinition;

#[test]
fn every_bundled_agent_parses_without_changing_its_authored_vocabulary() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/openhuman");
    let mut paths: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    paths.sort();
    assert_eq!(paths.len(), 12);
    let expected_caps = std::collections::BTreeMap::from([
        ("orchestrator", 200),
        ("planner", 50),
        ("task_manager_agent", 50),
        ("critic", 5),
        ("image_agent", 8),
        ("morning_briefing", 8),
        ("summarizer", 1),
        ("trigger_reactor", 6),
        ("trigger_triage", 2),
        ("video_agent", 8),
        ("vision_agent", 6),
        ("presentation_agent", 10),
    ]);
    for path in paths {
        let text = std::fs::read_to_string(&path).unwrap();
        let authored: toml::Value = toml::from_str(&text).unwrap();
        let definition: AgentDefinition =
            toml::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        assert_eq!(
            definition.effective_max_iterations(),
            expected_caps[definition.id.as_str()]
        );
        assert_eq!(
            definition.source,
            tinyagents_definition::DefinitionSource::Builtin
        );
        let serialized = toml::to_string(&definition).unwrap();
        let roundtrip: AgentDefinition = toml::from_str(&serialized).unwrap();
        let projected = serde_json::to_value(&definition).unwrap();
        assert_eq!(serde_json::to_value(roundtrip).unwrap(), projected);

        for (field, value) in authored.as_table().unwrap() {
            let value = if field == "subagents" {
                value.get("allowlist").unwrap_or(value)
            } else {
                value
            };
            assert_eq!(
                projected.get(field),
                Some(&serde_json::to_value(value).unwrap()),
                "{} field {field}",
                path.display()
            );
        }
    }
}
