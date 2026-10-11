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
    for path in paths {
        let text = std::fs::read_to_string(&path).unwrap();
        let authored: toml::Value = toml::from_str(&text).unwrap();
        let definition: AgentDefinition =
            toml::from_str(&text).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let projected = serde_json::to_value(definition).unwrap();
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
