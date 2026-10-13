use super::*;

fn named(names: &[&str]) -> ToolScope {
    ToolScope::Named(names.iter().map(|n| n.to_string()).collect())
}

#[test]
fn a_named_scope_listing_a_spawn_tool_is_detected() {
    for spawn in SPAWN_TOOL_NAMES {
        assert!(named(&["memory", spawn]).names_a_spawn_tool(), "{spawn}");
    }
    assert!(!named(&["memory", "cron"]).names_a_spawn_tool());
    assert!(!named(&[]).names_a_spawn_tool());
    assert!(
        !ToolScope::Wildcard.names_a_spawn_tool(),
        "a wildcard names no tool"
    );
}

#[derive(serde::Deserialize)]
struct Holder {
    tools: ToolScope,
}

#[test]
fn every_tool_scope_spelling_in_existing_toml_still_parses() {
    let parse = |src: &str| toml::from_str::<Holder>(src).unwrap().tools;
    assert_eq!(parse("[tools]\nwildcard = {}\n"), ToolScope::Wildcard);
    assert_eq!(parse("tools = \"wildcard\"\n"), ToolScope::Wildcard);
    assert_eq!(
        parse("[tools]\nnamed = [\"a\", \"b\"]\n"),
        named(&["a", "b"])
    );
    assert_eq!(parse("tools = [\"a\"]\n"), named(&["a"]));
    assert_eq!(parse("[tools]\nnamed = []\n"), named(&[]));
}
