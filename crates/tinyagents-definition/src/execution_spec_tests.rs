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
