use super::*;
use crate::{DefinitionSource, SubagentEntry};

fn def(id: &str, tier: AgentTier, subagents: &[&str]) -> AgentDefinition {
    AgentDefinition {
        id: id.into(),
        when_to_use: format!("{id} does things"),
        agent_tier: tier,
        subagents: subagents
            .iter()
            .map(|s| SubagentEntry::AgentId((*s).into()))
            .collect(),
        ..Default::default()
    }
}

#[test]
fn replacing_an_id_keeps_its_original_position() {
    let mut set = DefinitionSet::new();
    set.insert(def("a", AgentTier::Worker, &[]));
    set.insert(def("b", AgentTier::Worker, &[]));
    let mut replaced = def("a", AgentTier::Worker, &[]);
    replaced.when_to_use = "new".into();
    set.insert(replaced);
    let ids: Vec<_> = set.list().iter().map(|d| d.id.clone()).collect();
    assert_eq!(ids, ["a", "b"]);
    assert_eq!(set.get("a").unwrap().when_to_use, "new");
    assert_eq!(set.len(), 2);
}

#[test]
fn with_definitions_layers_without_touching_the_source() {
    let mut base = DefinitionSet::new();
    base.insert(def("a", AgentTier::Worker, &[]));
    let next = base.with_definitions([
        def("a", AgentTier::Chat, &[]),
        def("c", AgentTier::Worker, &[]),
    ]);
    assert_eq!(base.get("a").unwrap().agent_tier, AgentTier::Worker);
    assert_eq!(next.len(), 2);
    assert_eq!(next.get("a").unwrap().agent_tier, AgentTier::Chat);
}

#[test]
fn tier_hierarchy_rejects_worker_parents_and_same_tier_edges() {
    let mut set = DefinitionSet::new();
    set.insert(def("w", AgentTier::Worker, &["x"]));
    assert!(
        set.validate_tier_hierarchy()
            .unwrap_err()
            .to_string()
            .contains("leaf executors")
    );

    let mut set = DefinitionSet::new();
    set.insert(def("r1", AgentTier::Reasoning, &["r2"]));
    set.insert(def("r2", AgentTier::Reasoning, &[]));
    assert!(
        set.validate_tier_hierarchy()
            .unwrap_err()
            .to_string()
            .contains("r1")
    );
}

#[test]
fn tier_hierarchy_ignores_unknown_children_and_accepts_descent() {
    let mut set = DefinitionSet::new();
    set.insert(def("r", AgentTier::Reasoning, &["w", "ghost"]));
    set.insert(def("w", AgentTier::Worker, &[]));
    set.validate_tier_hierarchy().unwrap();
}

#[test]
fn load_dirs_overrides_in_order_skips_missing_and_validates_merge() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let toml = |id: &str, desc: &str, extra: &str| {
        format!(
            "id = \"{id}\"\nwhen_to_use = \"{desc}\"\nsystem_prompt = {{ inline = \"p\" }}\n{extra}"
        )
    };
    std::fs::write(first.path().join("a.toml"), toml("a", "first", "")).unwrap();
    std::fs::write(second.path().join("a.toml"), toml("a", "second", "")).unwrap();
    let missing = first.path().join("nope");

    let mut set: DefinitionSet = DefinitionSet::new();
    set.insert(def("builtin", AgentTier::Worker, &[]));
    set.load_dirs(
        &[first.path(), &missing, first.path(), second.path()],
        DefinitionValidation::Diagnostics,
    )
    .unwrap();
    assert_eq!(set.len(), 2);
    let a = set.get("a").unwrap();
    assert_eq!(a.when_to_use, "second");
    assert!(matches!(a.source, DefinitionSource::File(_)));

    std::fs::write(
        second.path().join("bad.toml"),
        toml("bad", "x", "agent_tier = \"worker\"\nsubagents = [\"a\"]\n"),
    )
    .unwrap();
    let err = set
        .load_dirs(&[second.path()], DefinitionValidation::Diagnostics)
        .unwrap_err();
    assert!(err.to_string().contains("bad"));
    // A failed load leaves the set untouched.
    assert!(set.get("bad").is_none());
    assert_eq!(set.len(), 2);
}
