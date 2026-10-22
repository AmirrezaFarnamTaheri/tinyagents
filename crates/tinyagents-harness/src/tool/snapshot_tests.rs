use super::*;
use tinytools::ToolSpec;

fn spec(name: &str, description: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        description: description.into(),
        parameters: serde_json::json!({"type":"object"}),
    }
}

#[test]
fn snapshot_deduplicates_equal_declarations_and_sorts_names() {
    let snapshot = ToolSnapshot::new(vec![spec("z", "z"), spec("a", "a"), spec("a", "a")]).unwrap();
    assert_eq!(
        snapshot
            .specs()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
}

#[test]
fn snapshot_rejects_conflicting_declarations() {
    assert_eq!(
        ToolSnapshot::new(vec![spec("read", "first"), spec("read", "second")]).unwrap_err(),
        ToolSnapshotError::NameCollision("read".into())
    );
}

#[test]
fn snapshot_json_round_trip_preserves_exact_flag_semantics() {
    let snapshot = ToolSnapshot::new(vec![spec("read", "read")]).unwrap();
    let restored = ToolSnapshot::from_json(&snapshot.to_json()).unwrap();
    assert_eq!(restored.to_json(), snapshot.to_json());
    assert!(!restored.is_exact());
}
