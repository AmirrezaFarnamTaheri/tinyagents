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

#[derive(Clone, Debug, PartialEq, Eq)]
struct HostAlpha(u8);
#[derive(Clone, Debug, PartialEq, Eq)]
struct HostBeta(&'static str);

#[test]
fn typed_extensions_survive_snapshot_copies_without_changing_json() {
    let original_json = serde_json::json!([]);
    let snapshot = ToolSnapshot::from_json(&original_json)
        .unwrap()
        .with_extension(HostAlpha(7))
        .with_extension(HostBeta("host-owned"));
    assert_eq!(snapshot.extension::<HostAlpha>(), Some(&HostAlpha(7)));
    assert_eq!(
        snapshot.extension::<HostBeta>(),
        Some(&HostBeta("host-owned"))
    );
    assert_eq!(snapshot.to_json(), original_json);

    let clone = snapshot.clone();
    let narrowed = snapshot.retaining(|_| true);
    let (retained, count) = snapshot.with_retained(&ToolSnapshot::default()).unwrap();
    assert_eq!(count, 0);
    for copy in [&clone, &narrowed, &retained] {
        assert_eq!(copy.extension::<HostAlpha>(), Some(&HostAlpha(7)));
        assert_eq!(copy.extension::<HostBeta>(), Some(&HostBeta("host-owned")));
        assert_eq!(copy.to_json(), original_json);
    }
    let replaced = clone.with_extension(HostAlpha(8));
    assert_eq!(replaced.extension::<HostAlpha>(), Some(&HostAlpha(8)));
    assert_eq!(snapshot.extension::<HostAlpha>(), Some(&HostAlpha(7)));
}

#[test]
fn exact_snapshot_never_retains_recorded_declarations() {
    let exact = ToolSnapshot::new(vec![spec("live", "live")])
        .unwrap()
        .exact();
    let recorded = ToolSnapshot::new(vec![spec("recorded", "recorded")]).unwrap();
    let (merged, count) = exact.with_retained(&recorded).unwrap();
    assert_eq!(count, 0);
    assert!(merged.is_exact());
    assert_eq!(
        merged
            .specs()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["live"]
    );
}
