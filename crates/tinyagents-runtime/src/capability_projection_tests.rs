use super::*;

#[test]
fn descriptors_dedupe_and_skip_existing() {
    let mut registry = CapabilityRegistry::<()>::new();
    assert_eq!(
        project_descriptors(&mut registry, ComponentKind::Agent, ["a", "b", "a"]),
        2
    );
    assert_eq!(
        project_descriptors(&mut registry, ComponentKind::Agent, ["b", "c"]),
        1
    );
    assert_eq!(registry.names(ComponentKind::Agent).len(), 3);
}
