use super::*;

#[test]
fn scope_starts_empty_and_reports_added_callbacks() {
    let scope = HookScope::<(), TurnCost>::default();
    assert!(scope.is_empty());
    assert!(scope.tool_hooks().is_empty());
    assert!(scope.learning_hooks().is_empty());
    assert!(scope.stop_hooks().is_empty());
}
