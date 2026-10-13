use super::*;

fn set(names: &[&str]) -> HashSet<String> {
    names.iter().map(|n| n.to_string()).collect()
}

fn open() -> ToolAdmission {
    ToolAdmission { has_thread: true, ..Default::default() }
}

#[test]
fn no_allowlist_admits_all_deduplicated_in_order() {
    let plan = plan_registration(["b", "a", "b"], &open());
    assert_eq!(plan.admitted, ["b", "a"]);
    assert_eq!(plan.candidates, ["b", "a"]);
}

#[test]
fn empty_allowlist_denies_everything() {
    let admission = ToolAdmission { allowed: Some(HashSet::new()), ..open() };
    let plan = plan_registration(["a", "b"], &admission);
    assert!(plan.admitted.is_empty());
    assert_eq!(plan.candidates.len(), 2);
}

#[test]
fn allowlist_admits_exact_members() {
    let admission = ToolAdmission { allowed: Some(set(&["a"])), ..open() };
    assert_eq!(plan_registration(["a", "b"], &admission).admitted, ["a"]);
}

#[test]
fn thread_scoped_tools_dropped_without_thread() {
    let admission = ToolAdmission {
        has_thread: false,
        thread_scoped: set(&["goal_get"]),
        ..Default::default()
    };
    let plan = plan_registration(["goal_get", "a"], &admission);
    assert_eq!(plan.candidates, ["a"]);
    assert_eq!(plan.admitted, ["a"]);
}

#[test]
fn subagent_never_gets_spawn_tools_even_if_allowlisted() {
    let admission = ToolAdmission {
        allowed: Some(set(&["spawn", "a"])),
        is_subagent_run: true,
        spawn_tools: set(&["spawn"]),
        ..open()
    };
    let plan = plan_registration(["spawn", "a"], &admission);
    assert_eq!(plan.admitted, ["a"]);
    assert_eq!(plan.spawn_refused, ["spawn"]);
}

#[test]
fn root_run_keeps_spawn_tools() {
    let admission = ToolAdmission { spawn_tools: set(&["spawn"]), ..open() };
    assert_eq!(plan_registration(["spawn"], &admission).admitted, ["spawn"]);
}
