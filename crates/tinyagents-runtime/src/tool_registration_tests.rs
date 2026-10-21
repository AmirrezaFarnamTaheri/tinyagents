use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn set(names: &[&str]) -> HashSet<String> {
    names.iter().map(|n| n.to_string()).collect()
}

fn open() -> ToolAdmission {
    ToolAdmission {
        has_thread: true,
        ..Default::default()
    }
}

struct CountingPolicy(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl ToolCallPolicy<()> for CountingPolicy {
    fn name(&self) -> &str {
        "counting-host"
    }
    async fn check(&self, _: &RunContext<()>, _: &ToolCall) -> PolicyDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        PolicyDecision::Allow
    }
}

fn policy_call(name: &str) -> ToolCall {
    ToolCall {
        id: "call-1".into(),
        name: name.into(),
        arguments: serde_json::json!({}),
        invalid: None,
    }
}

#[tokio::test]
async fn host_only_policy_denies_before_delegating_and_allows_named_calls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy = HostOnlyToolPolicy::new(
        ["read_file".to_owned()],
        Some(Arc::new(CountingPolicy(calls.clone()))),
    );
    let context = RunContext::new(
        tinyagents_harness::context::RunConfig::new("host-only-test"),
        (),
    );
    let denied = policy.check(&context, &policy_call("write_file")).await;
    assert!(matches!(denied, PolicyDecision::Deny { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        policy.check(&context, &policy_call("read_file")).await,
        PolicyDecision::Allow
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let empty = HostOnlyToolPolicy::new(Vec::<String>::new(), None);
    assert!(matches!(
        empty.check(&context, &policy_call("read_file")).await,
        PolicyDecision::Deny { .. }
    ));
}

fn tool_spec(name: &str, description: &str) -> Arc<ToolSpec> {
    Arc::new(ToolSpec {
        name: name.into(),
        description: description.into(),
        parameters: serde_json::json!({"type":"object"}),
    })
}

#[test]
fn visible_spec_dedup_keeps_first_order_and_allocation() {
    let first = tool_spec("read", "first");
    let duplicate = tool_spec("read", "duplicate");
    let second = tool_spec("write", "write");
    let result = dedup_visible_tool_specs(vec![first.clone(), duplicate, second.clone()]);
    assert_eq!(result.len(), 2);
    assert!(Arc::ptr_eq(&result[0], &first));
    assert!(Arc::ptr_eq(&result[1], &second));
}

#[test]
fn durable_declarations_remove_synthesized_name_collisions() {
    let durable = tool_spec("search", "durable");
    let search = tool_spec("search", "synthesized");
    let render = tool_spec("render", "render");
    let result = drop_synthesized_name_collisions(&[durable], vec![search, render.clone()]);
    assert_eq!(result.len(), 1);
    assert!(Arc::ptr_eq(&result[0], &render));
}

#[test]
fn no_allowlist_admits_all_deduplicated_in_order() {
    let plan = plan_registration(["b", "a", "b"], &open());
    assert_eq!(plan.admitted, ["b", "a"]);
    assert_eq!(plan.candidates, ["b", "a"]);
}

#[test]
fn empty_allowlist_denies_everything() {
    let admission = ToolAdmission {
        allowed: Some(HashSet::new()),
        ..open()
    };
    let plan = plan_registration(["a", "b"], &admission);
    assert!(plan.admitted.is_empty());
    assert_eq!(plan.candidates.len(), 2);
}

#[test]
fn allowlist_admits_exact_members() {
    let admission = ToolAdmission {
        allowed: Some(set(&["a"])),
        ..open()
    };
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
fn bounded_subagent_spawn_requires_depth_room_and_exact_allowlisting() {
    let below_cap = ToolAdmission {
        allowed: Some(set(&["spawn", "read"])),
        is_subagent_run: true,
        spawn_tools: set(&["spawn"]),
        nested_spawn_depth: Some(NestedSpawnDepth::try_new(1, 3).unwrap()),
        ..open()
    };
    let plan = plan_registration(["spawn", "read", "other"], &below_cap);
    assert_eq!(plan.admitted, ["spawn", "read"]);
    assert!(plan.spawn_refused.is_empty());

    let at_cap = ToolAdmission {
        nested_spawn_depth: Some(NestedSpawnDepth::try_new(3, 3).unwrap()),
        ..below_cap.clone()
    };
    let plan = plan_registration(["spawn", "read"], &at_cap);
    assert_eq!(plan.admitted, ["read"]);
    assert_eq!(plan.spawn_refused, ["spawn"]);

    let not_allowlisted = ToolAdmission {
        allowed: Some(set(&["read"])),
        nested_spawn_depth: Some(NestedSpawnDepth::try_new(1, 3).unwrap()),
        ..below_cap
    };
    let plan = plan_registration(["spawn", "read"], &not_allowlisted);
    assert_eq!(plan.admitted, ["read"]);
    assert!(plan.spawn_refused.is_empty());
}

#[test]
fn nested_spawn_depth_rejects_invalid_or_overflowing_depths() {
    assert!(NestedSpawnDepth::try_new(4, 3).is_none());
    assert!(!NestedSpawnDepth::try_new(u32::MAX, u32::MAX)
        .unwrap()
        .allows_child());
}

#[test]
fn root_run_keeps_spawn_tools() {
    let admission = ToolAdmission {
        spawn_tools: set(&["spawn"]),
        ..open()
    };
    assert_eq!(plan_registration(["spawn"], &admission).admitted, ["spawn"]);
}

#[test]
fn tool_surface_diff_reports_refresh_changes_in_stable_order() {
    let diff = tool_surface_diff(&set(&["keep", "revoke", "a"]), &set(&["keep", "new", "b"]));
    assert_eq!(diff.added, ["b", "new"]);
    assert_eq!(diff.removed, ["a", "revoke"]);
    assert_eq!(diff.retained, ["keep"]);
}
