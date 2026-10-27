#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use tinyagents_harness::testkit::FakeTool;
use tinytools::ToolExposure;

fn set(names: &[&str]) -> SharedToolSet {
    Arc::new(
        names
            .iter()
            .map(|name| Box::new(FakeTool::new(*name)) as Box<dyn Tool>)
            .collect(),
    )
}

fn registered(builder: &ToolSetBuilder<(), ()>) -> (Vec<String>, AgentHarness<(), ()>) {
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    let report = builder.register(&mut harness, None);
    (report.registered, harness)
}

#[test]
fn registers_each_name_once_across_sets_in_first_seen_order() {
    let builder = ToolSetBuilder::<(), ()>::new()
        .set(set(&["a", "b"]))
        .set(set(&["b", "c"]));
    assert_eq!(builder.candidate_names(), ["a", "b", "c"]);
    assert_eq!(registered(&builder).0, ["a", "b", "c"]);
}

#[test]
fn synthesized_refresh_diffs_only_dynamic_sets_and_replaces_executors() {
    let mut builder = ToolSetBuilder::<(), ()>::new()
        .set(set(&["static"]))
        .synthesized_set(set(&["old", "kept"]));
    let diff = builder.refresh_synthesized_sets([set(&["kept", "new"])]);
    assert_eq!(diff.added, ["new"]);
    assert_eq!(diff.removed, ["old"]);
    assert_eq!(diff.retained, ["kept"]);
    assert_eq!(builder.candidate_names(), ["static", "kept", "new"]);
    assert_eq!(registered(&builder).0, ["static", "kept", "new"]);
}

#[test]
fn an_empty_allowlist_denies_everything_and_none_admits_all() {
    let deny_all = ToolSetBuilder::<(), ()>::new()
        .set(set(&["a", "b"]))
        .admission(ToolAdmission {
            allowed: Some(Default::default()),
            ..ToolAdmission::default()
        });
    assert!(registered(&deny_all).0.is_empty());

    let one = ToolSetBuilder::<(), ()>::new()
        .set(set(&["a", "b"]))
        .admission(ToolAdmission {
            allowed: Some(["b".to_string()].into()),
            ..ToolAdmission::default()
        });
    assert_eq!(registered(&one).0, ["b"]);

    assert_eq!(
        registered(&ToolSetBuilder::<(), ()>::new().set(set(&["a"]))).0,
        ["a"]
    );
}

#[test]
fn thread_scoped_tools_are_dropped_without_a_thread() {
    let admission = |has_thread| ToolAdmission {
        has_thread,
        thread_scoped: ["goal".to_string()].into(),
        ..ToolAdmission::default()
    };
    let without = ToolSetBuilder::<(), ()>::new()
        .set(set(&["goal", "a"]))
        .admission(admission(false));
    assert_eq!(registered(&without).0, ["a"]);
    let with = ToolSetBuilder::<(), ()>::new()
        .set(set(&["goal", "a"]))
        .admission(admission(true));
    assert_eq!(registered(&with).0, ["goal", "a"]);
}

#[test]
fn session_deferred_tools_leave_the_direct_surface() {
    let builder = ToolSetBuilder::<(), ()>::new()
        .set(set(&["a", "b"]))
        .deferred(["b".to_string()]);
    let (_, harness) = registered(&builder);
    let direct: Vec<_> = harness
        .tools()
        .schemas()
        .into_iter()
        .map(|s| s.name)
        .collect();
    let deferred: Vec<_> = harness
        .tools()
        .deferred_schemas()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(direct, ["a"]);
    assert_eq!(deferred, ["b"]);
    assert_ne!(ToolExposure::Direct, ToolExposure::Deferred);
}

struct HiddenTool;

#[async_trait::async_trait]
impl Tool for HiddenTool {
    fn name(&self) -> &str {
        "hidden"
    }
    fn description(&self) -> &str {
        "named by hand only"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    fn exposure(&self) -> ToolExposure {
        ToolExposure::Hidden
    }
    async fn execute(&self, _: serde_json::Value) -> anyhow::Result<tinytools::ToolResult> {
        Ok(tinytools::ToolResult::success("ok"))
    }
}

fn with_hidden() -> SharedToolSet {
    Arc::new(vec![
        Box::new(FakeTool::new("visible")) as Box<dyn Tool>,
        Box::new(HiddenTool),
    ])
}

#[test]
fn a_wildcard_never_exposes_a_hidden_tool_but_an_allowlist_may_name_it() {
    let wildcard = ToolSetBuilder::<(), ()>::new().set(with_hidden());
    assert_eq!(registered(&wildcard).0, ["visible"]);

    let named = ToolSetBuilder::<(), ()>::new()
        .set(with_hidden())
        .admission(ToolAdmission {
            allowed: Some(["hidden".to_string()].into()),
            ..ToolAdmission::default()
        });
    assert_eq!(registered(&named).0, ["hidden"]);
}
