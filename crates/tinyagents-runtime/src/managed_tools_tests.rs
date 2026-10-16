use super::*;
use std::sync::Arc;

const OPEN: &str = "<perm>\n";
const CLOSE: &str = "</perm>";

fn spec(name: &str) -> Arc<ToolSpec> {
    Arc::new(ToolSpec {
        name: name.into(),
        description: format!("{name} desc"),
        parameters: serde_json::json!({"type": "object"}),
    })
}

fn names(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn managed_names_reconcile_revocation_deferred_and_permanent_sets() {
    let mut managed = ManagedToolNames {
        visible: names(&["old", "kept", "permanent"]),
        deferred: names(&["old", "hidden", "permanent"]),
        permanent: names(&["permanent"]),
    };
    managed.refresh_synthesized(
        &names(&["old", "kept"]),
        &names(&["kept", "new"]),
        false,
    );
    assert!(!managed.visible.contains("old"));
    assert!(!managed.deferred.contains("old"));
    assert!(!managed.visible.contains("new"));
    assert!(managed.visible.contains("permanent"));
    assert!(!managed.deferred.contains("permanent"));

    managed.defer_visible(&names(&["kept", "hidden", "permanent"]));
    assert!(!managed.visible.contains("kept"));
    assert!(managed.deferred.contains("kept"));
    assert!(!managed.deferred.contains("permanent"));
}

#[test]
fn wildcard_refresh_adds_new_synthesized_names() {
    let mut managed = ManagedToolNames {
        visible: names(&["old"]),
        ..ManagedToolNames::default()
    };
    managed.refresh_synthesized(&names(&["old"]), &names(&["new"]), true);
    assert!(!managed.visible.contains("old"));
    assert!(managed.visible.contains("new"));
}

#[test]
fn empty_names_leave_the_prefix_alone() {
    let prefix = PrefixSnapshot::new(vec![Message::system("base")]);
    assert!(refresh_prefix(&prefix, &[spec("a")], &names(&[]), OPEN, CLOSE).is_none());
}

#[test]
fn catalogue_is_sorted_appended_and_idempotent() {
    let prefix = PrefixSnapshot::new(vec![Message::system("base")]);
    let specs = [spec("b"), spec("a"), spec("c")];
    let refreshed =
        refresh_prefix(&prefix, &specs, &names(&["b", "a"]), OPEN, CLOSE).expect("changed");
    let messages = refreshed.messages();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].text(), "base");
    let text = messages[1].text();
    assert!(text.starts_with(OPEN) && text.ends_with(CLOSE));
    assert!(text.find("a: a desc").unwrap() < text.find("b: b desc").unwrap());
    assert!(!text.contains("c desc"));
    // Same catalogue again: no refresh needed.
    assert!(refresh_prefix(&refreshed, &specs, &names(&["b", "a"]), OPEN, CLOSE).is_none());
}

#[test]
fn collision_with_a_synthesized_tool_is_rejected() {
    struct T;
    #[async_trait::async_trait]
    impl tinytools::Tool for T {
        fn name(&self) -> &str {
            "dup"
        }
        fn description(&self) -> &str {
            ""
        }
        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({})
        }
        async fn execute(&self, _: serde_json::Value) -> anyhow::Result<tinytools::ToolResult> {
            Ok(tinytools::ToolResult::success(""))
        }
    }
    let synthesized: Vec<Box<dyn tinytools::Tool>> = vec![Box::new(T)];
    assert!(reject_synthesized_collisions(&names(&["dup"]), &synthesized).is_err());
    assert!(reject_synthesized_collisions(&names(&["other"]), &synthesized).is_ok());
}
