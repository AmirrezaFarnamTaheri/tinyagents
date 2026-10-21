use super::*;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinytools::{PermissionLevel, Tool, ToolPolicyEngine, ToolResult};

static PACKS: &[ToolPack] = &[ToolPack {
    id: "code",
    summary: "Read and modify source.",
    tools: &["preview_patch", "delete_source"],
    owners: &[],
    guide: "",
}];

const CATALOG: PackCatalog = PackCatalog::new(PACKS, "[missing]");

struct CodeTool(&'static str);

#[async_trait]
impl Tool for CodeTool {
    fn name(&self) -> &str {
        self.0
    }

    fn description(&self) -> &str {
        "permission fixture"
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn permission_level(&self) -> PermissionLevel {
        if self.0 == "delete_source" {
            PermissionLevel::Dangerous
        } else {
            PermissionLevel::ReadOnly
        }
    }

    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        if self.0 == "delete_source" {
            PermissionLevel::Dangerous
        } else if args.get("dry_run") == Some(&Value::Bool(true)) {
            PermissionLevel::ReadOnly
        } else {
            PermissionLevel::Write
        }
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(args.to_string()))
    }
}

fn tools() -> (Arc<Vec<Box<dyn Tool>>>, Arc<UseSkillTool>) {
    let handle = PackRegistryHandle::default();
    let tools: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![
        Box::new(CodeTool("preview_patch")),
        Box::new(CodeTool("delete_source")),
    ]);
    handle.bind(Arc::downgrade(&tools));
    (tools, Arc::new(UseSkillTool::new(handle, CATALOG)))
}

#[test]
fn read_only_session_keeps_mixed_pack_disclosure_and_preview_available() {
    let (tools, dispatcher) = tools();
    let mut refs: Vec<&dyn Tool> = tools.iter().map(AsRef::as_ref).collect();
    refs.push(dispatcher.as_ref());
    let policy = ToolPolicyEngine::build_session_from_refs(
        "reader",
        "chat",
        "test",
        &HashMap::from([("chat".to_owned(), "read_only".to_owned())]),
        &refs,
        &HashSet::new(),
    );

    assert!(policy.is_allowed(USE_SKILL));
    assert!(policy.is_allowed("preview_patch"));
    assert!(!policy.is_allowed("delete_source"));
    assert_eq!(
        dispatcher.permission_level_with_args(&json!({"skill": "code"})),
        PermissionLevel::ReadOnly
    );
    assert_eq!(
        dispatcher.permission_level_with_args(&json!({
            "skill": "code", "tool": "delete_source"
        })),
        PermissionLevel::Dangerous
    );
}

#[test]
fn packed_preview_permission_requires_literal_true_and_survives_binding() {
    let (_tools, dispatcher) = tools();
    let mut registry = tinyagents_harness::tool::ToolRegistry::<(), ()>::new();
    registry.register(dispatcher.clone());
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();

    for (arguments, expected) in [
        (json!({"dry_run": true}), PermissionLevel::ReadOnly),
        (json!({"dry_run": false}), PermissionLevel::Write),
        (json!({"dry_run": "true"}), PermissionLevel::Write),
        (json!({"dry_run": null}), PermissionLevel::Write),
        (json!({}), PermissionLevel::Write),
    ] {
        let call = json!({"skill": "code", "tool": "preview_patch", "args": arguments});
        assert_eq!(dispatcher.permission_level_with_args(&call), expected);
        let bound = dispatch.bind_call(&call).unwrap().unwrap();
        assert_eq!(bound.tool().permission_level(), PermissionLevel::ReadOnly);
        assert_eq!(
            bound.tool().permission_level_with_args(&json!({
                "skill": "code", "tool": "preview_patch", "args": {"dry_run": true}
            })),
            expected,
            "a wrapper cannot replace the arguments whose permission was admitted"
        );
    }
}

#[test]
fn unresolved_named_calls_keep_the_ceiling_even_in_an_all_read_only_registry() {
    let handle = PackRegistryHandle::default();
    let tools: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![Box::new(CodeTool("preview_patch"))]);
    handle.bind(Arc::downgrade(&tools));
    let dispatcher = Arc::new(UseSkillTool::new(handle, CATALOG));
    let mut registry = tinyagents_harness::tool::ToolRegistry::<(), ()>::new();
    registry.register(dispatcher.clone());
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();

    for call in [
        json!({"skill": "code", "tool": "missing"}),
        json!({"skill": "wrong", "tool": "preview_patch"}),
        json!({"tool": "preview_patch"}),
    ] {
        assert_eq!(
            dispatcher.permission_level_with_args(&call),
            PermissionLevel::Dangerous
        );
        let bound = dispatch.bind_call(&call).unwrap().unwrap();
        assert_eq!(
            bound
                .tool()
                .permission_level_with_args(&json!({"skill": "code"})),
            PermissionLevel::Dangerous,
            "an unresolved named target keeps its admission ceiling after binding"
        );
        assert!(bound.admission_target().is_none());
    }
}

#[test]
fn pinned_disclosure_stays_read_only_in_a_mixed_pack() {
    let (_tools, dispatcher) = tools();
    let mut registry = tinyagents_harness::tool::ToolRegistry::<(), ()>::new();
    registry.register(dispatcher);
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();
    let bound = dispatch
        .bind_call(&json!({"skill": "code"}))
        .unwrap()
        .unwrap();

    assert_eq!(bound.tool().permission_level(), PermissionLevel::ReadOnly);
    assert_eq!(
        bound.tool().permission_level_with_args(&json!({
            "skill": "code", "tool": "delete_source"
        })),
        PermissionLevel::ReadOnly
    );
    assert!(bound.admission_target().is_none());
}
