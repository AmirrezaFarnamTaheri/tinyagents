use super::*;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_harness::tool::{BoundToolCall, CanonicalSharedToolAdapter};
use tinytools::{PermissionLevel, Tool, ToolPolicyEngine, ToolResult};

struct PackExtension;

static PACK_EXTENSION: PackExtension = PackExtension;

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

    fn host_extension(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        Some(&PACK_EXTENSION)
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(args.to_string()))
    }
}

fn tools() -> (PackRegistryHandle, Arc<Vec<Box<dyn Tool>>>, Arc<UseSkillTool>) {
    let handle = PackRegistryHandle::default();
    let tools: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![
        Box::new(CodeTool("preview_patch")),
        Box::new(CodeTool("delete_source")),
    ]);
    handle.bind(Arc::downgrade(&tools));
    (handle.clone(), tools, Arc::new(UseSkillTool::new(handle, CATALOG)))
}

#[test]
fn read_only_session_keeps_mixed_pack_disclosure_and_preview_available() {
    let (_handle, tools, dispatcher) = tools();
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
    let (handle, _tools, dispatcher) = tools();

    for (arguments, expected) in [
        (json!({"dry_run": true}), PermissionLevel::ReadOnly),
        (json!({"dry_run": false}), PermissionLevel::Write),
        (json!({"dry_run": "true"}), PermissionLevel::Write),
        (json!({"dry_run": null}), PermissionLevel::Write),
        (json!({}), PermissionLevel::Write),
    ] {
        let call = json!({"skill": "code", "tool": "preview_patch", "args": arguments});
        assert_eq!(dispatcher.permission_level_with_args(&call), expected);
        let bound = handle.bind_call(dispatcher.clone(), &call);
        assert_eq!(bound.permission_level(), PermissionLevel::ReadOnly);
        assert_eq!(
            bound.permission_level_with_args(&json!({
                "skill": "code", "tool": "preview_patch", "args": {"dry_run": true}
            })),
            expected,
            "a wrapper cannot replace the arguments whose permission was admitted"
        );
    }
}

#[test]
fn shared_tool_adapter_preserves_packed_host_extension() {
    let (_handle, tools, _dispatcher) = tools();
    let adapter = CanonicalSharedToolAdapter::for_name(vec![tools], "preview_patch")
        .expect("registered packed tool");

    assert!(adapter
        .host_extension()
        .is_some_and(|extension| extension.is::<PackExtension>()));
}

#[test]
fn unresolved_named_calls_keep_the_ceiling_even_in_an_all_read_only_registry() {
    let handle = PackRegistryHandle::default();
    let tools: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![Box::new(CodeTool("preview_patch"))]);
    handle.bind(Arc::downgrade(&tools));
    let dispatcher = Arc::new(UseSkillTool::new(handle, CATALOG));


    for call in [
        json!({"skill": "code", "tool": "missing"}),
        json!({"skill": "wrong", "tool": "preview_patch"}),
        json!({"tool": "preview_patch"}),
    ] {
        assert_eq!(
            dispatcher.permission_level_with_args(&call),
            PermissionLevel::Dangerous
        );
        let bound = handle.bind_call(dispatcher.clone(), &call);
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
    let (handle, _tools, dispatcher) = tools();
    let bound = handle.bind_call(dispatcher, &json!({"skill": "code"}));

    assert_eq!(bound.permission_level(), PermissionLevel::ReadOnly);
    assert_eq!(
        bound.permission_level_with_args(&json!({
            "skill": "code", "tool": "delete_source"
        })),
        PermissionLevel::ReadOnly
    );
    assert!(bound.admission_target().is_none());
}
