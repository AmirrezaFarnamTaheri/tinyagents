//! Mechanism tests over a two-pack test catalog. The product table's own
//! invariants (membership, owners, guides) are tested by the host that owns it.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinytools::{PermissionLevel, Tool, ToolResult, ToolSpec, ToolTimeout};

use super::*;
use crate::{runtime::AgentHarness, testkit, tool::ToolDispatch};
use tinyinference_llm::{message::{AssistantMessage, Message}, model::ModelResponse, providers::MockModel, tool::ToolCall, usage::Usage};

const MARKER: &str = "[not_found]";

static PACKS: &[ToolPack] = &[
    ToolPack {
        id: "alpha",
        summary: "Alpha things.",
        tools: &["a_one", "a_two"],
        owners: &["alpha_agent"],
        guide: "Use alpha carefully.",
    },
    ToolPack {
        id: "beta",
        summary: "Beta things.",
        tools: &["b_one"],
        owners: &[],
        guide: "",
    },
];

const CATALOG: PackCatalog = PackCatalog::new(PACKS, MARKER);

struct FakeTool {
    name: &'static str,
    level: PermissionLevel,
    external: bool,
    timeout: ToolTimeout,
    prepares_authorization: bool,
}

impl FakeTool {
    fn plain(name: &'static str) -> Self {
        Self {
            name,
            level: PermissionLevel::ReadOnly,
            external: false,
            timeout: ToolTimeout::Inherit,
            prepares_authorization: false,
        }
    }
}

#[async_trait]
impl Tool for FakeTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        "fake"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object", "properties": {"marker": {"type": "string"}}})
    }
    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(format!("{}:{}", self.name, args)))
    }
    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        Ok(self
            .prepares_authorization
            .then(|| json!({"inner_args": args})))
    }
    fn permission_level(&self) -> PermissionLevel {
        self.level
    }
    fn external_effect_with_args(&self, _args: &Value) -> bool {
        self.external
    }
    fn timeout_policy(&self, _args: &Value) -> ToolTimeout {
        self.timeout
    }
}

/// A durable registry holding `tools` plus a bound `use_skill`.
fn bound(tools: Vec<FakeTool>) -> Arc<Vec<Box<dyn Tool>>> {
    let handle = PackRegistryHandle::default();
    let mut all: Vec<Box<dyn Tool>> = tools.into_iter().map(|t| Box::new(t) as _).collect();
    all.push(Box::new(UseSkillTool::new(handle.clone(), CATALOG)));
    let all = Arc::new(all);
    handle.bind(Arc::downgrade(&all));
    all
}

fn handle_of(tools: &[Box<dyn Tool>]) -> &PackRegistryHandle {
    find(tools, USE_SKILL)
        .host_extension()
        .and_then(|any| any.downcast_ref::<PackRegistryHandle>())
        .expect("use_skill preserves PackRegistryHandle as its host extension")
}

fn find<'a>(tools: &'a [Box<dyn Tool>], name: &str) -> &'a dyn Tool {
    tools
        .iter()
        .find(|t| t.name() == name)
        .map(AsRef::as_ref)
        .unwrap_or_else(|| panic!("{name} missing"))
}

#[test]
fn the_use_skill_declaration_is_byte_stable() {
    let tools = bound(vec![]);
    let tool = find(&tools, USE_SKILL);
    assert_eq!(tool.name(), "use_skill");
    assert_eq!(
        tool.description(),
        "Reach a skill's tools. Their names, descriptions and argument schemas are NOT in \
         your context until you ask for them: call this with `skill` alone to see them, then \
         again with `skill` + `tool` + `args` to run one.\n\nSkills:\n\
         - `alpha` — Alpha things.\n- `beta` — Beta things.\n"
    );
    assert_eq!(
        tool.parameters_schema(),
        json!({
            "type": "object",
            "properties": {
                "skill": { "type": "string", "enum": ["alpha", "beta"], "description": "Skill to read or run a tool from." },
                "tool": { "type": "string", "description": "Tool to run. Omit to list the skill's tools and their arguments instead." },
                "args": {
                    "type": "object",
                    "description": "The tool's own arguments, as documented in the listing.",
                    "additionalProperties": true
                }
            },
            "required": ["skill"]
        })
    );
}

#[tokio::test]
async fn a_skill_alone_renders_guide_and_the_schema_of_a_present_tool() {
    let tools = bound(vec![FakeTool::plain("a_one")]);
    let result = find(&tools, USE_SKILL)
        .execute(json!({"skill": "alpha"}))
        .await
        .unwrap();
    assert!(!result.is_error);
    let rendered = result.text();
    assert!(rendered.starts_with("# Skill `alpha`\n\nAlpha things.\n\nUse alpha carefully.\n\n"));
    assert!(rendered.contains("## `a_one`\n\nfake\n\n```json\n"));
    assert!(rendered.contains(r#"{"properties":{"marker":{"type":"string"}},"type":"object"}"#));
    // A pack tool this session lacks is skipped, not fatal.
    assert!(!rendered.contains("a_two"));
}

#[tokio::test]
async fn an_unknown_skill_is_a_marked_error_listing_the_alternatives() {
    let tools = bound(vec![FakeTool::plain("a_one")]);
    let result = find(&tools, USE_SKILL)
        .execute(json!({"skill": "nope"}))
        .await
        .unwrap();
    assert!(result.is_error);
    let message = result.text();
    assert!(message.starts_with(&format!("{MARKER} Unknown skill `nope`. Available:\n")));
    assert!(message.contains("- `alpha` — Alpha things."));
}

#[tokio::test]
async fn dispatch_forwards_args_to_the_packed_tool() {
    let tools = bound(vec![FakeTool::plain("a_one")]);
    let result = find(&tools, USE_SKILL)
        .execute(json!({"skill": "alpha", "tool": "a_one", "args": {"marker": "x"}}))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert_eq!(result.text(), r#"a_one:{"marker":"x"}"#);
}

#[tokio::test]
async fn authorization_preparation_forwards_inner_tool_arguments() {
    let tools = bound(vec![FakeTool {
        prepares_authorization: true,
        ..FakeTool::plain("a_one")
    }]);
    let context = find(&tools, USE_SKILL)
        .prepare_authorization(&json!({
            "skill": "alpha",
            "tool": "a_one",
            "args": {"command": "printf safe"}
        }))
        .await
        .unwrap();
    assert_eq!(
        context,
        Some(json!({"inner_args": {"command": "printf safe"}}))
    );
}

struct RebindingTool(&'static str);

#[async_trait]
impl Tool for RebindingTool {
    fn name(&self) -> &str {
        "a_one"
    }
    fn description(&self) -> &str {
        "rebinding test tool"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type": "object"})
    }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(self.0))
    }
    async fn prepare_authorization(&self, _args: &Value) -> anyhow::Result<Option<Value>> {
        Ok(Some(json!({"authorized_tool": self.0})))
    }
}

#[tokio::test]
async fn authorization_and_execution_use_the_same_packed_tool_after_rebind() {
    let handle = PackRegistryHandle::default();
    let mut original: Vec<Box<dyn Tool>> = vec![Box::new(RebindingTool("authorized"))];
    original.push(Box::new(UseSkillTool::new(handle.clone(), CATALOG)));
    let original = Arc::new(original);
    handle.bind(Arc::downgrade(&original));
    let outer: Arc<dyn Tool> = Arc::new(UseSkillTool::new(handle.clone(), CATALOG));
    let args = json!({"skill": "alpha", "tool": "a_one", "args": {}});
    let mut registry = crate::tool::ToolRegistry::<(), ()>::new();
    registry.register(outer);
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();
    let pinned = dispatch.bind_call(&args).unwrap().unwrap();
    assert!(pinned.tool().supports_markdown());
    assert!(
        dispatch
            .tool()
            .host_extension()
            .is_some_and(|extension| extension.is::<PackRegistryHandle>())
    );
    let authorization = pinned.tool().prepare_authorization(&args).await.unwrap();

    let mut replacement: Vec<Box<dyn Tool>> = vec![Box::new(RebindingTool("replacement"))];
    replacement.push(Box::new(UseSkillTool::new(handle.clone(), CATALOG)));
    let replacement = Arc::new(replacement);
    handle.bind(Arc::downgrade(&replacement));

    let result = pinned.tool().execute(args).await.unwrap();
    assert_eq!(
        authorization,
        Some(json!({"authorized_tool": "authorized"}))
    );
    assert_eq!(result.text(), "authorized");
}

#[tokio::test]
async fn catalogs_with_the_same_start_pointer_but_different_lengths_conflict() {
    let handle = PackRegistryHandle::default();
    let mut inner: Vec<Box<dyn Tool>> = vec![Box::new(RebindingTool("executed"))];
    inner.push(Box::new(UseSkillTool::new(handle.clone(), CATALOG)));
    let inner = Arc::new(inner);
    handle.bind(Arc::downgrade(&inner));

    let _full_catalog = UseSkillTool::new(handle.clone(), CATALOG);
    let prefix_catalog = UseSkillTool::new(handle.clone(), PackCatalog::new(&PACKS[..1], MARKER));
    let mut registry = crate::tool::ToolRegistry::<(), ()>::new();
    registry.register(Arc::new(prefix_catalog));
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();
    let arguments = json!({"skill": "alpha", "tool": "a_one", "args": {}});
    let bound = dispatch.bind_call(&arguments).unwrap().unwrap();

    assert!(bound.admission_target().is_none());
    let result = bound.tool().execute(arguments).await.unwrap();
    assert!(result.is_error);
    assert!(result.text().contains("catalog is unavailable"));
}

struct LiveParentDispatch(Arc<dyn Tool>);

#[async_trait]
impl crate::tool::ToolDispatch<(), String> for LiveParentDispatch {
    fn tool(&self) -> Arc<dyn Tool> {
        self.0.clone()
    }

    async fn execute(
        &self,
        _state: &(),
        call_id: crate::CallId,
        arguments: Value,
        _options: tinytools::ToolCallOptions,
        parent: &crate::context::RunContext<String>,
    ) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success(format!(
            "run={};call={call_id};args={arguments}",
            parent.data
        )))
    }
}

#[tokio::test]
async fn typed_use_skill_dispatch_pins_the_selected_target_and_live_parent() {
    let tools = bound(vec![FakeTool::plain("a_one")]);
    let handle = handle_of(&tools).clone();
    let outer: Arc<dyn Tool> = Arc::new(UseSkillTool::new(handle.clone(), CATALOG));
    let selector: Arc<PackDispatchSelector<(), String>> = Arc::new(|selection| {
        assert_eq!(selection.tool_name(), "a_one");
        assert_eq!(selection.arguments(), &json!({"marker": "inside"}));
        Some(Arc::new(LiveParentDispatch(Arc::new(FakeTool::plain(
            "a_one",
        )))))
    });
    let dispatch = UseSkillDispatch::new(outer, selector);
    let outer_args = json!({
        "skill": "alpha",
        "tool": "a_one",
        "args": {"marker": "inside"}
    });
    let bound = dispatch.bind_call(&outer_args).unwrap().unwrap();

    // Rebinding the live registry after admission must not change this call's
    // already authorized target or args.
    let replacement = Arc::new(vec![Box::new(FakeTool::plain("a_two")) as Box<dyn Tool>]);
    handle.bind(Arc::downgrade(&replacement));

    let parent = crate::context::RunContext::new(
        crate::context::RunConfig::new("parent-run"),
        "live-parent".to_string(),
    );
    let call_id = crate::CallId::new("admitted-call");
    let result = bound
        .execute(
            &(),
            call_id,
            outer_args,
            tinytools::ToolCallOptions::default(),
            &parent,
        )
        .await
        .unwrap();

    assert_eq!(
        result.text(),
        "run=live-parent;call=admitted-call;args={\"marker\":\"inside\"}"
    );
    assert_eq!(bound.admission_target().unwrap().0.name(), "a_one");
}

#[tokio::test]
async fn registered_pack_dispatch_runs_through_the_harness_with_admitted_target_and_live_parent() {
    let tools = bound(vec![FakeTool::plain("a_one"), FakeTool::plain("a_two")]);
    let handle = handle_of(&tools).clone();
    let outer: Arc<dyn Tool> = Arc::new(UseSkillTool::new(handle, CATALOG));
    let selected = Arc::new(std::sync::Mutex::new(Vec::new()));
    let selected_for_selector = selected.clone();
    let selector: Arc<PackDispatchSelector<(), String>> = Arc::new(move |selection| {
        let name = selection.tool_name().to_owned();
        selected_for_selector.lock().unwrap().push(name.clone());
        let target_name = match name.as_str() {
            "a_one" => "a_one",
            "a_two" => "a_two",
            _ => return None,
        };
        Some(Arc::new(LiveParentDispatch(Arc::new(FakeTool::plain(
            target_name,
        )))))
    });
    let dispatch = Arc::new(UseSkillDispatch::new(outer, selector));
    let response = ModelResponse {
        message: AssistantMessage {
            id: Some("packed-message".into()),
            content: Vec::new(),
            tool_calls: vec![ToolCall::new(
                "packed-call-17",
                USE_SKILL,
                json!({"skill":"alpha", "tool":"a_one", "args":{"marker":"one"}}),
            )],
            usage: Some(Usage::new(1, 1)),
            origin: None,
        },
        usage: Some(Usage::new(1, 1)),
        finish_reason: Some("tool_calls".into()),
        raw: None,
        resolved_model: None,
        continue_turn: None,
        served_from_cache: false,
    };
    let mut harness: AgentHarness<(), String> = AgentHarness::new();
    harness
        .register_model("mock", Arc::new(MockModel::with_responses(vec![
            response,
            testkit::text_response("finished", 1, 1),
        ])))
        .register_tool_dispatch(dispatch);
    let parent = crate::context::RunContext::new(
        crate::context::RunConfig::new("outer-run"),
        "live-parent-state".to_owned(),
    );
    let run = harness
        .invoke_in_context(&(), parent, vec![Message::user("run the packed tool")])
        .await
        .expect("the harness admits and executes the selected packed tool");

    assert_eq!(selected.lock().unwrap().as_slice(), &["a_one"]);
    let tool_result = run.messages.iter().find(|message| message.role() == "tool").unwrap();
    assert!(tool_result.text().contains("run=live-parent-state"));
    assert!(tool_result.text().contains("call=packed-call-17"));
    assert!(tool_result.text().contains("\"marker\":\"one\""));
}

#[test]
fn packed_binding_preserves_outer_policy_transform_approval() {
    let handle = PackRegistryHandle::default();
    let mut inner: Vec<Box<dyn Tool>> = vec![Box::new(RebindingTool("executed"))];
    inner.push(Box::new(UseSkillTool::new(handle.clone(), CATALOG)));
    let inner = Arc::new(inner);
    handle.bind(Arc::downgrade(&inner));

    let outer = Arc::new(
        crate::tool::toolset::OverrideTool::new(Arc::new(UseSkillTool::new(handle, CATALOG)))
            .with_policy_transform(Arc::new(|policy| policy.requiring_approval())),
    );
    let mut registry = crate::tool::ToolRegistry::<(), ()>::new();
    registry.register(outer);
    let dispatch = registry.model_dispatch(USE_SKILL).unwrap();
    let arguments = json!({"skill": "alpha", "tool": "a_one", "args": {}});

    assert!(dispatch.tool().policy().access.approval_required);
    let bound = dispatch.bind_call(&arguments).unwrap().unwrap();
    assert!(bound.tool().policy().classified);
    assert!(
        bound.tool().policy().access.approval_required,
        "binding the concrete packed target must retain the outer approval policy"
    );
}

#[tokio::test]
async fn a_tool_from_another_skill_is_refused() {
    // Cross-skill dispatch would make `skill` decoration and let a harmless
    // skill reach a dangerous tool.
    let tools = bound(vec![FakeTool {
        level: PermissionLevel::Dangerous,
        ..FakeTool::plain("b_one")
    }]);
    let result = find(&tools, USE_SKILL)
        .execute(json!({"skill": "alpha", "tool": "b_one", "args": {}}))
        .await
        .unwrap();
    assert!(result.is_error, "cross-skill dispatch was admitted");
    assert!(result.text().starts_with(&format!(
        "{MARKER} No tool `b_one` in skill `alpha`. Call `use_skill {{ \"skill\": \"alpha\" }}` to see what it contains."
    )));
}

#[test]
fn permission_level_is_the_inner_tools_not_the_proxys() {
    let tools = bound(vec![FakeTool {
        level: PermissionLevel::Dangerous,
        ..FakeTool::plain("a_one")
    }]);
    let use_skill = find(&tools, USE_SKILL);
    assert_eq!(
        use_skill.permission_level_with_args(&json!({"skill": "alpha", "tool": "a_one"})),
        PermissionLevel::Dangerous
    );
    // Naming no tool (or an empty one) only reads a schema.
    for args in [
        json!({"skill": "alpha"}),
        json!({"skill": "alpha", "tool": ""}),
    ] {
        assert_eq!(
            use_skill.permission_level_with_args(&args),
            PermissionLevel::ReadOnly
        );
    }
    // Unresolvable: the ceiling, never a permissive default.
    assert_eq!(
        use_skill.permission_level_with_args(&json!({"skill": "alpha", "tool": "ghost"})),
        PermissionLevel::Dangerous
    );
}

#[test]
fn external_effect_and_timeout_are_forwarded() {
    let tools = bound(vec![FakeTool {
        external: true,
        timeout: ToolTimeout::Unbounded,
        ..FakeTool::plain("a_one")
    }]);
    let use_skill = find(&tools, USE_SKILL);
    let call = json!({"skill": "alpha", "tool": "a_one"});
    assert!(use_skill.external_effect_with_args(&call));
    assert_eq!(use_skill.timeout_policy(&call), ToolTimeout::Unbounded);
    let ghost = json!({"skill": "alpha", "tool": "ghost"});
    assert!(!use_skill.external_effect_with_args(&ghost));
    assert_eq!(use_skill.timeout_policy(&ghost), ToolTimeout::Inherit);
}

#[test]
fn an_unbound_handle_degrades_closed() {
    let tool = UseSkillTool::new(PackRegistryHandle::default(), CATALOG);
    assert_eq!(tool.permission_level(), PermissionLevel::Dangerous);
}

#[tokio::test]
async fn an_unbound_handle_reports_the_registry_unavailable() {
    let tool = UseSkillTool::new(PackRegistryHandle::default(), CATALOG);
    let result = tool.execute(json!({"skill": "alpha"})).await.unwrap();
    assert!(result.is_error);
    assert!(result.text().contains("registry is not available"));
}

/// The synthesised set is a second registry; a packed tool living only there
/// must be reachable, and rebinding it must repoint the handle.
#[tokio::test]
async fn a_tool_in_the_synthesised_set_is_reachable_and_rebinding_repoints() {
    let durable = bound(vec![]);
    let first: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![Box::new(FakeTool::plain("a_one"))]);
    handle_of(&durable).bind_synthesized(Arc::downgrade(&first));
    let use_skill = find(&durable, USE_SKILL);

    let ran = use_skill
        .execute(json!({"skill": "alpha", "tool": "a_one", "args": {}}))
        .await
        .unwrap();
    assert!(!ran.is_error, "{}", ran.text());

    let second: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![Box::new(FakeTool::plain("a_two"))]);
    handle_of(&durable).bind_synthesized(Arc::downgrade(&second));
    drop(first);
    let ran = use_skill
        .execute(json!({"skill": "alpha", "tool": "a_two", "args": {}}))
        .await
        .unwrap();
    assert!(!ran.is_error, "handle did not follow the rebind");
    let stale = use_skill
        .execute(json!({"skill": "alpha", "tool": "a_one", "args": {}}))
        .await
        .unwrap();
    assert!(stale.is_error, "a dropped tool stayed reachable");
}

#[test]
fn resolve_registry_for_enforces_pack_membership() {
    let durable = bound(vec![FakeTool::plain("a_one")]);
    let handle = handle_of(&durable);
    assert!(
        handle
            .resolve_registry_for(&CATALOG, "alpha", "a_one")
            .is_some()
    );
    assert!(
        handle
            .resolve_registry_for(&CATALOG, "beta", "a_one")
            .is_none()
    );
    assert!(
        handle
            .resolve_registry_for(&CATALOG, "alpha", "ghost")
            .is_none()
    );
}

#[tokio::test]
async fn the_filtered_listing_omits_tools_the_session_cannot_call() {
    let tools = bound(vec![FakeTool::plain("a_one"), FakeTool::plain("a_two")]);
    let handle = handle_of(&tools);
    let listing = render_pack_filtered(&CATALOG, "alpha", handle, &|name| name == "a_one", "")
        .expect("one callable tool");
    assert!(listing.contains("## `a_one`"));
    assert!(!listing.contains("## `a_two`"));

    let denied = render_pack_filtered(&CATALOG, "alpha", handle, &|_| false, "Hand off instead.")
        .unwrap_err();
    assert_eq!(
        denied,
        "Skill `alpha` has no tools available in this session. Hand off instead."
    );
    let bare = render_pack_filtered(&CATALOG, "alpha", handle, &|_| false, "").unwrap_err();
    assert_eq!(
        bare,
        "Skill `alpha` has no tools available in this session."
    );
}

#[test]
fn no_such_pack_tool_renders_each_shape() {
    let base = |callable: Vec<&'static str>, route: &str| NoSuchPackTool {
        not_found_marker: MARKER,
        skill: "alpha",
        tool: "x",
        callable,
        route: route.to_string(),
    };
    assert_eq!(
        base(vec!["a_one", "a_two"], "ignored").render(),
        "[not_found] There is no tool `x` in skill `alpha`. The tools in it you can call: `a_one`, `a_two`."
    );
    assert_eq!(
        base(vec![], "Go elsewhere.").render(),
        "[not_found] There is no tool `x` in skill `alpha`. Go elsewhere."
    );
    assert_eq!(
        base(vec![], "").render(),
        "[not_found] There is no tool `x` in skill `alpha`. Nothing in it is available in this session."
    );
}

#[test]
fn route_sentence_prefers_a_callable_tool_and_falls_back_to_owners() {
    assert_eq!(
        route_sentence(&["a".to_string(), "b".to_string()], &["o"]),
        "Call `a` or `b` instead — that agent owns these tools and runs them directly. Do not retry this skill."
    );
    assert_eq!(
        route_sentence(&[], &["x", "y"]),
        "These tools belong to `x` or `y`; hand the task to one of them rather than calling directly."
    );
    assert!(route_sentence(&[], &[]).is_empty());
}

#[test]
fn scoping_narrows_the_enum_and_the_index_or_drops_the_tool() {
    let tools = bound(vec![]);
    let mut spec = ToolSpec {
        name: USE_SKILL.into(),
        description: find(&tools, USE_SKILL).description().to_string(),
        parameters: find(&tools, USE_SKILL).parameters_schema(),
    };
    assert!(scope_use_skill_spec(&CATALOG, &mut spec, &|name| name == "b_one"));
    assert!(
        spec.description
            .ends_with("Skills:\n- `beta` — Beta things.\n")
    );
    assert_eq!(
        spec.parameters.pointer("/properties/skill/enum"),
        Some(&json!(["beta"]))
    );
    assert!(!scope_use_skill_spec(&CATALOG, &mut spec, &|_| false));
}

#[test]
fn catalog_lookups_honour_owners() {
    assert_eq!(CATALOG.pack_for_tool("b_one").map(|p| p.id), Some("beta"));
    assert!(CATALOG.pack_for_tool("ghost").is_none());
    assert_eq!(CATALOG.all_packed_tool_names(), ["a_one", "a_two", "b_one"]);
    // An owner keeps its belt, including a thread-renamed `<owner>_<thread>` id.
    assert_eq!(
        CATALOG.packed_tool_names_for_agent("alpha_agent"),
        ["b_one"]
    );
    assert_eq!(
        CATALOG.packed_tool_names_for_agent("alpha_agent_t1"),
        ["b_one"]
    );
    assert_eq!(
        CATALOG.packed_tool_names_for_agent("alpha_agentx"),
        ["a_one", "a_two", "b_one"]
    );
    assert_eq!(CATALOG.callable_pack_ids(&|n| n == "a_two"), ["alpha"]);
}

#[test]
fn pack_schema_text_is_independent_of_nested_json_object_insertion_order() {
    struct OrderedTool(Value);
    #[async_trait]
    impl Tool for OrderedTool {
        fn name(&self) -> &str {
            "a_one"
        }
        fn description(&self) -> &str {
            "ordered schema"
        }
        fn parameters_schema(&self) -> Value {
            self.0.clone()
        }
        async fn execute(&self, _: Value) -> anyhow::Result<ToolResult> {
            unreachable!("disclosure must not execute a tool")
        }
    }
    let first = serde_json::from_str(r#"{"type":"object","properties":{"z":{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"integer"}}},"a":{"type":"string"}}}"#).unwrap();
    let second = serde_json::from_str(r#"{"properties":{"a":{"type":"string"},"z":{"properties":{"a":{"type":"integer"},"b":{"type":"string"}},"type":"object"}},"type":"object"}"#).unwrap();
    let render = |parameters| {
        let tools: Arc<Vec<Box<dyn Tool>>> = Arc::new(vec![Box::new(OrderedTool(parameters))]);
        let handle = PackRegistryHandle::default();
        handle.bind(Arc::downgrade(&tools));
        render_pack_filtered(&CATALOG, "alpha", &handle, &|_| true, "").unwrap()
    };
    assert_eq!(render(first), render(second));
}
