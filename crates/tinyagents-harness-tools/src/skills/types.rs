//! Tool-pack types: the unit of on-demand tool disclosure.

use std::sync::Arc;

use serde_json::Value;
use tinytools::{Tool, ToolResult};

use super::{catalog::PackCatalog, handle::ToolVec, render::render_pack};

/// Compatibility name for the erased `use_skill` host extension.
///
/// This remains a type alias so callers that downcast `Tool::host_extension`
/// to [`super::PackRegistryHandle`] keep observing the same concrete type.
pub type PackToolExtension = super::PackRegistryHandle;

/// A call-scoped target captured before authorization and reused for execution.
pub(crate) struct PinnedPackCall {
    pub(super) outer: Arc<dyn Tool>,
    pub(super) target: PackTarget,
}

/// The immutable packed target selected for one admitted `use_skill` call.
///
/// Hosts may use the registry and tool name to build the same typed dispatch
/// they would register for a directly advertised tool. The selected registry
/// is retained so a concurrent rebind cannot change this call's target.
#[derive(Clone)]
pub struct PackSelection {
    pub(super) tools: ToolVec,
    pub(super) index: usize,
    pub(super) arguments: Value,
}

impl PackSelection {
    /// Name of the packed tool selected by the call.
    pub fn tool_name(&self) -> &str {
        self.tools[self.index].name()
    }

    /// Effective arguments passed to the selected packed tool.
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }

    /// The exact registry allocation from which the tool was selected.
    pub fn registry(&self) -> Arc<Vec<Box<dyn Tool>>> {
        Arc::clone(&self.tools)
    }
}

pub(super) enum PackTarget {
    Execute {
        tools: ToolVec,
        index: usize,
        arguments: Value,
    },
    Answer(ToolResult),
}

pub(super) enum ResolvedPackCall {
    Execute {
        tools: ToolVec,
        index: usize,
        arguments: Value,
    },
    Answer(ToolResult),
}

pub(super) fn resolve_call(
    handle: &super::PackRegistryHandle,
    catalog: &PackCatalog,
    args: &Value,
) -> ResolvedPackCall {
    let Some(skill) = args.get("skill").and_then(Value::as_str) else {
        return ResolvedPackCall::Answer(ToolResult::error(format!(
            "`skill` is required.\n\nSkills:\n{}",
            catalog.pack_index_markdown()
        )));
    };

    let Some(name) = super::named_tool(args) else {
        return ResolvedPackCall::Answer(match render_pack(catalog, skill, handle) {
            Ok(text) => ToolResult::success(text),
            Err(message) => ToolResult::error(message),
        });
    };

    let Some((tools, index)) = handle.resolve(catalog, skill, name) else {
        return ResolvedPackCall::Answer(ToolResult::error(format!(
            "{} No tool `{name}` in skill `{skill}`. Call `use_skill {{ \"skill\": \"{skill}\" }}` \
             to see what it contains.\n\nSkills:\n{}",
            catalog.not_found_marker(),
            catalog.pack_index_markdown()
        )));
    };
    ResolvedPackCall::Execute {
        tools,
        index,
        arguments: args
            .get("args")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
    }
}

/// A named bundle of tools that is **not** advertised to the model by default.
///
/// The pack's tools stay fully constructed and executable; what changes is that
/// their JSON schemas never reach the provider until the agent asks for them.
/// That trade is the whole point: an orchestrator carrying ~77 tool schemas
/// spends far more of its fixed per-turn budget on schemas than on its own
/// instructions, and most of those tools go untouched in most conversations.
#[derive(Debug)]
pub struct ToolPack {
    /// Stable id the agent names in `use_skill`.
    pub id: &'static str,
    /// One line, rendered in the always-on pack index. This is the only text
    /// about the pack the model sees before loading it, so it has to carry
    /// enough intent for the model to know when to reach for it.
    pub summary: &'static str,
    /// Tool names this pack owns. A name listed here is removed from the
    /// agent's advertised surface and reachable only through `use_skill`.
    pub tools: &'static [&'static str],
    /// Agent ids for which this pack is **not** applied.
    ///
    /// Withholding is a bet that the tools are idle in most turns. That bet is
    /// wrong for the specialist a family was delegated to: `workflow_builder`
    /// exists precisely to run the flow authoring tools, so packing them would
    /// put a `use_skill` round trip in front of the first call of every one of
    /// its turns and buy nothing — its whole belt is the pack.
    ///
    /// The earlier packs did not need this because they held only synthesised
    /// `delegate_*` tools, which exist on the orchestrator alone. Packs over
    /// raw tools do, and an owner list is the narrowest way to say so.
    pub owners: &'static [&'static str],
    /// The skill's playbook, printed when `use_skill` loads the pack, between
    /// the summary and the tool schemas. Empty for a pack that is only a
    /// schema bundle.
    ///
    /// This is what replaced most single-belt specialists: a sub-agent whose
    /// whole value was a prompt over a handful of tools is a ~500-token guide
    /// here plus `Deferred` tools the orchestrator reaches itself, instead of
    /// a separate context, model call and hand-off envelope. Lookup detail
    /// belongs in the guide; a rule that must bind before the model thinks to
    /// load the skill (confirm before moving money) stays in the orchestrator
    /// prompt. Kept under ~550 tokens by `toolpacks_tests.rs`.
    pub guide: &'static str,
}

impl ToolPack {
    pub fn owns(&self, tool: &str) -> bool {
        self.tools.contains(&tool)
    }

    /// Whether `agent_id` owns this pack, including a web-chat thread name
    /// formed as `<owner>_<thread>` after the session is built.
    pub fn is_owner(&self, agent_id: &str) -> bool {
        self.owners.iter().any(|owner| {
            agent_id == *owner
                || agent_id
                    .strip_prefix(*owner)
                    .is_some_and(|suffix| suffix.starts_with('_'))
        })
    }
}
