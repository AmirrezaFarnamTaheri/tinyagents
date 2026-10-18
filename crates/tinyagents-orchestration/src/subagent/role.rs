//! Subagent role and framing: who may delegate further, and what a child is
//! told about its place in the tree.
//!
//! A [`SubagentRole::Leaf`] child never receives the delegation surface this
//! crate registers (the job and message tools, plus whichever
//! [`SubAgentTool`](super::SubAgentTool) names the host lists), and
//! [`restrict_tools`] guarantees a child's declarations are a subset of the
//! set it inherits. [`subagent_framing`] is an optional, neutral system
//! preamble a host may prepend; nothing applies it by default.

use tinyagents_harness::tool::ToolSnapshot;

/// Fleet-control tools whose visibility shapes orchestration prompts and
/// decisions. The names are stable tool identifiers supplied by the host.
pub const FLEET_TOOLS: &[&str] = &[
    "steer_subagent",
    "wait_subagent",
    "wait",
    "wait_loop",
    "close_subagent",
    "continue_subagent",
    "list_subagents",
];

/// The fleet-control subset visible to one parent agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetToolSet {
    available: Vec<&'static str>,
}

impl FleetToolSet {
    /// Treats an unavailable parent definition as having the full fleet
    /// vocabulary, allowing host prompts to retain their prior fallback.
    pub fn all() -> Self {
        Self {
            available: FLEET_TOOLS.to_vec(),
        }
    }

    /// Resolves fleet visibility from a definition's tool scope and denylist.
    /// A trailing `*` denies every fleet name with that prefix.
    pub fn from_scope(scope: &tinyagents_definition::ToolScope, disallowed: &[String]) -> Self {
        let denied = |name: &str| {
            disallowed
                .iter()
                .any(|entry| match entry.strip_suffix('*') {
                    Some(prefix) => name.starts_with(prefix),
                    None => entry == name,
                })
        };
        let available = FLEET_TOOLS
            .iter()
            .copied()
            .filter(|name| match scope {
                tinyagents_definition::ToolScope::Wildcard => true,
                tinyagents_definition::ToolScope::Named(names) => {
                    names.iter().any(|candidate| candidate == name)
                }
            })
            .filter(|name| !denied(name))
            .collect();
        Self { available }
    }

    /// Resolves fleet visibility from the effective, already-filtered tool
    /// names for the current turn.
    pub fn from_visible_tool_names(names: &std::collections::HashSet<String>) -> Self {
        Self {
            available: FLEET_TOOLS
                .iter()
                .copied()
                .filter(|name| names.contains(*name))
                .collect(),
        }
    }

    /// Whether the parent can call `tool`.
    pub fn has(&self, tool: &str) -> bool {
        self.available.contains(&tool)
    }

    /// Whether the parent can block on or poll a worker.
    pub fn can_wait(&self) -> bool {
        self.has("wait_subagent")
    }
}

/// Name of the job query tool [`register_subagent_job_tools`](tinyagents_harness_tools::subagents::register_subagent_job_tools) registers.
pub const SUBAGENT_JOBS_TOOL: &str = "subagent_jobs";
/// Name of the job message tool [`register_subagent_job_tools`](tinyagents_harness_tools::subagents::register_subagent_job_tools) registers.
pub const SUBAGENT_MESSAGE_TOOL: &str = "subagent_message";

/// Whether a child may delegate further.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubagentRole {
    /// May spawn its own children; keeps the tools it inherited. The default,
    /// so existing hosts are unchanged.
    #[default]
    Orchestrator,
    /// Does the work itself: the delegation tools are removed.
    Leaf,
}

impl SubagentRole {
    /// Whether this role may delegate.
    pub const fn can_delegate(self) -> bool {
        matches!(self, Self::Orchestrator)
    }
}

/// Map authored hierarchy vocabulary onto runtime delegation capability.
/// Chat/reasoning can orchestrate; a worker is a leaf. Tier-to-tier admission
/// remains `tinyagents_definition::validate_tier_transition`.
impl From<tinyagents_definition::AgentTier> for SubagentRole {
    fn from(tier: tinyagents_definition::AgentTier) -> Self {
        match tier {
            tinyagents_definition::AgentTier::Chat
            | tinyagents_definition::AgentTier::Reasoning => Self::Orchestrator,
            tinyagents_definition::AgentTier::Worker => Self::Leaf,
        }
    }
}

/// Whether `name` is part of the delegation surface: this crate's fixed tools
/// or one of the host's [`SubAgentTool`](super::SubAgentTool) names.
pub fn is_delegation_tool(name: &str, host_delegation_tools: &[String]) -> bool {
    name == SUBAGENT_JOBS_TOOL
        || name == SUBAGENT_MESSAGE_TOOL
        || host_delegation_tools.iter().any(|tool| tool == name)
}

/// The tool declarations a child may actually be given.
///
/// Starts from `tools` and (1) when `ceiling` is set, keeps only names the
/// ceiling also declares, so a child can never widen what it inherited, then
/// (2) for [`SubagentRole::Leaf`], drops every delegation tool. The one-off
/// marker of `tools` is preserved.
pub fn restrict_tools(
    tools: &ToolSnapshot,
    role: SubagentRole,
    ceiling: Option<&ToolSnapshot>,
    host_delegation_tools: &[String],
) -> ToolSnapshot {
    restrict_tools_with_scope(tools, role, ceiling, host_delegation_tools, None, None)
}

/// Restricts a child's declarations by inherited ceiling, role, authored
/// [`ToolScope`](tinyagents_definition::ToolScope), and optional `{skill}__`
/// prefix. Every restriction is an intersection; no policy can widen the
/// supplied snapshot.
pub fn restrict_tools_with_scope(
    tools: &ToolSnapshot,
    role: SubagentRole,
    ceiling: Option<&ToolSnapshot>,
    host_delegation_tools: &[String],
    scope: Option<&tinyagents_definition::ToolScope>,
    skill_filter: Option<&str>,
) -> ToolSnapshot {
    let skill_prefix = skill_filter.map(|skill| format!("{skill}__"));
    tools.retaining(|spec| {
        let inherited = ceiling.is_none_or(|ceiling| {
            ceiling
                .specs()
                .iter()
                .any(|inherited| inherited.name == spec.name)
        });
        let allowed = role.can_delegate() || !is_delegation_tool(&spec.name, host_delegation_tools);
        let in_scope = scope.is_none_or(|scope| match scope {
            tinyagents_definition::ToolScope::Wildcard => true,
            tinyagents_definition::ToolScope::Named(names) => {
                names.iter().any(|name| name == &spec.name)
            }
        });
        let matches_skill = skill_prefix
            .as_deref()
            .is_none_or(|prefix| spec.name.starts_with(prefix));
        inherited && allowed && in_scope && matches_skill
    })
}

/// A neutral system preamble for a child at `depth` working on `task`.
///
/// Says it is a subagent, who it reports to, that inherited context is
/// background rather than its assignment, and whether it may delegate. Hosts
/// prepend it themselves; it carries no product wording.
pub fn subagent_framing(role: SubagentRole, depth: usize, task: &str) -> String {
    let delegation = match role {
        SubagentRole::Leaf => {
            "You are a leaf worker: you cannot spawn further subagents, so do the work yourself with the tools you have."
        }
        SubagentRole::Orchestrator => {
            "You may delegate parts of the work to your own subagents when that helps; keep their results concise."
        }
    };
    format!(
        "You are a subagent (depth {depth}) working for a parent agent, not the end user.\n\
         Your assignment: {task}\n\
         Report your result concisely to your parent; do not address the end user directly.\n\
         Any inherited conversation or context is background, not your assignment; only the assignment above is.\n\
         Work to completion in the background of the parent's turn and finish with a final report.\n\
         Return a compact result under these headings: Answer, Evidence used, Actions taken, Open uncertainties, Failed tool calls, Recommended next step. Do not state unsupported facts; disclose truncated or partial tool results.\n\
         {delegation}"
    )
}

#[cfg(test)]
#[path = "role_tests.rs"]
mod tests;
