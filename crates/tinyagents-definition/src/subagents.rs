//! `subagents` allowlist entries: agent ids and the `{ skills = "*" }`
//! wildcard, plus the lenient deserializer that accepts both TOML shapes.

use serde::{Deserialize, Deserializer, Serialize};

/// One entry in [`super::AgentDefinition::subagents`]. Parses from TOML as either
/// a bare string (agent id) or an inline table (`{ skills = "*" }`) thanks
/// to `#[serde(untagged)]`.
///
/// # TOML shapes
///
/// ```toml
/// [subagents]
/// allowlist = [
///     "task_manager_agent",    # AgentId("task_manager_agent")
///     "vision_agent",          # AgentId("vision_agent")
///     { skills = "*" },        # Skills { pattern: "*" }
/// ]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SubagentEntry {
    /// Delegate to a specific built-in or custom agent by id.
    AgentId(String),
    /// Host-expanded skill catalogue selector. It grants no delegate ids;
    /// the host decides which skills are available and how to execute them.
    Skills(SkillsWildcard),
}

/// The `{ skills = "*" }` inline table in a `subagents` list.
///
/// The portable vocabulary recognizes `"*"` through [`Self::matches_all`].
/// Hosts interpret any other selector according to their own catalogue policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillsWildcard {
    /// Glob / wildcard pattern. Only `"*"` is currently supported.
    pub skills: String,
}

pub(super) fn deserialize_subagent_entries<'de, D>(
    deserializer: D,
) -> Result<Vec<SubagentEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Wire {
        Section { allowlist: Vec<SubagentEntry> },
        LegacyList(Vec<SubagentEntry>),
    }

    match Option::<Wire>::deserialize(deserializer)? {
        Some(Wire::Section { allowlist }) => Ok(allowlist),
        Some(Wire::LegacyList(entries)) => Ok(entries),
        None => Ok(Vec::new()),
    }
}

impl SkillsWildcard {
    /// True when this wildcard should expand to the whole host-provided skill catalogue.
    pub fn matches_all(&self) -> bool {
        self.skills == "*"
    }
}
