//! Runtime execution knobs of a definition: model selection
//! ([`ModelSpec`]), tool visibility ([`ToolScope`]) and sandboxing
//! ([`SandboxMode`]).

use serde::{Deserialize, Serialize};

/// Model selection for a sub-agent.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModelSpec {
    /// Use the parent agent's currently-selected model at spawn time.
    #[default]
    Inherit,
    /// Exact model name (e.g. `"neocortex-mk1"`).
    Exact(String),
    /// Router hint (e.g. `"reasoning"`, `"coding"`, `"local"`). Resolved
    /// to a real model by the routing provider.
    Hint(String),
}

impl ModelSpec {
    /// Exact model pin. Inherited selections and routing hints have no pin.
    pub fn pin(&self) -> Option<String> {
        match self {
            Self::Inherit | Self::Hint(_) => None,
            Self::Exact(name) => Some(name.clone()),
        }
    }
    /// Host-interpreted routing hint, distinct from a concrete model id.
    pub fn hint(&self) -> Option<String> {
        match self {
            Self::Hint(hint) => Some(hint.clone()),
            Self::Inherit | Self::Exact(_) => None,
        }
    }
    /// Resolve inherited selection against a parent model; hints become `hint:` aliases.
    pub fn resolve(&self, parent_model: &str) -> String {
        match self {
            Self::Inherit => parent_model.to_string(),
            Self::Exact(name) => name.clone(),
            Self::Hint(hint) => format!("hint:{hint}"),
        }
    }
}

/// Which tools a sub-agent is allowed to call.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolScope {
    /// All tools the parent has (subject to `disallowed_tools` and
    /// `skill_filter`).
    #[default]
    Wildcard,
    /// An explicit allowlist of tool names. Names not present in the parent
    /// registry must be handled according to the host tool catalogue policy.
    Named(Vec<String>),
}

/// Sandbox mode for a sub-agent's tool execution. Serialises as a simple
/// `snake_case` string in TOML (`none` / `read_only` / `sandboxed`). In
/// the future this may map directly into a `SecurityPolicy` builder.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// No additional sandboxing beyond what the parent already enforces.
    #[default]
    None,
    /// Read-only — write/execute tools are filtered out.
    ReadOnly,
    /// Request host-provided isolation.
    Sandboxed,
}

/// The tools through which an agent spawns sub-agents of its own choosing.
/// A named scope that lists none of them delegates only to what its host
/// declared, never to an inherited roster.
pub const SPAWN_TOOL_NAMES: &[&str] = &[
    "spawn_subagent",
    "spawn_async_subagent",
    "spawn_parallel_agents",
    "spawn_worker_thread",
];

impl ToolScope {
    /// Whether this is a named scope that lists one of [`SPAWN_TOOL_NAMES`].
    pub fn names_a_spawn_tool(&self) -> bool {
        matches!(self, ToolScope::Named(names)
            if names.iter().any(|name| SPAWN_TOOL_NAMES.contains(&name.as_str())))
    }
}

impl<'de> Deserialize<'de> for ModelSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Tagged {
            Inherit,
            Exact(String),
            Hint(String),
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Tagged(Tagged),
            Legacy(String),
        }
        Ok(match Option::<Wire>::deserialize(deserializer)? {
            None | Some(Wire::Tagged(Tagged::Inherit)) => Self::Inherit,
            Some(Wire::Tagged(Tagged::Exact(name))) | Some(Wire::Legacy(name)) => Self::Exact(name),
            Some(Wire::Tagged(Tagged::Hint(hint))) => Self::Hint(hint),
        })
    }
}
impl<'de> Deserialize<'de> for ToolScope {
    /// Accepts the canonical `"wildcard"` / `{ named = [..] }` forms, a bare
    /// list (legacy), and the legacy TOML table form `wildcard = {}` that
    /// OpenHuman definitions on disk already use.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, IgnoredAny, MapAccess, SeqAccess, Visitor};
        struct ScopeVisitor;
        impl<'de> Visitor<'de> for ScopeVisitor {
            type Value = ToolScope;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("`wildcard`, `{ named = [..] }`, or a list of tool names")
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<ToolScope, E> {
                match v {
                    "wildcard" => Ok(ToolScope::Wildcard),
                    other => Err(E::unknown_variant(other, &["wildcard", "named"])),
                }
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ToolScope, A::Error> {
                let mut names = Vec::new();
                while let Some(name) = seq.next_element::<String>()? {
                    names.push(name);
                }
                Ok(ToolScope::Named(names))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<ToolScope, A::Error> {
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| A::Error::custom("empty tool scope table"))?;
                let scope = match key.as_str() {
                    "wildcard" => {
                        map.next_value::<IgnoredAny>()?;
                        ToolScope::Wildcard
                    }
                    "named" => ToolScope::Named(map.next_value()?),
                    other => return Err(A::Error::unknown_variant(other, &["wildcard", "named"])),
                };
                if map.next_key::<IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom("tool scope table has more than one key"));
                }
                Ok(scope)
            }
        }
        deserializer.deserialize_any(ScopeVisitor)
    }
}
