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
    /// Resolve this spec into the model name string the provider expects.
    /// `parent_model` is the model the parent agent is using right now.
    ///
    /// Hints are resolved to the `hint:{hint}` role alias (e.g. `"agentic"` →
    /// `"hint:agentic"`), which the inference factory translates to the role's
    /// configured route — the managed default model, or the BYOK/local model
    /// routed to that workload. When a `RouterProvider` is present its route
    /// table takes priority over this default.
    /// A routing pin, or None when the caller should inherit its current model.
    pub fn pin(&self) -> Option<String> {
        match self {
            Self::Inherit => None,
            Self::Exact(name) => Some(name.clone()),
            Self::Hint(hint) => Some(format!("hint:{hint}")),
        }
    }
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
    /// registry at spawn time are silently dropped (logged at debug).
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
    /// Drop privileges, restrict filesystem (Landlock / Bubblewrap).
    Sandboxed,
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
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Tagged {
            Wildcard,
            Named(Vec<String>),
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Tagged(Tagged),
            Legacy(Vec<String>),
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Tagged(Tagged::Wildcard) => Self::Wildcard,
            Wire::Tagged(Tagged::Named(names)) | Wire::Legacy(names) => Self::Named(names),
        })
    }
}
