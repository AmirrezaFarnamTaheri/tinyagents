//! Portable prompt sources. Host prompt builders belong in the extension slot.
use serde::{Deserialize, Serialize};

/// The portable system-prompt source authored in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSource {
    /// Prompt body to compose with the host's standard sections.
    Inline(String),
    /// Host-resolved prompt file. This crate does not interpret relative roots.
    File { path: String },
    /// Entire prompt without composed identity, safety, tools or memory sections.
    Verbatim(String),
    /// Inert marker for a host-built prompt; the executable builder belongs
    /// in the typed definition extension. This cannot be authored in TOML.
    #[serde(skip_deserializing)]
    Dynamic(()),
}
impl Default for PromptSource {
    fn default() -> Self {
        Self::Inline(String::new())
    }
}
