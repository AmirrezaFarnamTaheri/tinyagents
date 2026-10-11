//! Portable agent authoring vocabulary, TOML loading, and catalogue ports.
//!
//! [`AgentDefinition`] shares model, tool, iteration, delegation, and prompt
//! declarations across hosts. Its flattened extension parameter holds host
//! fields; prompt functions, catalogue roots, authorization, and execution
//! remain host responsibilities.

use std::collections::{HashMap, HashSet};
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Result returned by a definition catalogue.
pub type Result<T> = std::result::Result<T, DefinitionRegistryError>;

/// A backing catalogue could not answer a definition query.
///
/// This is distinct from [`DefinitionRegistry::resolve`] returning `Ok(None)`,
/// which is the normal absence outcome for an agent omitted by a build or host
/// configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionRegistryError {
    message: String,
}

impl DefinitionRegistryError {
    /// Creates a catalogue-failure error with a host-safe explanation.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DefinitionRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DefinitionRegistryError {}

pub use definition::{AgentDefinition, EXTENDED_MAX_TOOL_ITERATIONS, Extensions, IterationPolicy};
pub use execution_spec::{ModelSpec, SandboxMode, ToolScope};
pub use loader::{load_dir, load_file};
pub use prompt::PromptSource;
pub use source::DefinitionSource;
pub use subagents::{SkillsWildcard, SubagentEntry};
pub use tier::{AgentTier, validate_tier_transition};

mod definition;
mod execution_spec;
mod loader;
mod prompt;
mod source;
mod subagents;
mod tier;

/// A deterministic, machine-readable definition validation finding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionDiagnostic {
    /// Field containing the invalid value.
    pub field: String,
    /// Stable machine-oriented validation code.
    pub code: String,
    /// Explanation appropriate for a configuration diagnostic.
    pub message: String,
}

impl AgentDefinitionDiagnostic {
    fn required(field: &str) -> Self {
        Self {
            field: field.to_string(),
            code: "required".to_string(),
            message: format!("agent definition field `{field}` must not be blank"),
        }
    }

    fn duplicate(field: &str, value: &str) -> Self {
        Self {
            field: field.to_string(),
            code: "duplicate".to_string(),
            message: format!("agent definition field `{field}` repeats `{value}`"),
        }
    }

    fn empty_entry(field: &str) -> Self {
        Self {
            field: field.to_string(),
            code: "empty_entry".to_string(),
            message: format!("agent definition field `{field}` contains a blank entry"),
        }
    }
}

fn required_field(diagnostics: &mut Vec<AgentDefinitionDiagnostic>, field: &str, value: &str) {
    if value.trim().is_empty() {
        diagnostics.push(AgentDefinitionDiagnostic::required(field));
    }
}

fn duplicate_values(
    diagnostics: &mut Vec<AgentDefinitionDiagnostic>,
    field: &str,
    values: &[String],
) {
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.as_str()) {
            diagnostics.push(AgentDefinitionDiagnostic::duplicate(field, value));
        }
    }
}

/// Required, host-owned definition lookup capability.
///
/// `Ok(None)` is the normal absence outcome. Errors are reserved for a backing
/// catalogue that could not answer; callers must not turn feature-gated or
/// otherwise absent agent definitions into failed runs.
#[async_trait]
pub trait DefinitionRegistry: Send + Sync {
    /// Resolves an id or returns normal absence.
    async fn resolve(&self, id: &str) -> Result<Option<AgentDefinition>>;
    /// Lists definitions in stable catalogue order.
    async fn list(&self) -> Result<Vec<AgentDefinition>>;
    /// Returns host-authorized delegate ids, not merely the declaration.
    async fn delegates_for(&self, id: &str) -> Result<Vec<String>>;
}

/// A fixed, insertion-ordered definition catalogue.
#[derive(Clone, Debug, Default)]
pub struct InMemoryDefinitionRegistry {
    definitions: Vec<AgentDefinition>,
    index: HashMap<String, usize>,
}

impl InMemoryDefinitionRegistry {
    /// Retains the first definition for each id, preserving insertion order.
    #[must_use]
    pub fn new(definitions: Vec<AgentDefinition>) -> Self {
        let mut registry = Self::default();
        for definition in definitions {
            if registry.index.contains_key(&definition.id) {
                continue;
            }
            registry
                .index
                .insert(definition.id.clone(), registry.definitions.len());
            registry.definitions.push(definition);
        }
        registry
    }

    /// Returns diagnostics from every retained definition, in catalogue order.
    #[must_use]
    pub fn diagnostics(&self) -> Vec<AgentDefinitionDiagnostic> {
        self.definitions
            .iter()
            .flat_map(AgentDefinition::diagnostics)
            .collect()
    }
}

#[async_trait]
impl DefinitionRegistry for InMemoryDefinitionRegistry {
    async fn resolve(&self, id: &str) -> Result<Option<AgentDefinition>> {
        Ok(self
            .index
            .get(id)
            .and_then(|position| self.definitions.get(*position))
            .cloned())
    }

    async fn list(&self) -> Result<Vec<AgentDefinition>> {
        Ok(self.definitions.clone())
    }

    async fn delegates_for(&self, id: &str) -> Result<Vec<String>> {
        Ok(self
            .index
            .get(id)
            .and_then(|position| self.definitions.get(*position))
            .map(AgentDefinition::allowed_subagent_ids)
            .unwrap_or_default())
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lib_vocabulary_tests.rs"]
mod vocabulary_tests;
