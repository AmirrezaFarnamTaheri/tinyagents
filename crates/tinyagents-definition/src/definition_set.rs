//! Mutable, insertion-ordered definition set with tier-hierarchy validation.
//!
//! Unlike [`crate::InMemoryDefinitionRegistry`] (first definition wins, fixed),
//! a [`DefinitionSet`] models the layered catalogue a host builds at boot:
//! built-ins first, then user or workspace overrides that replace a built-in of
//! the same id in place.

use crate::{
    AgentDefinition, AgentTier, DefinitionValidation, Extensions, SubagentEntry,
    load_dir_with_validation, validate_tier_transition,
};
use anyhow::{Result, bail};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::path::Path;

/// Insertion-ordered definitions keyed by id; a later insert replaces in place.
#[derive(Debug, Clone)]
pub struct DefinitionSet<X = Extensions> {
    by_id: HashMap<String, AgentDefinition<X>>,
    order: Vec<String>,
}

impl<X> Default for DefinitionSet<X> {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            order: Vec::new(),
        }
    }
}

impl<X> DefinitionSet<X> {
    /// Creates an empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a definition. An existing id is replaced and keeps its position.
    pub fn insert(&mut self, definition: AgentDefinition<X>) {
        let id = definition.id.clone();
        if self.by_id.insert(id.clone(), definition).is_none() {
            self.order.push(id);
        }
    }

    /// Looks up a definition by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&AgentDefinition<X>> {
        self.by_id.get(id)
    }

    /// Lists definitions in first-insertion order.
    #[must_use]
    pub fn list(&self) -> Vec<&AgentDefinition<X>> {
        self.order
            .iter()
            .filter_map(|id| self.by_id.get(id))
            .collect()
    }

    /// Number of definitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// True when the set holds no definitions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Checks every `subagents` edge against the tier rules.
    ///
    /// Unknown child ids and skills wildcards are skipped: they are separate
    /// integrity concerns, not tier errors.
    pub fn validate_tier_hierarchy(&self) -> Result<()> {
        let tiers: HashMap<&str, AgentTier> = self
            .by_id
            .values()
            .map(|d| (d.id.as_str(), d.agent_tier))
            .collect();
        for definition in self.list() {
            for entry in &definition.subagents {
                let SubagentEntry::AgentId(child) = entry else {
                    continue;
                };
                if definition.agent_tier == AgentTier::Worker {
                    bail!(
                        "agent `{}` is a `worker` tier and must not list `{child}` in its \
                         subagents — workers are leaf executors.",
                        definition.id
                    );
                }
                let Some(child_tier) = tiers.get(child.as_str()).copied() else {
                    continue;
                };
                if let Err(reason) = validate_tier_transition(definition.agent_tier, child_tier) {
                    bail!(
                        "agent `{}` ({}) lists `{child}` ({child_tier}) in subagents — {reason}",
                        definition.id,
                        definition.agent_tier,
                    );
                }
            }
        }
        Ok(())
    }
}

impl<X: Clone> DefinitionSet<X> {
    /// Returns a copy with `definitions` layered on top; the receiver is untouched.
    #[must_use]
    pub fn with_definitions(
        &self,
        definitions: impl IntoIterator<Item = AgentDefinition<X>>,
    ) -> Self {
        let mut next = self.clone();
        for definition in definitions {
            next.insert(definition);
        }
        next
    }
}

impl<X: DeserializeOwned + Default> DefinitionSet<X> {
    /// Layers every `*.toml` in each existing directory, in order, over the set.
    ///
    /// Missing directories are skipped, a directory already visited is not read
    /// twice, and the merged hierarchy is validated afterwards.
    pub fn load_dirs(&mut self, dirs: &[&Path], validation: DefinitionValidation) -> Result<()> {
        let mut seen: Vec<&Path> = Vec::new();
        for dir in dirs {
            if !dir.is_dir() || seen.contains(dir) {
                continue;
            }
            seen.push(dir);
            let mut loaded = Vec::new();
            load_dir_with_validation(dir, &mut loaded, validation)?;
            for definition in loaded {
                tracing::info!(id = %definition.id, "[agent_defs] layered custom definition");
                self.insert(definition);
            }
        }
        self.validate_tier_hierarchy()
    }
}

#[cfg(test)]
#[path = "definition_set_tests.rs"]
mod tests;
