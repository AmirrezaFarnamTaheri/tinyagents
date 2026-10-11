//! Shared authored definition and host extension slot.
use crate::subagents::deserialize_subagent_entries;
use crate::{
    AgentDefinitionDiagnostic, AgentTier, DefinitionSource, ModelSpec, PromptSource, SandboxMode,
    SubagentEntry, ToolScope,
};
use serde::{Deserialize, Serialize};

/// Unknown host fields retained verbatim by the default definition payload.
pub type Extensions = std::collections::BTreeMap<String, serde_json::Value>;
/// Minimum ceiling applied to extended iteration policy.
pub const EXTENDED_MAX_TOOL_ITERATIONS: usize = 50;

/// How the harness interprets an authored iteration limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IterationPolicy {
    /// Use the authored limit exactly.
    #[default]
    Strict,
    /// Use at least the extended ceiling; other runtime guards still apply.
    Extended,
}

/// Provider-neutral definition. Host-specific prompt builders, compression,
/// prompt omissions and graph selection belong in `extensions`.
///
/// The default extension map retains fields unknown to this library. A host
/// may replace it with a typed serde struct (skipping runtime-only members).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "X: Deserialize<'de> + Default"))]
pub struct AgentDefinition<X = Extensions> {
    /// Stable archetype identity.
    pub id: String,
    /// Capability summary exposed to delegating parents.
    #[serde(alias = "description")]
    pub when_to_use: String,
    /// Optional display name, falling back to the id.
    #[serde(default, alias = "name")]
    pub display_name: Option<String>,
    /// Host-defined routing role; independent of the spawn tier.
    #[serde(default)]
    pub role: Option<String>,
    /// Portable prompt source; built-ins may inject it after parsing.
    #[serde(default)]
    pub system_prompt: PromptSource,
    /// Model pin, hint or inherited routing.
    #[serde(default)]
    pub model: ModelSpec,
    /// Sampling temperature.
    #[serde(default = "temperature")]
    pub temperature: f64,
    /// Explicit tool scope; named-empty never means wildcard.
    #[serde(default)]
    pub tools: ToolScope,
    /// Tools denied even when the scope otherwise permits them.
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    /// Host-resolved skill restriction.
    #[serde(default)]
    pub skill_filter: Option<String>,
    /// Additional tools, still subject to the denylist.
    #[serde(default)]
    pub extra_tools: Vec<String>,
    /// Allowed tools exposed through discovery rather than the initial wire.
    #[serde(default)]
    pub deferred_tools: Vec<String>,
    /// Shared tool-pattern policy applied by the harness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_rules: Option<tinytools::ToolRules>,
    /// Authored iteration cap.
    #[serde(default = "iterations")]
    pub max_iterations: usize,
    /// Whether to raise the authored cap to the extended floor.
    #[serde(default)]
    pub iteration_policy: IterationPolicy,
    /// Optional final-result character limit.
    #[serde(default)]
    pub max_result_chars: Option<usize>,
    /// Optional per-model-call output token limit.
    #[serde(default)]
    pub max_turn_output_tokens: Option<u32>,
    /// Optional wall-clock execution limit in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Authored sandbox request; enforcement remains the host's responsibility.
    #[serde(default)]
    pub sandbox_mode: SandboxMode,
    /// Request background execution when supported by the host.
    #[serde(default)]
    pub background: bool,
    /// Agent delegates and catalogue-widening skill entries.
    #[serde(default, deserialize_with = "deserialize_subagent_entries")]
    pub subagents: Vec<SubagentEntry>,
    /// Optional delegation-tool name override.
    #[serde(default)]
    pub delegate_name: Option<String>,
    /// Static spawn hierarchy tier.
    #[serde(default)]
    pub agent_tier: AgentTier,
    /// Provenance assigned by the loader, not authored in TOML.
    #[serde(skip)]
    pub source: DefinitionSource,
    /// Host-specific authored fields and runtime-only extension members.
    #[serde(flatten, default)]
    pub extensions: X,
}
fn temperature() -> f64 {
    0.4
}
fn iterations() -> usize {
    8
}
impl<X: Default> Default for AgentDefinition<X> {
    fn default() -> Self {
        Self {
            id: String::new(),
            when_to_use: String::new(),
            display_name: None,
            role: None,
            system_prompt: Default::default(),
            model: Default::default(),
            temperature: temperature(),
            tools: Default::default(),
            disallowed_tools: vec![],
            skill_filter: None,
            extra_tools: vec![],
            deferred_tools: vec![],
            tool_rules: None,
            max_iterations: iterations(),
            iteration_policy: Default::default(),
            max_result_chars: None,
            max_turn_output_tokens: None,
            timeout_secs: None,
            sandbox_mode: Default::default(),
            background: false,
            subagents: vec![],
            delegate_name: None,
            agent_tier: Default::default(),
            source: Default::default(),
            extensions: X::default(),
        }
    }
}
impl AgentDefinition {
    /// Construct a runtime catalogue entry with an inherited model and tools.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            display_name: Some(name.into()),
            when_to_use: description.into(),
            ..Default::default()
        }
    }
}
impl<X> AgentDefinition<X> {
    /// Display name or stable identity when no display name was authored.
    pub fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or(&self.id)
    }
    /// Declared agent ids. Skill wildcard entries never grant a delegate.
    pub fn allowed_subagent_ids(&self) -> Vec<String> {
        self.subagents
            .iter()
            .filter_map(|entry| match entry {
                SubagentEntry::AgentId(id) => Some(id.clone()),
                SubagentEntry::Skills(_) => None,
            })
            .collect()
    }
    /// Iteration cap after applying the extended-policy floor.
    pub fn effective_max_iterations(&self) -> usize {
        match self.iteration_policy {
            IterationPolicy::Strict => self.max_iterations,
            IterationPolicy::Extended => self.max_iterations.max(EXTENDED_MAX_TOOL_ITERATIONS),
        }
    }
    /// Materialize generic allow/deny semantics against a known catalogue.
    /// Named scopes include extra and deferred tools before deny filtering
    /// and deduplication; registration still determines tool exposure.
    /// A wildcard with a denylist fails closed when no catalogue is supplied;
    /// a bare wildcard stays explicit, and a named-empty scope stays empty.
    pub fn resolve_tool_scope(&self, registered: Option<&[String]>) -> ToolScope {
        let mut names = match &self.tools {
            ToolScope::Wildcard if self.disallowed_tools.is_empty() => return ToolScope::Wildcard,
            ToolScope::Wildcard => match registered {
                Some(names) => names.to_vec(),
                None => return ToolScope::Named(vec![]),
            },
            ToolScope::Named(names) => names.clone(),
        };
        names.extend(self.extra_tools.iter().cloned());
        names.extend(self.deferred_tools.iter().cloned());
        names.retain(|name| !self.disallowed_tools.contains(name));
        let mut seen = std::collections::HashSet::new();
        names.retain(|name| seen.insert(name.clone()));
        ToolScope::Named(names)
    }

    /// Replace host metadata without projecting or reconstructing core fields.
    pub fn with_extensions<Y>(self, extensions: Y) -> AgentDefinition<Y> {
        AgentDefinition {
            id: self.id,
            when_to_use: self.when_to_use,
            display_name: self.display_name,
            role: self.role,
            system_prompt: self.system_prompt,
            model: self.model,
            temperature: self.temperature,
            tools: self.tools,
            disallowed_tools: self.disallowed_tools,
            skill_filter: self.skill_filter,
            extra_tools: self.extra_tools,
            deferred_tools: self.deferred_tools,
            tool_rules: self.tool_rules,
            max_iterations: self.max_iterations,
            iteration_policy: self.iteration_policy,
            max_result_chars: self.max_result_chars,
            max_turn_output_tokens: self.max_turn_output_tokens,
            timeout_secs: self.timeout_secs,
            sandbox_mode: self.sandbox_mode,
            background: self.background,
            subagents: self.subagents,
            delegate_name: self.delegate_name,
            agent_tier: self.agent_tier,
            source: self.source,
            extensions,
        }
    }
    /// Set an exact model pin.
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = ModelSpec::Exact(model.into());
        self
    }
    /// Set declared agent ids without adding skill wildcard entries.
    pub fn with_subagents<I, S>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.subagents = ids
            .into_iter()
            .map(|id| SubagentEntry::AgentId(id.into()))
            .collect();
        self
    }
    /// Set an explicit tool allowlist, including an explicit empty list.
    pub fn with_tools<I, S>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tools = ToolScope::Named(tools.into_iter().map(Into::into).collect());
        self
    }
    /// Set the shared pattern rules.
    pub fn with_tool_rules(mut self, rules: tinytools::ToolRules) -> Self {
        self.tool_rules = Some(rules);
        self
    }
    /// Set the host-defined routing role.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
    /// Deterministic diagnostics for malformed required fields and lists.
    pub fn diagnostics(&self) -> Vec<AgentDefinitionDiagnostic> {
        let mut diagnostics = vec![];
        crate::required_field(&mut diagnostics, "id", &self.id);
        crate::required_field(&mut diagnostics, "name", self.display_name());
        crate::required_field(&mut diagnostics, "description", &self.when_to_use);
        let subagents = self.allowed_subagent_ids();
        crate::duplicate_values(&mut diagnostics, "subagents", &subagents);
        let tools = match &self.tools {
            ToolScope::Named(tools) => tools.as_slice(),
            ToolScope::Wildcard => &[],
        };
        crate::duplicate_values(&mut diagnostics, "tools", tools);
        for (field, values) in [("subagents", subagents.as_slice()), ("tools", tools)] {
            for value in values {
                if value.trim().is_empty() {
                    diagnostics.push(AgentDefinitionDiagnostic::empty_entry(field));
                }
            }
        }
        diagnostics
    }
    /// Whether required fields and declared list entries are well formed.
    pub fn is_valid(&self) -> bool {
        self.diagnostics().is_empty()
    }
}
