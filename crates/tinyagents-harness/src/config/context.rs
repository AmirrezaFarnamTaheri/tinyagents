use serde::{Deserialize, Serialize};

/// Default number of recent tool results preserved by microcompaction.
pub const DEFAULT_KEEP_RECENT_TOOL_RESULTS: usize = 5;

/// Generic token and result controls used by context middleware.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub microcompact_enabled: bool,
    #[serde(default = "default_true")]
    pub autocompact_enabled: bool,
    #[serde(default = "default_microcompact_keep_recent")]
    pub microcompact_keep_recent: usize,
    #[serde(default = "default_tool_result_budget_bytes")]
    pub tool_result_budget_bytes: usize,
    #[serde(default = "default_summarizer_payload_threshold_tokens", alias = "summarizer_payload_threshold_bytes")]
    pub summarizer_payload_threshold_tokens: usize,
    #[serde(default = "default_summarizer_max_payload_tokens", alias = "summarizer_max_payload_bytes")]
    pub summarizer_max_payload_tokens: usize,
    #[serde(default)]
    pub summarizer_model: Option<String>,
    #[serde(default = "default_true")]
    pub prefer_markdown_tool_output: bool,
    #[serde(default = "default_true")]
    pub compaction_enabled: bool,
    #[serde(default)]
    pub compaction_trigger_tokens: Option<u64>,
    #[serde(default)]
    pub compaction_strategy: CompactionStrategy,
}

/// Normalized compaction options carried by a turn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionSettings {
    pub trigger_tokens: Option<u64>,
    pub strategy: CompactionStrategy,
}

/// How context compaction creates a checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStrategy {
    #[default]
    TaskState,
    Summary,
}

impl ContextConfig {
    pub fn compaction_settings(&self) -> CompactionSettings {
        CompactionSettings {
            trigger_tokens: self.compaction_trigger_tokens.filter(|value| *value > 0),
            strategy: self.compaction_strategy,
        }
    }
}

impl CompactionStrategy {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "task_state" | "typed" | "s4" => Some(Self::TaskState),
            "summary" | "free_form" | "s0" => Some(Self::Summary),
            _ => None,
        }
    }
}

fn default_enabled() -> bool { true }
fn default_true() -> bool { true }
fn default_microcompact_keep_recent() -> usize { DEFAULT_KEEP_RECENT_TOOL_RESULTS }
fn default_tool_result_budget_bytes() -> usize {
    crate::config::TurnConfig::default().tool_result_budget_bytes
}
fn default_summarizer_payload_threshold_tokens() -> usize { 4_000 }
fn default_summarizer_max_payload_tokens() -> usize { 64_000 }

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            microcompact_enabled: default_true(),
            autocompact_enabled: default_true(),
            microcompact_keep_recent: default_microcompact_keep_recent(),
            tool_result_budget_bytes: default_tool_result_budget_bytes(),
            summarizer_payload_threshold_tokens: default_summarizer_payload_threshold_tokens(),
            summarizer_max_payload_tokens: default_summarizer_max_payload_tokens(),
            summarizer_model: None,
            prefer_markdown_tool_output: default_true(),
            compaction_enabled: default_true(),
            compaction_trigger_tokens: None,
            compaction_strategy: CompactionStrategy::default(),
        }
    }
}

#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
