//! The tool-output ladder's mechanism: which limit bounds a result, which
//! stages a tool is exempt from, and the order of the closing disclosure.
//!
//! The host's `after_tool` middleware runs the stages (content-aware
//! compaction, the byte cap with artifact spill); this module decides *what
//! applies* from an [`OutputCapPolicy`] and renders the disclosure last, so no
//! cap can eat it.

/// Per-tool facts the ladder reads. Implemented by the host.
pub trait OutputCapPolicy: Send + Sync {
    /// The shared byte budget for tools that declare no cap of their own.
    fn default_budget_bytes(&self) -> usize;
    /// A tool's own declared cap, if any. It wins over the shared budget.
    fn tool_cap(&self, tool: &str) -> Option<usize>;
    /// Exempt from summary and compaction (content must stay structural).
    fn is_compaction_exempt(&self, tool: &str) -> bool;
    /// Exempt from the cap and the backstop (a cut breaks the consumer).
    fn is_truncation_exempt(&self, tool: &str) -> bool;
    /// Largest body worth keeping whole for artifact persistence.
    fn max_persistable_bytes(&self) -> u64 {
        u64::MAX
    }
}

/// An [`OutputCapPolicy`] built from plain lists: the shared budget, per-tool
/// caps, and the tool names exempt from each stage. Enough for a host whose
/// exemptions are fixed name sets; a host with computed rules implements the
/// trait itself.
#[derive(Clone, Debug, Default)]
pub struct ListedOutputCapPolicy {
    /// The shared byte budget.
    pub default_budget_bytes: usize,
    /// Tool name to its own declared cap.
    pub tool_caps: std::collections::HashMap<String, usize>,
    /// Tools exempt from summary and compaction only.
    pub compaction_exempt: Vec<String>,
    /// Tools exempt from the cap and the backstop (also compaction-exempt
    /// only if listed in `compaction_exempt`).
    pub truncation_exempt: Vec<String>,
    /// Largest body worth keeping whole; `0` means no limit.
    pub max_persistable_bytes: u64,
}

impl OutputCapPolicy for ListedOutputCapPolicy {
    fn default_budget_bytes(&self) -> usize {
        self.default_budget_bytes
    }
    fn tool_cap(&self, tool: &str) -> Option<usize> {
        self.tool_caps.get(tool).copied()
    }
    fn is_compaction_exempt(&self, tool: &str) -> bool {
        self.compaction_exempt.iter().any(|t| t == tool)
    }
    fn is_truncation_exempt(&self, tool: &str) -> bool {
        self.truncation_exempt.iter().any(|t| t == tool)
    }
    fn max_persistable_bytes(&self) -> u64 {
        match self.max_persistable_bytes {
            0 => u64::MAX,
            n => n,
        }
    }
}

/// What the ladder applies to one result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputStagePlan {
    /// The one limit that bounds this result; `0` means unbounded.
    pub budget_bytes: usize,
    /// The limit came from the tool's own declaration.
    pub tool_declared_cap: bool,
    /// Skip summary and compaction.
    pub compaction_exempt: bool,
    /// Skip the cap and the backstop.
    pub truncation_exempt: bool,
    /// Keep the tool's untouched output so a spill stores what it returned.
    pub keep_full_output: bool,
}

/// Decide the stages for `tool`'s `content_len`-byte result.
/// `has_artifact_store` says whether an oversized body can spill.
pub fn plan_output_stages(
    policy: &dyn OutputCapPolicy,
    tool: &str,
    content_len: usize,
    has_artifact_store: bool,
) -> OutputStagePlan {
    let tool_cap = policy.tool_cap(tool);
    let budget_bytes = tool_cap.unwrap_or_else(|| policy.default_budget_bytes());
    let truncation_exempt = policy.is_truncation_exempt(tool);
    let keep_full_output = !truncation_exempt
        && budget_bytes > 0
        && has_artifact_store
        && content_len > budget_bytes
        && content_len as u64 <= policy.max_persistable_bytes();
    OutputStagePlan {
        budget_bytes,
        tool_declared_cap: tool_cap.is_some(),
        compaction_exempt: policy.is_compaction_exempt(tool),
        truncation_exempt,
        keep_full_output,
    }
}

/// Whether the summary/compaction stage should run: a tool that declares its
/// own cap bounds itself (the spill gives losslessly what a summary would),
/// unless the caller asked a `focus` question only a summary can answer.
pub fn should_compact(plan: &OutputStagePlan, has_focus: bool, wants_compaction: bool) -> bool {
    wants_compaction && !plan.compaction_exempt && (!plan.tool_declared_cap || has_focus)
}

/// Prefix the closing disclosure after every cap has run: the unavailable
/// notice first, then the authoritative size of what a summary replaced.
pub fn disclose(
    content: String,
    notice: Option<&str>,
    summarized_from_bytes: Option<usize>,
    summary_label: &str,
) -> String {
    let mut content = content;
    if let Some(notice) = notice {
        content = format!("{notice}\n\n{content}");
    }
    if let Some(bytes) = summarized_from_bytes {
        content = format!(
            "[{summary_label}: summary of {bytes} bytes of tool output, complete]\n\n{content}"
        );
    }
    content
}

#[cfg(test)]
#[path = "output_cap_tests.rs"]
mod tests;
