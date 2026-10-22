//! Deterministic inputs and fallback text for a run stopped at its call cap.

/// One tool result paired with the call outcome captured by the executor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapToolResult<'a> {
    /// Tool call id from the assistant transcript.
    pub call_id: &'a str,
    /// Tool name from the assistant transcript.
    pub name: &'a str,
    /// Tool output, already normalized by the caller.
    pub output: &'a str,
    /// Whether execution completed successfully. Missing outcomes must be
    /// represented as `false`, never guessed successful.
    pub success: bool,
}

/// Builds a stable tool/result digest in input order for a cap-hit summary.
pub fn build_cap_digest(results: &[CapToolResult<'_>], per_result_chars: usize) -> String {
    use std::fmt::Write as _;
    let mut digest = String::new();
    for result in results {
        let status = if result.success { "ok" } else { "failed" };
        let output = truncate_chars(result.output, per_result_chars);
        let _ = writeln!(digest, "- {} [{status}]: {output}", result.name);
    }
    digest.trim_end().to_owned()
}

/// Stable fallback when a cap-hit model summary fails or returns no prose.
pub fn cap_summary_fallback(max_calls: usize, digest: &str) -> String {
    format!(
        "I reached my tool-call limit ({max_calls} steps) before finishing this task. Progress so far (tool calls + results):\n{digest}\n\nThe task is incomplete — the above is what I accomplished; continue from here."
    )
}

fn truncate_chars(text: &str, limit: usize) -> String {
    let mut chars = text.chars();
    let prefix: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
#[path = "cap_summary_tests.rs"]
mod tests;
