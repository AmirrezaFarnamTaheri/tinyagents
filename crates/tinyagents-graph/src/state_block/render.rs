//! Rendering the run-state block from the plan and the ledger.

use crate::todos::{TodoItem, TodoStatus};

use super::ledger::one_line;
use super::types::RunLedger;

/// First line of every block; also how tests and logs recognise one.
pub const STATE_BLOCK_HEADER: &str = "[run state]";
/// Longest plan step shown, in characters.
const MAX_STEP_CHARS: usize = 160;
/// Plan steps shown before the rest are summarised as a count.
const MAX_STEPS_SHOWN: usize = 20;

/// The block for this request, or `None` when there is neither a plan nor a
/// fact to show.
pub(super) fn render(items: &[TodoItem], ledger: &RunLedger) -> Option<String> {
    let mut sections = Vec::new();
    if let Some(plan) = plan_section(items, ledger) {
        sections.push(plan);
    }
    if !ledger.failures.is_empty() {
        let mut lines = vec!["Recent failures (oldest first):".to_string()];
        for failure in &ledger.failures {
            lines.push(format!(
                "  - {}, {} `{}`: {}",
                age(ledger.round, failure.round),
                failure.tool,
                failure.summary,
                failure.error
            ));
        }
        sections.push(lines.join("\n"));
    }
    if !ledger.missing.is_empty() {
        let names: Vec<&str> = ledger
            .missing
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        sections.push(format!("Reported not found: {}", names.join(", ")));
    }
    if !ledger.files.is_empty() {
        sections.push(format!(
            "Files written this run: {}",
            ledger.files.join(", ")
        ));
    }
    if sections.is_empty() {
        return None;
    }
    Some(format!(
        "{STATE_BLOCK_HEADER} Rebuilt by the harness for this call from your plan and your tool \
         results; it is not part of the conversation.\n{}",
        sections.join("\n")
    ))
}

fn plan_section(items: &[TodoItem], ledger: &RunLedger) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    let total = items.len();
    let header = match items
        .iter()
        .position(|item| item.status == TodoStatus::InProgress)
    {
        Some(index) => {
            let rounds = ledger.round.saturating_sub(ledger.step_since);
            format!(
                "Plan: step {} of {total} in progress ({rounds} tool round{} on it).",
                index + 1,
                if rounds == 1 { "" } else { "s" }
            )
        }
        None => match items.iter().position(|item| item.status.is_open()) {
            Some(index) => format!(
                "Plan: no step in progress; step {} of {total} is next.",
                index + 1
            ),
            None => format!("Plan: all {total} steps closed."),
        },
    };
    let mut lines = vec![header];
    for (index, item) in items.iter().enumerate().take(MAX_STEPS_SHOWN) {
        let mark = match item.status {
            TodoStatus::Completed => "[x]",
            TodoStatus::InProgress => "[>]",
            TodoStatus::Pending => "[ ]",
            TodoStatus::Cancelled => "[-]",
        };
        let mut line = format!(
            "  {mark} {}. {}",
            index + 1,
            one_line(&item.content, MAX_STEP_CHARS)
        );
        if let Some(reason) = item
            .reason
            .as_deref()
            .filter(|_| item.status == TodoStatus::Cancelled)
        {
            line.push_str(&format!(
                " (cancelled: {})",
                one_line(reason, MAX_STEP_CHARS)
            ));
        }
        lines.push(line);
    }
    if total > MAX_STEPS_SHOWN {
        lines.push(format!("  … {} more", total - MAX_STEPS_SHOWN));
    }
    Some(lines.join("\n"))
}

/// How long ago round `then` was, counting the latest round as 1: the block is
/// rendered after the latest round's results are in.
fn age(now: usize, then: usize) -> String {
    match now.saturating_sub(then) + 1 {
        1 => "last round".to_string(),
        rounds => format!("{rounds} rounds ago"),
    }
}

#[cfg(test)]
#[path = "render_tests.rs"]
mod tests;
