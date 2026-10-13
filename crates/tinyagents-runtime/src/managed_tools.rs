//! Refresh only an attached-tool catalogue inside a frozen prefix.

use std::{collections::HashSet, sync::Arc};

use tinyinference_llm::message::Message;
use tinytools::ToolSpec;

use crate::PrefixSnapshot;

/// Rebuild `prefix` so its trailing system message (opened by `marker`) lists
/// the named tools, leaving every other prefix message untouched.
///
/// Returns `None` when `names` is empty or the prefix would not change. The
/// session freezes its prefix after the first committed turn; this catalogue
/// is the sole permitted refresh, and the runtime starts a successor transcript
/// generation when it changes. `marker` opens the message and `closing` ends it.
#[must_use]
pub fn refresh_prefix(
    prefix: &PrefixSnapshot,
    specs: &[Arc<ToolSpec>],
    names: &HashSet<String>,
    marker: &str,
    closing: &str,
) -> Option<PrefixSnapshot> {
    if names.is_empty() {
        return None;
    }
    let mut messages: Vec<Message> = prefix
        .messages()
        .iter()
        .filter(|message| !message.text().starts_with(marker))
        .cloned()
        .collect();
    let mut entries: Vec<&ToolSpec> = specs
        .iter()
        .filter(|spec| names.contains(&spec.name))
        .map(AsRef::as_ref)
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let catalogue = entries
        .iter()
        .map(|spec| {
            format!(
                "{}: {}\nParameters: {}",
                spec.name, spec.description, spec.parameters
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    messages.push(Message::system(format!("{marker}{catalogue}\n{closing}")));
    if messages == prefix.messages() {
        None
    } else {
        Some(PrefixSnapshot::new(messages).refreshing())
    }
}

/// Fail before deduplication when a new permanent tool name would shadow an
/// already-synthesized tool, so the synthesized executors and schemas survive.
pub fn reject_synthesized_collisions(
    names: &HashSet<String>,
    synthesized: &[Box<dyn tinytools::Tool>],
) -> Result<(), String> {
    match synthesized.iter().find(|tool| names.contains(tool.name())) {
        Some(tool) => Err(format!(
            "permanent tool name collision with synthesized tool: {}",
            tool.name()
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "managed_tools_tests.rs"]
mod tests;
