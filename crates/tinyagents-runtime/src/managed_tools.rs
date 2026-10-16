//! Refresh only an attached-tool catalogue inside a frozen prefix.

use std::{collections::HashSet, sync::Arc};

use tinyinference_llm::message::Message;
use tinytools::ToolSpec;

use crate::PrefixSnapshot;

/// The host-neutral visibility sets for a turn's managed tools.
///
/// `visible` is the executable model surface, `deferred` is searchable but
/// omitted from that surface, and `permanent` is always directly visible.
/// Hosts still decide which tools are managed and which discovery policy to
/// apply; this type keeps the set algebra consistent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManagedToolNames {
    /// Tools advertised directly to the model.
    pub visible: HashSet<String>,
    /// Tools available through a discovery/search mechanism.
    pub deferred: HashSet<String>,
    /// Tools that remain directly advertised across synthesized refreshes.
    pub permanent: HashSet<String>,
}

impl ManagedToolNames {
    /// Reconciles the synthesized portion of visibility at a turn boundary.
    /// Revoked names are always removed. New names are admitted only when the
    /// host's existing wildcard policy opts into them. Permanent names are
    /// restored after the refresh and removed from the deferred set.
    pub fn refresh_synthesized(
        &mut self,
        previous: &HashSet<String>,
        next: &HashSet<String>,
        auto_include_new: bool,
    ) {
        for name in previous.difference(next) {
            self.visible.remove(name);
            self.deferred.remove(name);
        }
        if auto_include_new {
            self.visible.extend(next.iter().cloned());
        }
        self.visible.extend(self.permanent.iter().cloned());
        self.deferred.retain(|name| !self.permanent.contains(name));
    }

    /// Applies deferred classification to synthesized names. Hidden names
    /// stay hidden because this only moves names already in `visible`.
    pub fn defer_visible(&mut self, names: &HashSet<String>) {
        let moved = self
            .visible
            .intersection(names)
            .filter(|name| !self.permanent.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        for name in moved {
            self.visible.remove(&name);
            self.deferred.insert(name);
        }
    }

    /// Promotes names to permanent visibility and removes them from deferred.
    pub fn make_permanent(&mut self, names: impl IntoIterator<Item = String>) {
        self.permanent.extend(names);
        self.visible.extend(self.permanent.iter().cloned());
        self.deferred.retain(|name| !self.permanent.contains(name));
    }
}

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
