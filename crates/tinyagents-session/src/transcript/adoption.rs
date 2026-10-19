//! The result of folding a conversation's pre-identity transcripts into its
//! session ([`TranscriptLocator::adopt_legacy`](super::TranscriptLocator::adopt_legacy)).

use std::borrow::Cow;
use std::path::PathBuf;

use super::TranscriptMessage;
use tinyagents_harness::tinyinference_llm::message::Message;

/// Removes only the contiguous orphan tool-result head following the system prefix.
///
/// This is a replay view: durable rows are left untouched, and later tool rows
/// remain visible to strict transcript validation.
pub fn repair_orphaned_tool_head(rows: &[TranscriptMessage]) -> Cow<'_, [TranscriptMessage]> {
    let prefix = rows.iter().take_while(|row| row.role == "system").count();
    let orphans = rows[prefix..]
        .iter()
        .take_while(|row| row.role == "tool")
        .count();
    if orphans == 0 {
        Cow::Borrowed(rows)
    } else {
        Cow::Owned(
            rows[..prefix]
                .iter()
                .chain(&rows[prefix + orphans..])
                .cloned()
                .collect(),
        )
    }
}

/// Removes only the contiguous orphan tool-result head from decoded replay history.
pub fn repair_orphaned_tool_head_messages(history: &[Message]) -> Cow<'_, [Message]> {
    let prefix = history
        .iter()
        .take_while(|message| matches!(message, Message::System(_)))
        .count();
    let orphans = history[prefix..]
        .iter()
        .take_while(|message| matches!(message, Message::Tool(_)))
        .count();
    if orphans == 0 {
        Cow::Borrowed(history)
    } else {
        Cow::Owned(
            history[..prefix]
                .iter()
                .chain(&history[prefix + orphans..])
                .cloned()
                .collect(),
        )
    }
}

#[cfg(test)]
#[path = "adoption_tests.rs"]
mod tests;

/// What adoption did for one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAdoption {
    /// The transcript now backing the session.
    pub path: PathBuf,
    /// The legacy roots folded into it, oldest first.
    pub adopted: Vec<PathBuf>,
    /// Messages carried over.
    pub messages: usize,
}
