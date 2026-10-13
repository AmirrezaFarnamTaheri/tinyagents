//! Conversation entry tree: append-only, branchable session history.
//!
//! This module holds the tree's types and its pure projection; the tree itself
//! is stored by the run ledger
//! ([`RunLedgerStore::entry_append`](crate::run_ledger::RunLedgerStore::entry_append)
//! and its siblings), so it lives on whichever backend the host opened. See
//! `docs/modules/session/README.md` for the design write-up (entry kinds, the
//! context-projection rule, fork semantics, and the legacy import path). In
//! short: every entry has an [`EntryId`] and an optional `parent_id`; a session
//! is a *tree*, not a list; a tip is any entry with no children (or a
//! [`Branch`] label on one);
//! [`RunLedgerStore::entry_build_context`](crate::run_ledger::RunLedgerStore::entry_build_context)
//! projects one tip's ancestor chain to a model-ready message list, stopping at
//! the newest compaction; and
//! [`entry_fork`](crate::run_ledger::RunLedgerStore::entry_fork) creates a new
//! tip without disturbing existing ones.
//!
//! Pre-tree data (a JSONL transcript or the session message history) has no
//! parent pointer. [`legacy::from_transcript`] and
//! [`legacy::from_session_messages`] derive one deterministically (entry *n*
//! parents entry *n+1*, in file/row order) so that data reads into this same
//! model;
//! [`entry_import_legacy`](crate::run_ledger::RunLedgerStore::entry_import_legacy)
//! persists the result idempotently.

mod compaction_sink;
pub mod legacy;
mod types;

use tinyagents_harness::tinyinference_llm::message::{CustomMessage, SystemMessage, UserMessage};
use tinyagents_harness::tinyinference_llm::{ContentBlock, Message};

pub use compaction_sink::SessionCompactionSink;
pub use legacy::{from_messages, from_session_messages, from_transcript};
pub use types::{
    Branch, BranchSummaryEntry, CompactionEntry, CustomEntry, Entry, EntryId, EntryKind, Fork,
    ForkPosition, ForkScope, LabelEntry,
};

/// Shared pure projection; storage admission and chain lookup belong to callers.
pub(crate) fn project_chain(chain: &[Entry]) -> Vec<Message> {
    let compaction = chain
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, entry)| match &entry.kind {
            EntryKind::Compaction(c) => Some((idx, c.clone())),
            _ => None,
        });

    let mut messages = Vec::new();
    let start_idx = match compaction {
        Some((idx, compaction)) => {
            messages.push(match compaction.placement {
                tinyagents_harness::summarization::SummaryPlacement::User => {
                    Message::user(compaction.summary.clone())
                }
                tinyagents_harness::summarization::SummaryPlacement::System => {
                    Message::System(SystemMessage {
                        content: vec![ContentBlock::Text(compaction.summary.clone())],
                        ..SystemMessage::default()
                    })
                }
            });
            let start_idx = chain
                .iter()
                .position(|e| e.id == compaction.first_kept_entry_id)
                .unwrap_or(idx + 1);
            // A user message the compaction pinned lies inside the folded
            // range but was kept verbatim: it follows the summary.
            if let Some(pinned) = compaction
                .details
                .get(compaction_sink::PINNED_ENTRY_ID)
                .and_then(|id| serde_json::from_value::<EntryId>(id.clone()).ok())
                .and_then(|id| chain[..start_idx].iter().find(|e| e.id == id))
                .and_then(|entry| entry_to_message(&entry.kind))
            {
                messages.push(pinned);
            }
            start_idx
        }
        None => 0,
    };

    for entry in &chain[start_idx..] {
        if let Some(message) = entry_to_message(&entry.kind) {
            messages.push(message);
        }
    }
    messages
}

/// Converts one entry's kind to a context message, or `None` for kinds that
/// carry no conversational content ([`EntryKind::Label`],
/// [`EntryKind::BranchSummary`], and a stray [`EntryKind::Compaction`] that
/// is not the boundary entry itself — [`project_chain`] never
/// passes one of those in, but the match stays exhaustive for safety).
fn entry_to_message(kind: &EntryKind) -> Option<Message> {
    match kind {
        EntryKind::Message(message) => Some(transcript_message_to_message(message)),
        EntryKind::Custom(custom) => Some(Message::Custom(CustomMessage {
            kind: custom.kind.clone(),
            payload: custom.payload.clone(),
            display: custom.display.clone(),
        })),
        EntryKind::Label(_) | EntryKind::BranchSummary(_) | EntryKind::Compaction(_) => None,
    }
}

/// Best-effort mapping from the durable, provider-neutral
/// [`crate::transcript::TranscriptMessage`] to an inference [`Message`].
///
/// `system`/`user`/`assistant` map directly to their typed counterparts as a
/// single text content block holding the row's
/// [`legacy_content`](crate::transcript::TranscriptMessage::legacy_content),
/// so typed tool calls, tool-result ids and image parts are kept inside the
/// text rather than as structured blocks. The round trip for richer providers
/// is therefore lossy here; callers that need typed fidelity should read the
/// row's own fields. Any other role, including `tool`, is carried through as
/// `Message::Custom` tagged `legacy:{role}` so no content is silently dropped.
fn transcript_message_to_message(message: &crate::transcript::TranscriptMessage) -> Message {
    let text = message.legacy_content();
    match message.role.as_str() {
        "system" => Message::System(SystemMessage {
            content: vec![ContentBlock::Text(text)],
            ..SystemMessage::default()
        }),
        "user" => Message::User(UserMessage {
            content: vec![ContentBlock::Text(text)],
        }),
        "assistant" => {
            Message::Assistant(tinyagents_harness::tinyinference_llm::AssistantMessage {
                id: message.id.clone(),
                content: vec![ContentBlock::Text(text)],
                tool_calls: Vec::new(),
                usage: None,
                origin: None,
            })
        }
        other => Message::Custom(CustomMessage {
            kind: format!("legacy:{other}"),
            payload: serde_json::json!({ "content": text }),
            display: Some(text),
        }),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
