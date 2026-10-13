//! [`tinyagents_harness::summarization::CompactionSink`] implementation
//! backed by the entry tree.
//!
//! `tinyagents-harness` cannot depend on `tinyagents-session` (the
//! dependency runs the other way — see that trait's docs), so the harness
//! only defines the interface; this is the durable, session-backed
//! implementation a host attaches to a [`RunContext`][tinyagents_harness::context::RunContext]
//! via `RunContext::with_compaction_sink`.
//!
//! [`CompactionSink::persist`] is synchronous, so each call reaches the
//! run ledger's async entry-tree operations through a [`Blocking`] bridge
//! thread; that works from inside or outside a tokio runtime.

use std::sync::{Arc, Mutex};

use tinystoragedrivers_core::Blocking;

use tinyagents_harness::error::{Result, TinyAgentsError};
use tinyagents_harness::summarization::{CompactionRecord, CompactionSink};

use super::types::{CompactionEntry, EntryId, EntryKind};
use crate::run_ledger::RunLedgerStore;

/// A [`CompactionSink`] that persists every [`CompactionRecord`] a run
/// produces into a session's entry tree as a durable
/// [`CompactionEntry`][crate::entry_tree::CompactionEntry], anchored to the
/// tree's current head at the moment [`Self::persist`] is called.
///
/// # Index translation
///
/// [`CompactionRecord::first_kept_index`] is a position in the *harness's*
/// non-system message slice, not an [`EntryId`] the entry tree understands.
/// `persist` translates it by re-walking the ancestor chain of the current
/// tip and counting only the entries
/// [`project_chain`](super::project_chain) itself turns into messages
/// ([`EntryKind::Message`] and [`EntryKind::Custom`] — the same filter
/// `entry_to_message` applies internally), so the index lines up with what a
/// harness compaction step actually operated on. A record whose index falls
/// outside that count (for example, one produced from a transcript this
/// session never saw) is skipped rather than persisted as a boundary that
/// would corrupt the projected context.
/// [`CompactionRecord::details`] key carrying the live index of a user
/// message the harness pinned out of the folded range.
const PINNED_USER_INDEX: &str = "pinned_user_index";

/// [`CompactionEntry::details`] key carrying that message's [`EntryId`], read
/// back by [`project_chain`](super::project_chain).
pub(crate) const PINNED_ENTRY_ID: &str = "pinned_entry_id";

pub struct SessionCompactionSink {
    ledger: Arc<dyn RunLedgerStore>,
    session_id: String,
    bridge: Blocking,
    tip: Mutex<Option<EntryId>>,
}

fn ledger_error(error: impl std::fmt::Display) -> TinyAgentsError {
    TinyAgentsError::Storage(format!("compaction sink: {error}"))
}

impl SessionCompactionSink {
    /// Builds a sink anchored to `session_id`'s current head at construction
    /// time. The tip advances to each new compaction entry as
    /// [`Self::persist`] is called, so a run that compacts more than once
    /// keeps appending to the same branch instead of re-anchoring at the
    /// original head every time.
    ///
    /// # Errors
    ///
    /// The bridge thread cannot start, or the ledger cannot read the head.
    pub fn new(ledger: Arc<dyn RunLedgerStore>, session_id: impl Into<String>) -> Result<Self> {
        let session_id = session_id.into();
        let bridge = Blocking::new().map_err(ledger_error)?;
        let tip = {
            let ledger = Arc::clone(&ledger);
            let session_id = session_id.clone();
            bridge
                .run(async move { ledger.entry_head(&session_id).await })
                .map_err(ledger_error)?
                .map_err(ledger_error)?
        };
        Ok(Self {
            ledger,
            session_id,
            bridge,
            tip: Mutex::new(tip),
        })
    }

    /// The session whose entry tree this sink writes to.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The current tip this sink will anchor its next compaction entry to.
    pub fn tip(&self) -> Option<EntryId> {
        self.tip.lock().expect("tip mutex poisoned").clone()
    }
}

impl CompactionSink for SessionCompactionSink {
    fn persist(&self, record: &CompactionRecord) -> Result<()> {
        let mut tip_guard = self.tip.lock().expect("tip mutex poisoned");
        let Some(tip) = tip_guard.clone() else {
            // Nothing has been appended to this session yet — there is
            // nothing to anchor a compaction boundary to.
            return Ok(());
        };

        let chain = {
            let ledger = Arc::clone(&self.ledger);
            let session_id = self.session_id.clone();
            let tip = tip.clone();
            self.bridge
                .run(async move { ledger.entry_ancestor_chain(&session_id, &tip).await })
                .map_err(ledger_error)?
                .map_err(ledger_error)?
        };
        let message_entry_ids: Vec<EntryId> = chain
            .iter()
            .filter(|entry| matches!(entry.kind, EntryKind::Message(_) | EntryKind::Custom(_)))
            .map(|entry| entry.id.clone())
            .collect();

        let Some(first_kept_entry_id) = message_entry_ids.get(record.first_kept_index).cloned()
        else {
            return Ok(());
        };

        // A user message the compaction pinned out of the folded range is
        // recorded by entry id too, so `build_context` restores it after the
        // summary instead of dropping it with the rest of the folded entries.
        let mut details = record.details.clone();
        if let Some(pinned_entry_id) = details
            .get(PINNED_USER_INDEX)
            .and_then(serde_json::Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| message_entry_ids.get(index))
        {
            details[PINNED_ENTRY_ID] = serde_json::json!(pinned_entry_id);
        }

        let entry = EntryKind::Compaction(CompactionEntry {
            summary: record.summary.clone(),
            placement: record.placement,
            first_kept_entry_id,
            tokens_before: record.tokens_before,
            usage: record.usage,
            details,
        });
        let new_tip = {
            let ledger = Arc::clone(&self.ledger);
            let session_id = self.session_id.clone();
            let parent = tip.clone();
            self.bridge
                .run(async move { ledger.entry_append(&session_id, Some(&parent), entry).await })
                .map_err(ledger_error)?
                .map_err(ledger_error)?
        };
        *tip_guard = Some(new_tip);
        Ok(())
    }
}
