//! Cross-generation row identity for a thread's transcript view.
//!
//! A compaction seals generation `n` and opens `{stem}.g{n+1}`, which inherits
//! `_meta.created` and opens with the retained message set rewritten.
//! Concatenating every generation would render the retained rows twice, so
//! [`drop_retained_rows`] removes the rewritten prefix when a successor is
//! projected (generations themselves arrive in chain order from the locator's
//! [`thread_transcripts`](crate::transcript::TranscriptLocator::thread_transcripts)).

use std::collections::HashMap;

use crate::transcript::{DisplayMessage, DisplayRecord};

/// Identity of one message row for cross-generation de-duplication.
type RowKey = (String, String, Option<String>, Option<String>, String);

fn row_key(msg: &DisplayMessage) -> RowKey {
    (
        msg.message.role.clone(),
        msg.message.content.clone(),
        msg.message.id.clone(),
        msg.request_id.clone(),
        // The typed structure moved out of `content`, so it is part of the
        // row's identity (as in `TranscriptMessage::same_row_as`).
        serde_json::to_string(&(
            &msg.message.tool_calls,
            &msg.message.tool_call_id,
            &msg.message.parts,
        ))
        .unwrap_or_default(),
    )
}

/// Multiset of the message rows of one generation, for [`drop_retained_rows`].
pub(super) fn generation_rows(records: &[DisplayRecord]) -> HashMap<RowKey, usize> {
    let mut rows = HashMap::new();
    for record in records {
        if let DisplayRecord::Message(msg) = record {
            *rows.entry(row_key(msg)).or_insert(0) += 1;
        }
    }
    rows
}

/// Split a successor generation's records into `(new records, retained rows)`.
///
/// A successor opens with the retained set rewritten verbatim (same role,
/// content, id and — because retained rows keep their own correlation id —
/// the same `request_id`). Each such row consumes one occurrence from the
/// predecessor's multiset, so a genuinely repeated row beyond what the
/// predecessor held still renders.
pub(super) fn drop_retained_rows(
    records: &[DisplayRecord],
    mut predecessor: HashMap<RowKey, usize>,
) -> (Vec<DisplayRecord>, Vec<DisplayMessage>) {
    let mut kept = Vec::with_capacity(records.len());
    let mut retained = Vec::new();
    let mut retained_prefix_open = true;
    for record in records {
        if retained_prefix_open
            && let DisplayRecord::Message(msg) = record
            && let Some(count) = predecessor.get_mut(&row_key(msg))
            && *count > 0
        {
            *count -= 1;
            retained.push((**msg).clone());
            continue;
        }
        retained_prefix_open = false;
        kept.push(record.clone());
    }
    (kept, retained)
}

#[cfg(test)]
#[path = "resolve_tests.rs"]
mod tests;
