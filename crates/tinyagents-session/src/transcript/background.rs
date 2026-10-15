//! Out-of-band appends into a session's durable transcript.
//!
//! A conversation's transcript is normally written only by its own live turns.
//! Some work finishes *outside* any turn but belongs in the conversation — a
//! scheduled job created from a chat reports back into that chat, so the next
//! turn can see it ("did you remind me?"). [`append_background_message_in`] is
//! that writer: one plain assistant message, appended to the session's head
//! generation, idempotent per caller key, and marked with where it came from.
//!
//! # Concurrency contract
//!
//! The live turn path appends a *delta* against the baseline it read when the
//! turn started, and validates that baseline when it writes. A foreign
//! line written between that read and the persist makes the baseline stale and
//! the user's turn fails to persist. So:
//!
//! - **A live turn must hold [`lock_session_turn`](super::lock_session_turn) from its resume read
//!   through its persist.** `tinyagents-runtime`'s `Session::turn` does this
//!   for every session-bound target whose locator names its destination.
//!   A host driving the transcript some other way must take the lock itself.
//! - **The background append awaits that same lock**, so it lands *between*
//!   turns, never inside one. It can therefore wait for as long as a turn
//!   runs; bound it with a timeout if the caller cannot wait.
//! - **Never await it from inside a turn of the same session** (for example a
//!   tool that delivers into its own conversation): the lock is not reentrant
//!   and that deadlocks.
//! - **The next turn must re-read the transcript** to see the message, which a
//!   turn resumed with `ResumeMode::Session` (or any mode other than `Never`)
//!   does on every turn. A session whose turns never resume keeps a stale
//!   baseline and its next persist fails rather than hiding the line.
//! - The turn lock is **in-process**. Another process appending mid-turn is
//!   still caught by the baseline check (that turn fails to persist; nothing is
//!   corrupted), and the append itself goes through the store's own write
//!   path like every other writer.

use super::display::with_background;
use super::history::seed_meta_for_discovered;
use super::history::TranscriptLocator;
use super::session::{SessionRef, session_stem};
use super::turn_lock::lock_session_turn_or_identity;
use super::types::{
    BackgroundAppend, BackgroundAppendOutcome, BackgroundOrigin, DisplayRecord, TranscriptMessage,
};

/// How many times the head may move under an unpinned append before it gives
/// up. Each move is a compaction or a fork landing between head resolution
/// and the write lock; more than a couple in a row means something is wrong.
const MAX_HEAD_MOVES: usize = 3;

/// Appends `message` — one plain assistant message — to the head generation of
/// `session` under `locator`, outside any turn.
///
/// - **Head only.** The head is resolved from the session's first generation,
///   whatever `session.generation` says. With
///   [`BackgroundAppend::expected_generation`] set, a head that has moved on
///   returns [`BackgroundAppendOutcome::StaleGeneration`] and writes nothing.
/// - **Idempotent.** A head generation whose *live* context already holds a
///   message delivered with the same [`BackgroundAppend::idempotency_key`]
///   returns [`BackgroundAppendOutcome::Duplicate`] and writes nothing. The
///   live context is what a compaction left: rows written before a
///   compaction of the same generation count only if that compaction's
///   replacement kept them (a kept row keeps its provenance in
///   `extra_metadata`). A key a compaction dropped is therefore appended
///   again, in the same generation or in a successor.
/// - **Never creates a session.** No transcript yet is
///   [`BackgroundAppendOutcome::NoSession`].
///
/// The message is appended through the head's
/// [`TranscriptHistory`](super::TranscriptHistory) handle, with the provenance
/// recorded in the row's `extra_metadata` under
/// [`BACKGROUND_METADATA_KEY`](super::BACKGROUND_METADATA_KEY), which the
/// display read lifts back into [`DisplayMessage::background`](super::DisplayMessage::background).
///
/// Awaits the session's turn lock first; see the module documentation for the
/// contract this places on live turns. The turn lock is the only
/// serialisation: the duplicate check and the append are two calls, so two
/// processes appending the same key to one store at once could both write it.
/// A locator with no [`destination_key`](TranscriptLocator::destination_key)
/// is locked by its own identity instead, so appends through that one locator
/// still serialise.
///
/// # Errors
///
/// When `message` is not a plain assistant message (another role, tool calls,
/// or an interrupted partial), when the key is empty, or on any store error.
pub async fn append_background_message_in(
    locator: &dyn TranscriptLocator,
    session: &SessionRef,
    message: TranscriptMessage,
    options: BackgroundAppend,
) -> anyhow::Result<BackgroundAppendOutcome> {
    append_background_message_impl(locator, session, message, options).await
}

async fn append_background_message_impl(
    locator: &dyn TranscriptLocator,
    session: &SessionRef,
    message: TranscriptMessage,
    options: BackgroundAppend,
) -> anyhow::Result<BackgroundAppendOutcome> {
    let message = validate(message, &options)?;
    let root = session.first_generation();
    let stem = session_stem(&root);
    let key = options.idempotency_key.as_str();
    tracing::debug!(session = %stem, idempotency_key = %key, expected_generation = ?options.expected_generation, "[transcript-background] store append requested");

    // Falls back to the locator's own identity when it names no destination,
    // so two appends through one such locator still serialise their
    // duplicate check and append.
    let _turn = lock_session_turn_or_identity(locator, &root).await;
    for _ in 0..MAX_HEAD_MOVES {
        let head = locator.head_generation(&root);
        if !locator.session_exists(&head) {
            tracing::debug!(session = %stem, idempotency_key = %key, "[transcript-background] no transcript in the store; nothing written");
            return Ok(BackgroundAppendOutcome::NoSession);
        }
        if let Some(stale) = stale(&options, head.generation) {
            return Ok(stale);
        }
        match append_to_stored_head(locator, &head, &message, &options)? {
            Attempt::Done(outcome) => {
                tracing::debug!(session = %stem, idempotency_key = %key, ?outcome, "[transcript-background] store append finished");
                return Ok(outcome);
            }
            Attempt::HeadMoved => {
                tracing::debug!(session = %stem, idempotency_key = %key, generation = head.generation, "[transcript-background] store head sealed during append; re-resolving");
            }
        }
    }
    anyhow::bail!(
        "session {stem} head kept moving during a background append ({MAX_HEAD_MOVES} attempts)"
    )
}

/// One attempt against a store's resolved `head`.
fn append_to_stored_head(
    locator: &dyn TranscriptLocator,
    head: &SessionRef,
    message: &TranscriptMessage,
    options: &BackgroundAppend,
) -> anyhow::Result<Attempt> {
    if locator.session_exists(&head.next_generation()) {
        return Ok(Attempt::HeadMoved);
    }
    let generation = head.generation;
    let handle = locator.open_session(head, seed_meta_for_discovered(""))?;
    let key = options.idempotency_key.as_str();
    let held = handle
        .read_display()?
        .is_some_and(|display| live_context_holds_key(&display.records, key));
    if held {
        return Ok(Attempt::Done(BackgroundAppendOutcome::Duplicate {
            generation,
        }));
    }
    let origin = BackgroundOrigin {
        idempotency_key: options.idempotency_key.clone(),
        provenance: options.provenance.clone(),
    };
    if let Err(error) = handle.append(with_background(message.clone(), &origin)) {
        // A compaction sealed the head between the check and the write.
        if locator.session_exists(&head.next_generation()) {
            return Ok(Attempt::HeadMoved);
        }
        return Err(error);
    }
    Ok(Attempt::Done(BackgroundAppendOutcome::Appended {
        generation,
    }))
}

/// Whether the *live* context of `records` holds a message delivered with
/// `key`. A compaction replaces the context accumulated so far, so only rows
/// after the last compaction and that compaction's own replacement count; a
/// key the compaction dropped is not held.
fn live_context_holds_key(records: &[DisplayRecord], key: &str) -> bool {
    let mut held = false;
    for record in records {
        match record {
            DisplayRecord::Message(message) => {
                held |= carries_key(message.background.as_ref(), key);
            }
            DisplayRecord::Compaction(compaction) => {
                held = compaction
                    .replacement
                    .iter()
                    .any(|message| carries_key(message.background.as_ref(), key));
            }
        }
    }
    held
}

fn carries_key(origin: Option<&BackgroundOrigin>, key: &str) -> bool {
    origin.is_some_and(|origin| origin.idempotency_key == key)
}

/// The result of one locked attempt against a resolved head.
enum Attempt {
    Done(BackgroundAppendOutcome),
    /// A successor generation appeared between resolution and the lock.
    HeadMoved,
}

/// `StaleGeneration` when the caller pinned a generation other than `head`.
fn stale(options: &BackgroundAppend, head: u32) -> Option<BackgroundAppendOutcome> {
    options
        .expected_generation
        .filter(|expected| *expected != head)
        .map(|expected| BackgroundAppendOutcome::StaleGeneration { expected, head })
}

/// Rejects anything but a plain, complete assistant message with a usable key.
///
/// A tool-calling row would leave calls with no results in the model context,
/// and an interrupted row would be skipped by the model-context reader — the
/// opposite of what a delivery is for.
fn validate(
    message: TranscriptMessage,
    options: &BackgroundAppend,
) -> anyhow::Result<TranscriptMessage> {
    let normalized = message.normalized();
    anyhow::ensure!(
        !options.idempotency_key.trim().is_empty(),
        "background append needs a non-empty idempotency key"
    );
    anyhow::ensure!(
        normalized.role == "assistant",
        "background append only writes assistant messages, got role `{}`",
        normalized.role
    );
    anyhow::ensure!(
        !normalized.is_typed()
            && normalized
                .turn_usage
                .as_ref()
                .is_none_or(|usage| usage.tool_calls.is_empty())
            && !normalized.interrupted
            && normalized.tool_failure.is_none(),
        "background append only writes plain, complete assistant messages"
    );
    Ok(normalized)
}

#[cfg(test)]
#[path = "background_tests.rs"]
mod tests;
