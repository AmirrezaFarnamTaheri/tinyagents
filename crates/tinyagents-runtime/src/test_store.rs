//! Test fixtures over a store-port transcript locator.
//!
//! Runtime tests bind sessions to an in-memory locator and read back what the
//! session persisted by stem, the way a host reads a store, rather than
//! naming any file.

use std::sync::Arc;

use tinyagents_session::port::InMemoryTranscriptLocator;
use tinyagents_session::transcript::{
    DisplaySessionTranscript, SessionTranscript, TranscriptLocator, TranscriptMessage,
    TranscriptMeta, TranscriptTurn,
};

/// An empty in-memory transcript store.
pub(crate) fn memory() -> Arc<InMemoryTranscriptLocator> {
    Arc::new(InMemoryTranscriptLocator::new("runtime-tests"))
}

/// Writes `messages` as the first turn of the transcript `stem`.
pub(crate) fn seed(
    locator: &dyn TranscriptLocator,
    stem: &str,
    messages: &[TranscriptMessage],
    meta: &TranscriptMeta,
) {
    locator
        .open_stem(stem, meta.clone())
        .expect("open stem")
        .append_turn(TranscriptTurn {
            prev: &[],
            next: messages,
            meta,
            turn_usage: None,
            request_id: None,
            tools: None,
        })
        .expect("seed transcript");
}

/// The model-context replay of the transcript `stem`; panics when it is absent.
pub(crate) fn read(locator: &dyn TranscriptLocator, stem: &str) -> SessionTranscript {
    try_read(locator, stem).unwrap_or_else(|| panic!("transcript {stem} is absent"))
}

/// The model-context replay of the transcript `stem`, when it exists.
pub(crate) fn try_read(locator: &dyn TranscriptLocator, stem: &str) -> Option<SessionTranscript> {
    locator
        .read_session_transcript(&session_ref_of(stem))
        .and_then(|handle| handle.read_session().expect("read transcript"))
}

/// The display projection of the transcript `stem`; panics when it is absent.
pub(crate) fn read_display(locator: &dyn TranscriptLocator, stem: &str) -> DisplaySessionTranscript {
    locator
        .read_session_transcript(&session_ref_of(stem))
        .and_then(|handle| handle.read_display().expect("read display"))
        .unwrap_or_else(|| panic!("transcript {stem} is absent"))
}
