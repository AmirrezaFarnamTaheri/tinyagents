//! Test fixtures: classic `session_raw/*.jsonl` files on disk, served through a
//! store-port locator.
//!
//! The crate no longer reads or writes a workspace directory at run time; the
//! one-time legacy import is the only reader of those files. Tests that build a
//! transcript fixture as raw JSONL therefore import it into an in-memory
//! locator and exercise the store-port APIs against that.

use std::path::Path;

use crate::port::InMemoryTranscriptLocator;

/// An in-memory locator holding every transcript under
/// `<workspace>/session_raw` (flat and date-grouped), imported id-preserving.
pub(crate) fn imported_locator(workspace: &Path) -> InMemoryTranscriptLocator {
    let locator = InMemoryTranscriptLocator::new("test-import");
    crate::legacy::import_transcripts(workspace, &locator).expect("import fixture transcripts");
    locator
}

/// [`thread_spend_in`](super::spend::thread_spend_in) over the fixtures
/// imported from `workspace`.
pub(crate) fn thread_spend(workspace: &Path, thread_id: &str) -> super::spend::ThreadSpend {
    super::spend::thread_spend_in(&imported_locator(workspace), thread_id)
}
