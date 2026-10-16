//! Test fixtures: classic `session_raw/*.jsonl` files on disk, served through a
//! store-port locator.
//!
//! The crate no longer reads or writes a workspace directory at run time; the
//! one-time legacy import is the only reader of those files. Tests that build a
//! transcript fixture as raw JSONL therefore import it into an in-memory
//! locator and exercise the store-port APIs against that.

use std::path::Path;
use std::sync::Arc;

use tinystoragedrivers_core::MemoryStorage;

use super::TranscriptLocator;
use crate::port::{DriverSessionStores, SessionStoreProvider};

/// An empty driver-backed locator (the production store, over memory).
pub(crate) fn driver_locator() -> Arc<dyn TranscriptLocator> {
    let provider = DriverSessionStores::new(Arc::new(MemoryStorage::new()), tinystoragedrivers_core::Scope::local())
        .expect("driver session stores over memory");
    provider.for_agent("test").transcripts
}

/// A driver-backed locator holding every transcript under
/// `<workspace>/session_raw` (flat and date-grouped), imported id-preserving.
pub(crate) fn imported_locator(workspace: &Path) -> Arc<dyn TranscriptLocator> {
    let locator = driver_locator();
    crate::legacy::import_transcripts(workspace, locator.as_ref())
        .expect("import fixture transcripts");
    locator
}

/// [`thread_spend_in`](super::spend::thread_spend_in) over the fixtures
/// imported from `workspace`.
pub(crate) fn thread_spend(workspace: &Path, thread_id: &str) -> super::spend::ThreadSpend {
    super::spend::thread_spend_in(imported_locator(workspace).as_ref(), thread_id)
}
