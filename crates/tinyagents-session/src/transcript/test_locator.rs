//! Test fixtures: classic `session_raw/*.jsonl` files read into an in-memory
//! locator. The file reader is used here only to seed view fixtures.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use super::{
    DisplayRecord, TranscriptHistory, TranscriptLocator, TranscriptTurn, read_transcript_display,
};
use crate::port::SessionStoreProvider;

/// An empty driver-backed locator (the production store, over memory).
pub(crate) fn driver_locator() -> Arc<dyn TranscriptLocator> {
    crate::testkit::in_memory_transcript()
}

/// A fixture locator holding parsed transcript rows from `<workspace>`.
pub(crate) fn imported_locator(workspace: &Path) -> Arc<dyn TranscriptLocator> {
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files)?;
            } else if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                files.push(path);
            }
        }
        Ok(())
    }

    let locator = crate::port::InMemorySessionStores::new()
        .for_agent("fixture")
        .transcripts;
    let raw = workspace.join("session_raw");
    let mut files = Vec::new();
    if raw.exists() {
        visit(&raw, &mut files).expect("walk transcript fixtures");
    }
    files.sort();
    for path in files {
        let transcript = read_transcript_display(&path).expect("read transcript fixture");
        let Some(stem) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".jsonl"))
        else {
            continue;
        };
        let history = locator
            .open_stem(stem, transcript.meta.clone())
            .expect("open fixture transcript");
        let mut previous = Vec::new();
        for record in transcript.records {
            let (next, request_id) = match record {
                DisplayRecord::Message(message) if message.interrupted => continue,
                DisplayRecord::Message(message) => {
                    let mut next = previous.clone();
                    next.push(message.message);
                    (next, message.request_id)
                }
                DisplayRecord::Compaction(marker) => (
                    marker
                        .replacement
                        .into_iter()
                        .map(|message| message.message)
                        .collect(),
                    marker.request_id,
                ),
            };
            history
                .append_turn(TranscriptTurn {
                    prev: &previous,
                    next: &next,
                    meta: &transcript.meta,
                    turn_usage: None,
                    request_id: request_id.as_deref(),
                    tools: None,
                })
                .expect("seed fixture transcript record");
            previous = next;
        }
    }
    locator
}

/// [`thread_spend_in`](super::spend::thread_spend_in) over the fixtures
/// imported from `workspace`.
pub(crate) fn thread_spend(workspace: &Path, thread_id: &str) -> super::spend::ThreadSpend {
    super::spend::thread_spend_in(imported_locator(workspace).as_ref(), thread_id)
}
