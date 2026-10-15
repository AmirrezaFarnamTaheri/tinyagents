//! Test-only workspace-path shims over the locator-based view API.
//!
//! View fixtures are written as raw classic JSONL under a temp workspace; each
//! shim imports that workspace into an in-memory locator and calls the
//! store-port function, so the projection under test is the production one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::types::ProjectedTranscript;
use super::{SpawnCallLookup, TranscriptPage, get_page_in, project};
use crate::transcript::test_locator::imported_locator;
use crate::transcript::history::seed_meta_for_discovered;
use crate::transcript::{ThreadTranscript, TranscriptLocator, TranscriptRead};

pub(crate) fn get_page(
    workspace_dir: &Path,
    thread_id: &str,
    cursor: Option<&str>,
    limit: Option<usize>,
) -> TranscriptPage {
    get_page_in(
        &imported_locator(workspace_dir),
        thread_id,
        None,
        cursor,
        limit,
    )
}

pub(crate) fn project_thread(workspace_dir: &Path, thread_id: &str) -> Option<ProjectedTranscript> {
    project::project_thread_in(&imported_locator(workspace_dir), thread_id, None, None)
}

pub(crate) fn project_thread_scoped_with(
    workspace_dir: &Path,
    thread_id: &str,
    agent_id: Option<&str>,
    lookup: &dyn SpawnCallLookup,
) -> Option<ProjectedTranscript> {
    project::project_thread_in(
        &imported_locator(workspace_dir),
        thread_id,
        agent_id,
        Some(lookup),
    )
}

/// The transcript files (as `session_raw/<stem>.jsonl` paths) the locator
/// lists for `thread_id`: root generations in chain order, then sub-agents.
pub(crate) fn resolve_files(
    workspace_dir: &Path,
    thread_id: &str,
) -> Option<(Vec<PathBuf>, Vec<PathBuf>)> {
    let listed = imported_locator(workspace_dir).thread_transcripts(thread_id, None)?;
    let path = |t: &ThreadTranscript| {
        workspace_dir
            .join("session_raw")
            .join(format!("{}.jsonl", t.stem))
    };
    Some((
        listed.roots.iter().map(path).collect(),
        listed.subagents.iter().map(path).collect(),
    ))
}

/// Projects the given fixture files (imported into one locator) as a thread's
/// roots and sub-agents.
pub(crate) fn project_from_files(
    thread_id: &str,
    root_paths: &[PathBuf],
    sub_paths: &[PathBuf],
    lookup: Option<&dyn SpawnCallLookup>,
) -> ProjectedTranscript {
    let workspace = root_paths
        .first()
        .or_else(|| sub_paths.first())
        .and_then(|path| path.parent())
        .and_then(Path::parent)
        .expect("fixture files live under <workspace>/session_raw");
    let locator = imported_locator(workspace);
    let handles = |paths: &[PathBuf]| -> Vec<ThreadTranscript> {
        paths
            .iter()
            .map(|path| {
                let stem = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .expect("utf-8 stem")
                    .to_owned();
                let transcript: Arc<dyn TranscriptRead> = locator
                    .open_stem(&stem, seed_meta_for_discovered(&stem))
                    .expect("fixture stem imported");
                ThreadTranscript { stem, transcript }
            })
            .collect()
    };
    project::project_transcripts(thread_id, &handles(root_paths), &handles(sub_paths), lookup)
}
