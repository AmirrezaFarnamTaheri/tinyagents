//! Pre-import maintenance of classic JSONL transcripts.
//!
//! A host that must fix legacy content before it is imported (for example a
//! migration that strips a retired prompt section from every persisted
//! system message) used to walk `session_raw/` and call the path-taking
//! reader and writer itself. [`rewrite_legacy_transcripts`] owns that walk
//! here, beside the importer that owns the format, so the host names no file.

use std::path::{Path, PathBuf};

use super::transcripts::raw_dir;
use super::types::LegacyRewriteReport;
use crate::transcript::{SessionTranscript, read_transcript, write_transcript};

const LOG_PREFIX: &str = "[session:legacy:rewrite]";

/// Applies `rewrite` to every classic transcript under
/// `<workspace_dir>/session_raw/` (flat and `DDMMYYYY/` date-grouped
/// `*.jsonl`), writing a transcript back in full when `rewrite` returns
/// `true`. A rewritten file is a full rewrite of its reduced context (its
/// pre-compaction history is not kept), exactly as the classic writer's
/// `write_transcript` produces.
///
/// Run it before the workspace is imported: the import is one-time, so a
/// file changed afterwards is not re-read. Per-file read or write failures
/// are counted in [`LegacyRewriteReport::errors`] and do not stop the walk; a
/// missing `session_raw/` is an empty report.
pub fn rewrite_legacy_transcripts(
    workspace_dir: &Path,
    mut rewrite: impl FnMut(&mut SessionTranscript) -> bool,
) -> LegacyRewriteReport {
    let mut report = LegacyRewriteReport::default();
    for path in jsonl_files(&raw_dir(workspace_dir)) {
        report.scanned += 1;
        let mut session = match read_transcript(&path) {
            Ok(session) => session,
            Err(error) => {
                report.errors += 1;
                tracing::warn!("{LOG_PREFIX} unreadable path={}: {error:#}", path.display());
                continue;
            }
        };
        if !rewrite(&mut session) {
            report.unchanged += 1;
            continue;
        }
        match write_transcript(&path, &session.messages, &session.meta, None) {
            Ok(()) => {
                report.rewritten += 1;
                tracing::debug!("{LOG_PREFIX} rewrote path={}", path.display());
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(
                    "{LOG_PREFIX} write failed path={}: {error:#}",
                    path.display()
                );
            }
        }
    }
    tracing::debug!(
        "{LOG_PREFIX} scanned={} rewritten={} unchanged={} errors={}",
        report.scanned,
        report.rewritten,
        report.unchanged,
        report.errors
    );
    report
}

/// Every `*.jsonl` directly under `raw` or one level down, sorted.
fn jsonl_files(raw: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(raw) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Ok(inner) = std::fs::read_dir(&path) {
                found.extend(
                    inner
                        .flatten()
                        .map(|entry| entry.path())
                        .filter(|p| is_jsonl(p)),
                );
            }
        } else if is_jsonl(&path) {
            found.push(path);
        }
    }
    found.sort();
    found
}

fn is_jsonl(path: &Path) -> bool {
    path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
}

#[cfg(test)]
#[path = "rewrite_tests.rs"]
mod tests;
