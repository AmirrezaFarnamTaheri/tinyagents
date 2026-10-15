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
use crate::transcript::writer::write_transcript_with_tools;
use crate::transcript::{SessionTranscript, read_transcript};

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
/// missing `session_raw/` is an empty report. A directory or directory entry
/// that cannot be read (anything but a missing `session_raw/`) is counted in
/// `errors` too, so a report with `errors == 0` means every transcript was
/// seen. Symlinks are skipped, never followed, so the rewrite cannot reach
/// files outside `session_raw/`. A rewritten transcript keeps its latest
/// recorded tool declarations ([`SessionTranscript::tools`]).
pub fn rewrite_legacy_transcripts(
    workspace_dir: &Path,
    mut rewrite: impl FnMut(&mut SessionTranscript) -> bool,
) -> LegacyRewriteReport {
    let mut report = LegacyRewriteReport::default();
    for path in jsonl_files(&raw_dir(workspace_dir), &mut report.errors) {
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
        match write_transcript_with_tools(
            &path,
            &session.messages,
            &session.meta,
            None,
            session.tools.as_ref(),
        ) {
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
///
/// A missing `raw` is no files. Any other failure to read a directory or one
/// of its entries adds one to `errors` (and is logged) instead of silently
/// shrinking the scan. Symlinks are skipped without being followed.
fn jsonl_files(raw: &Path, errors: &mut usize) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let entries = match std::fs::read_dir(raw) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return found,
        Err(error) => {
            *errors += 1;
            tracing::warn!("{LOG_PREFIX} unreadable dir={}: {error}", raw.display());
            return found;
        }
    };
    for entry in entries {
        let Some((path, kind)) = scanned_entry(raw, entry, errors) else {
            continue;
        };
        if kind.is_dir() {
            let inner = match std::fs::read_dir(&path) {
                Ok(inner) => inner,
                Err(error) => {
                    *errors += 1;
                    tracing::warn!("{LOG_PREFIX} unreadable dir={}: {error}", path.display());
                    continue;
                }
            };
            for entry in inner {
                if let Some((path, kind)) = scanned_entry(&path, entry, errors)
                    && kind.is_file()
                    && is_jsonl(&path)
                {
                    found.push(path);
                }
            }
        } else if kind.is_file() && is_jsonl(&path) {
            found.push(path);
        }
    }
    found.sort();
    found
}

/// One directory entry with its own (not followed) file type; `None` for a
/// symlink, or for an entry that could not be read (counted in `errors`).
fn scanned_entry(
    dir: &Path,
    entry: std::io::Result<std::fs::DirEntry>,
    errors: &mut usize,
) -> Option<(PathBuf, std::fs::FileType)> {
    // `DirEntry::file_type` does not traverse symlinks: a link reports as a
    // link, whatever it points at.
    let read = entry.and_then(|entry| Ok((entry.path(), entry.file_type()?)));
    match read {
        Ok((path, kind)) if kind.is_symlink() => {
            tracing::debug!("{LOG_PREFIX} skipping symlink path={}", path.display());
            None
        }
        Ok(found) => Some(found),
        Err(error) => {
            *errors += 1;
            tracing::warn!(
                "{LOG_PREFIX} unreadable entry in dir={}: {error}",
                dir.display()
            );
            None
        }
    }
}

fn is_jsonl(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
}

#[cfg(test)]
#[path = "rewrite_tests.rs"]
mod tests;
