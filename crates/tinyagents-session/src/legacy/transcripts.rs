//! Import of classic JSONL transcripts into a [`TranscriptLocator`].
//!
//! Each `session_raw/<stem>.jsonl` (and, for workspaces older than the flat
//! layout, `session_raw/<DDMMYYYY>/<stem>.jsonl`) becomes the transcript for
//! that stem in the destination. The stem is the identity, so a session's
//! generations (`<stem>.g1`, ...) and sub-agent transcripts (`<parent>__<child>`)
//! keep the relationships their names encode.
//!
//! # Fidelity
//!
//! The file is replayed record by record in file order against the
//! destination's append-only history, not flattened to its final message set:
//! turns are re-appended with their request ids and usage, a compaction record
//! becomes a compaction (the replaced turns stay in the history, as they stayed
//! in the file), and interrupted partials are re-recorded as display-only
//! partials. The model-context replay of the imported transcript therefore
//! equals the file's, and the display projection keeps the full timeline.
//!
//! # Not imported
//!
//! Legacy Markdown sessions (`sessions/<DDMMYYYY>/*.md`) are a read-only
//! rendering the classic reader fell back to for the oldest workspaces; they
//! are counted, not converted. A transcript the destination already has is
//! left exactly as it is.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::transcript::{
    DisplayMessage, DisplayRecord, TranscriptLocator, TranscriptMessage, TranscriptPartial,
    TranscriptTurn, read_transcript, read_transcript_display,
};

const LOG_PREFIX: &str = "[session_db:import:transcripts]";

/// What a transcript import did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptImportReport {
    /// JSONL files that parsed.
    pub read: usize,
    /// Transcripts written by this pass.
    pub imported: usize,
    /// Transcripts the destination already had, left untouched.
    pub already_present: usize,
    /// Files that did not parse, skipped.
    pub unreadable: usize,
    /// Legacy Markdown sessions seen and not converted.
    pub markdown_skipped: usize,
    /// Messages replayed into the destination, across every imported transcript.
    pub messages: usize,
}

/// `<workspace_dir>/session_raw`.
pub fn raw_dir(workspace_dir: &Path) -> PathBuf {
    workspace_dir.join("session_raw")
}

/// Every `*.jsonl` under the raw directory (flat and date-grouped), by stem.
fn transcript_files(raw: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut flat = Vec::new();
    let mut dated = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(raw)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            let mut inner: Vec<_> = std::fs::read_dir(&path)?.collect::<Result<_, _>>()?;
            inner.sort_by_key(|entry| entry.file_name());
            for entry in inner {
                push_jsonl(&mut dated, entry.path());
            }
        } else {
            push_jsonl(&mut flat, path);
        }
    }
    // The flat layout wins over a date-grouped duplicate of the same stem, so a
    // stem is imported once, from the newest layout that has it.
    let mut found = flat;
    found.extend(dated);
    let mut seen = std::collections::HashSet::new();
    found.retain(|(stem, _)| seen.insert(stem.clone()));
    Ok(found)
}

fn push_jsonl(found: &mut Vec<(String, PathBuf)>, path: PathBuf) {
    if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
        return;
    }
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        found.push((stem.to_owned(), path));
    }
}

/// Markdown sessions that have no JSONL transcript of the same stem.
///
/// The classic writer renders a `.md` companion beside every transcript it
/// writes; those are views of a transcript this pass imports, not data. Only a
/// Markdown file with no JSONL source is a session the importer cannot carry.
fn count_markdown_only(workspace_dir: &Path, stems: &std::collections::HashSet<String>) -> usize {
    let sessions = workspace_dir.join("sessions");
    let Ok(days) = std::fs::read_dir(sessions) else {
        return 0;
    };
    days.flatten()
        .filter_map(|day| std::fs::read_dir(day.path()).ok())
        .flat_map(|files| files.flatten())
        .filter(|file| {
            let path = file.path();
            path.extension().and_then(|e| e.to_str()) == Some("md")
                && path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .is_some_and(|stem| !stems.contains(stem))
        })
        .count()
}

/// Replay one transcript's display records against `history`; returns the
/// number of messages replayed.
fn replay(
    history: &dyn crate::transcript::TranscriptHistory,
    meta: &crate::transcript::TranscriptMeta,
    tools: Option<&serde_json::Value>,
    records: Vec<DisplayRecord>,
) -> anyhow::Result<usize> {
    let mut logical: Vec<TranscriptMessage> = Vec::new();
    let mut replayed = 0;
    let mut group: Vec<DisplayMessage> = Vec::new();

    // Append `group` as one ordinary turn on top of `logical`.
    let flush = |logical: &mut Vec<TranscriptMessage>,
                 group: &mut Vec<DisplayMessage>,
                 replayed: &mut usize|
     -> anyhow::Result<()> {
        if group.is_empty() {
            return Ok(());
        }
        let mut next = logical.clone();
        next.extend(group.iter().map(|m| m.message.clone()));
        let usage = group.iter().rev().find_map(|m| m.turn_usage.clone());
        let request_id = group.iter().rev().find_map(|m| m.request_id.clone());
        history.append_turn(TranscriptTurn {
            prev: logical,
            next: &next,
            meta,
            turn_usage: usage.as_ref(),
            request_id: request_id.as_deref(),
            tools: None,
        })?;
        *replayed += group.len();
        *logical = next;
        group.clear();
        Ok(())
    };

    for record in records {
        match record {
            DisplayRecord::Message(message) if message.interrupted => {
                flush(&mut logical, &mut group, &mut replayed)?;
                let partial = TranscriptPartial {
                    content: message.message.content.clone(),
                    reasoning_content: message.reasoning_content.clone(),
                    iteration: message.iteration,
                };
                history.append_turn_with_partial(
                    TranscriptTurn {
                        prev: &logical,
                        next: &logical,
                        meta,
                        turn_usage: None,
                        request_id: message.request_id.as_deref(),
                        tools: None,
                    },
                    Some(&partial),
                )?;
            }
            DisplayRecord::Message(message) => {
                // A turn is a run of rows under one request id.
                if group
                    .last()
                    .is_some_and(|last| last.request_id != message.request_id)
                {
                    flush(&mut logical, &mut group, &mut replayed)?;
                }
                group.push(*message);
            }
            DisplayRecord::Compaction(marker) => {
                flush(&mut logical, &mut group, &mut replayed)?;
                let next: Vec<TranscriptMessage> = marker
                    .replacement
                    .iter()
                    .map(|m| m.message.clone())
                    .collect();
                let usage = marker
                    .replacement
                    .iter()
                    .rev()
                    .find_map(|m| m.turn_usage.clone());
                history.append_turn(TranscriptTurn {
                    prev: &logical,
                    next: &next,
                    meta,
                    turn_usage: usage.as_ref(),
                    request_id: marker.request_id.as_deref(),
                    tools: None,
                })?;
                replayed += next.len();
                logical = next;
            }
        }
    }
    flush(&mut logical, &mut group, &mut replayed)?;
    if let Some(tools) = tools {
        // The last recorded tool declarations stay in force for resume.
        history.append_turn(TranscriptTurn {
            prev: &logical,
            next: &logical,
            meta,
            turn_usage: None,
            request_id: None,
            tools: Some(tools),
        })?;
    }
    Ok(replayed)
}

/// Import every transcript under `<workspace_dir>/session_raw/`.
///
/// A workspace with no raw directory imports nothing.
///
/// # Errors
///
/// The raw directory cannot be listed, or the destination refuses a write.
pub fn import_transcripts(
    workspace_dir: &Path,
    destination: &dyn TranscriptLocator,
) -> anyhow::Result<TranscriptImportReport> {
    let mut report = TranscriptImportReport::default();
    let raw = raw_dir(workspace_dir);
    let files = if raw.is_dir() {
        transcript_files(&raw)?
    } else {
        Vec::new()
    };
    let stems: std::collections::HashSet<String> =
        files.iter().map(|(stem, _)| stem.clone()).collect();
    report.markdown_skipped = count_markdown_only(workspace_dir, &stems);
    for (stem, path) in files {
        let (display, model) = match (read_transcript_display(&path), read_transcript(&path)) {
            (Ok(display), Ok(model)) => (display, model),
            (Err(error), _) | (_, Err(error)) => {
                tracing::warn!(
                    "{LOG_PREFIX} skipping unreadable transcript {}: {error}",
                    path.display()
                );
                report.unreadable += 1;
                continue;
            }
        };
        report.read += 1;
        let history = destination.open_stem(&stem, display.meta.clone())?;
        if history.read_session()?.is_some() {
            report.already_present += 1;
            continue;
        }
        let meta = display.meta.clone();
        report.messages += replay(
            history.as_ref(),
            &meta,
            model.tools.as_ref(),
            display.records,
        )?;
        report.imported += 1;
    }
    tracing::info!(
        "{LOG_PREFIX} read={} imported={} already_present={} unreadable={} markdown_skipped={}",
        report.read,
        report.imported,
        report.already_present,
        report.unreadable,
        report.markdown_skipped
    );
    Ok(report)
}
