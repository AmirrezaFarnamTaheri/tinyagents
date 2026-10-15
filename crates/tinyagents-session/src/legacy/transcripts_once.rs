//! The one-time form of the classic JSONL transcript import.
//!
//! [`import_transcripts`] is already read-only, id-preserving and
//! only-if-absent, so running it again is harmless but rescans and re-parses
//! every file in `session_raw/`. A host that imports on every launch records a
//! marker document beside the session collections instead: once a pass has
//! read every file, later launches skip the scan and never touch the legacy
//! directory again. The legacy files themselves are never modified, renamed or
//! deleted.

use std::path::Path;

use serde_json::{Value, json};
use tinystoragedrivers_core::{CollectionSpec, DocumentStore, ErrorKind, Precondition};

use super::transcripts::{TranscriptImportReport, import_transcripts};
use crate::port::DriverSessionStores;

/// Collection holding the import markers.
pub const MARKER_COLLECTION: &str = "session_import_markers";
/// Id of the transcript-import marker document.
pub const TRANSCRIPT_MARKER_ID: &str = "session_raw_jsonl_v1";

const LOG_PREFIX: &str = "[session_db:import:transcripts:once]";

/// How a one-time import ended.
#[derive(Debug, Clone, PartialEq)]
pub enum OnceOutcome {
    /// A marker from an earlier launch says the import already ran.
    AlreadyImported,
    /// This pass ran. `marked` is false when some file did not parse, so the
    /// next launch tries again instead of treating the import as complete.
    Imported {
        /// What the pass did.
        report: TranscriptImportReport,
        /// Whether the completion marker was written.
        marked: bool,
    },
}

/// Imports `<workspace_dir>/session_raw/` into `agent_id`'s transcripts once.
///
/// A marker document in the agent's scope records completion. A provider
/// without schema authority (`ValidateOnly`) cannot declare the marker
/// collection, so it runs the import every time (still idempotent).
///
/// # Errors
///
/// The destination refuses a read or write, or the raw directory cannot be
/// listed. No marker is written then, so a later call resumes the work.
pub fn import_transcripts_once(
    provider: &DriverSessionStores,
    agent_id: &str,
    workspace_dir: &Path,
) -> anyhow::Result<OnceOutcome> {
    let stores = provider.try_for_agent(agent_id)?;
    let (docs, may_declare) = provider.documents_for(agent_id)?;
    if may_declare {
        let docs = docs.clone();
        let marked = provider
            .bridge()
            .run(async move { marker_present(docs.as_ref()).await })??;
        if marked {
            tracing::debug!("{LOG_PREFIX} marker present; skipping scan agent_id={agent_id}");
            return Ok(OnceOutcome::AlreadyImported);
        }
    }
    let report = import_transcripts(workspace_dir, stores.transcripts.as_ref())?;
    let mut marked = false;
    if may_declare && report.unreadable == 0 {
        let docs = docs.clone();
        provider
            .bridge()
            .run(async move { write_marker(docs.as_ref(), &report).await })??;
        marked = true;
    }
    tracing::info!(
        "{LOG_PREFIX} agent_id={agent_id} imported={} already_present={} unreadable={} marked={marked}",
        report.imported,
        report.already_present,
        report.unreadable
    );
    Ok(OnceOutcome::Imported { report, marked })
}

async fn marker_present(docs: &dyn DocumentStore) -> Result<bool, tinystoragedrivers_core::StorageError> {
    docs.ensure_collection(&CollectionSpec::new(MARKER_COLLECTION))
        .await?;
    Ok(docs.get(MARKER_COLLECTION, TRANSCRIPT_MARKER_ID).await?.is_some())
}

async fn write_marker(
    docs: &dyn DocumentStore,
    report: &TranscriptImportReport,
) -> Result<(), tinystoragedrivers_core::StorageError> {
    let doc: Value = json!({
        "imported_at": chrono::Utc::now().to_rfc3339(),
        "read": report.read,
        "imported": report.imported,
        "already_present": report.already_present,
        "messages": report.messages,
    });
    match docs
        .put(MARKER_COLLECTION, TRANSCRIPT_MARKER_ID, doc, Precondition::Absent)
        .await
    {
        // A concurrent launch marked it first; the marker is idempotent.
        Err(error) if error.kind() == ErrorKind::Conflict => Ok(()),
        other => other.map(|_| ()),
    }
}
