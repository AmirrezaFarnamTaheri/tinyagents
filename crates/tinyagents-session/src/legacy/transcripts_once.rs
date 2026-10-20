//! The one-time form of the classic JSONL transcript import.
//!
//! [`import_transcripts`] is already read-only, id-preserving and
//! only-if-absent, so running it again is harmless but rescans and re-parses
//! every file in `session_raw/`. A host that imports on every launch records
//! the completed pass in `tsd.migrations` instead (owner [`MIGRATION_OWNER`],
//! step [`TRANSCRIPT_IMPORT_STEP`]): once a pass has read every file, later
//! launches skip the scan and never touch the legacy directory again. The
//! legacy files themselves are never modified, renamed or deleted.
//!
//! Before v2 the marker was a document in `session_import_markers`;
//! [`import_legacy_marker`] carries it over, and
//! [`import_transcripts_once`] does so on its own first launch after the move.

use std::path::Path;

use tinystoragedrivers_types::{
    CollectionSpec, MIGRATIONS_COLLECTION, Migrations, Precondition, ScopedStorage, StorageError,
};

use super::transcripts::import_transcripts;
use super::types::OnceOutcome;
use crate::port::DriverSessionStores;

/// The `tsd.migrations` owner of the session crate's importers.
pub const MIGRATION_OWNER: &str = "session";
/// The `tsd.migrations` step recording a completed transcript import.
pub const TRANSCRIPT_IMPORT_STEP: &str = "session_raw_jsonl_v1";

/// The v1 marker collection, read only to carry its marker over.
const LEGACY_MARKER_COLLECTION: &str = "session_import_markers";

const LOG_PREFIX: &str = "[session_db:import:transcripts:once]";

/// Imports `<workspace_dir>/session_raw/` into `agent_id`'s transcripts once.
///
/// A `tsd.migrations` record in the agent's scope marks completion. A provider
/// without schema authority (`ValidateOnly`) cannot declare the collection, so
/// it runs the import every time (still idempotent).
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
    let (scoped, may_declare) = provider.scoped_for(agent_id)?;
    if may_declare {
        let scoped = scoped.clone();
        let marked = provider
            .bridge()
            .run(async move { marker_present(&scoped).await })??;
        if marked {
            tracing::debug!("{LOG_PREFIX} marker present; skipping scan agent_id={agent_id}");
            return Ok(OnceOutcome::AlreadyImported);
        }
    }
    let report = import_transcripts(workspace_dir, stores.transcripts.as_ref())?;
    let mut marked = false;
    if may_declare && report.unreadable == 0 {
        let scoped = scoped.clone();
        provider
            .bridge()
            .run(async move { write_marker(&scoped).await })??;
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

/// Carries a v1 marker (`session_import_markers/session_raw_jsonl_v1`) over to
/// `tsd.migrations` and removes it. Returns whether there was one. Needs
/// `tsd.migrations` declared.
///
/// # Errors
///
/// A storage error; the v1 marker is only removed after the new one is
/// written, so a retry resumes.
pub async fn import_legacy_marker(storage: &ScopedStorage) -> Result<bool, StorageError> {
    let docs = storage.documents();
    if docs
        .get(LEGACY_MARKER_COLLECTION, TRANSCRIPT_IMPORT_STEP)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    Migrations::new(storage)
        .mark_done(MIGRATION_OWNER, TRANSCRIPT_IMPORT_STEP)
        .await?;
    docs.delete(
        LEGACY_MARKER_COLLECTION,
        TRANSCRIPT_IMPORT_STEP,
        Precondition::None,
    )
    .await?;
    tracing::info!("{LOG_PREFIX} carried the v1 import marker over to tsd.migrations");
    Ok(true)
}

async fn marker_present(storage: &ScopedStorage) -> Result<bool, StorageError> {
    storage
        .documents()
        .ensure_collection(&CollectionSpec::new(MIGRATIONS_COLLECTION))
        .await?;
    if import_legacy_marker(storage).await? {
        return Ok(true);
    }
    Migrations::new(storage)
        .done(MIGRATION_OWNER, TRANSCRIPT_IMPORT_STEP)
        .await
}

async fn write_marker(storage: &ScopedStorage) -> Result<(), StorageError> {
    Migrations::new(storage)
        .mark_done(MIGRATION_OWNER, TRANSCRIPT_IMPORT_STEP)
        .await
}
