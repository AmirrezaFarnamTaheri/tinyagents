//! Result types of the legacy imports.

use super::transcripts::TranscriptImportReport;

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
