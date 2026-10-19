//! Filesystem tools: read/write/edit/patch, grep/glob/list, diffs, CSV export,
//! git operations, linter and test runners, the workspace memory index, a
//! read-only workspace overview and image metadata.
//!
//! Every tool that can touch a path takes an `Arc<dyn FsGate>`: the tool
//! performs the I/O, the host's gate decides whether it is allowed. The gate
//! trait ([`FsGate`]) is the whole seam; nothing here knows what an autonomy
//! level, an approval prompt or a workspace boundary is.

// These tools moved here verbatim from a host crate; their behavior, messages
// and control flow are pinned by tests and by the fixtures in `fixtures/`, so
// the purely stylistic pedantic lints below are allowed rather than reshaping
// working code (long `execute` bodies, `usize as f64` size labels, and so on).
#![allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::similar_names,
    clippy::used_underscore_binding,
    clippy::manual_let_else,
    clippy::format_push_string,
    clippy::unwrap_used,
    clippy::unused_self,
    clippy::unnecessary_debug_formatting
)]

mod apply_patch;
mod csv_export;
mod edit_file;
mod file_read;
mod file_sink;
mod file_write;
mod gate;
mod git_operations;
mod glob_search;
mod grep;
mod image_info;
mod list_files;
mod read_diff;
mod read_only;
mod run_linter;
mod run_tests;
mod text;
mod update_memory_md;
mod workspace_state;

#[cfg(test)]
#[path = "gate_tests.rs"]
mod gate_tests;
#[cfg(test)]
mod test_support;

pub use apply_patch::ApplyPatchTool;
pub use csv_export::CsvExportTool;
pub use edit_file::EditFileTool;
pub use file_read::FileReadTool;
pub use file_write::FileWriteTool;
pub use gate::FsGate;
pub use git_operations::{GitOperationsTool, shell_git_env};
pub use glob_search::GlobTool;
pub use grep::GrepTool;
pub use image_info::ImageInfoTool;
pub use list_files::ListFilesTool;
pub use read_diff::ReadDiffTool;
pub use read_only::{is_read_only_call, is_read_only_command};
pub use run_linter::RunLinterTool;
pub use run_tests::RunTestsTool;
pub use update_memory_md::UpdateMemoryMdTool;
pub use workspace_state::WorkspaceStateTool;

#[cfg(test)]
#[path = "contract_tests.rs"]
mod contract_tests;
