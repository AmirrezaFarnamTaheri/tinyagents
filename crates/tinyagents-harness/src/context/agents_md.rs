//! Bounded loading of host-selected `AGENTS.md` instruction layers.
//!
//! This module owns safe file opening, size limits, UTF-8 handling, and the
//! workspace/project layer convention. The host supplies the path validation
//! callback so filesystem policy stays with the host.

use std::io::{Read, Take};
use std::path::Path;

/// Instruction file name used by both context layers.
pub const AGENTS_MD_FILENAME: &str = "AGENTS.md";
/// Maximum bytes read from one instruction file.
pub const MAX_AGENTS_MD_READ_BYTES: u64 = 81_024;

/// Loaded workspace-global and action-directory instruction content.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentsMdContent {
    /// `<workspace_dir>/AGENTS.md`, when present and readable.
    pub global: Option<String>,
    /// `<action_dir>/AGENTS.md`, unless it duplicates the global directory.
    pub local: Option<String>,
}

impl AgentsMdContent {
    /// Whether either layer contains non-whitespace text.
    pub fn is_empty(&self) -> bool {
        self.global.is_none() && self.local.is_none()
    }
}

/// Loads one `AGENTS.md` from `dir` if the host's path check accepts it.
///
/// Missing, unreadable, non-regular, empty, over-policy, and rejected files
/// are skipped without surfacing their content in errors or logs. The
/// validator receives `(candidate_file, allowed_root)`.
pub fn load_agents_md(dir: &Path, validate_path: impl Fn(&Path, &Path) -> bool) -> Option<String> {
    let path = dir.join(AGENTS_MD_FILENAME);
    let file = open_no_follow(&path).ok()?;
    if !file.metadata().ok()?.is_file() || !validate_path(&path, dir) {
        return None;
    }
    let mut bytes = Vec::new();
    if Take::take(file, MAX_AGENTS_MD_READ_BYTES)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return None;
    }
    let value = String::from_utf8_lossy(&bytes);
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

/// Loads global and local instructions once, respecting the existing enable
/// gate. If both paths resolve to the same directory, content is loaded once.
pub fn load_agents_md_layers(
    workspace_dir: &Path,
    action_dir: &Path,
    enabled: bool,
    validate_path: impl Fn(&Path, &Path) -> bool + Copy,
) -> AgentsMdContent {
    if !enabled {
        return AgentsMdContent::default();
    }
    let global = load_agents_md(workspace_dir, validate_path);
    let local = if paths_equal(workspace_dir, action_dir) {
        None
    } else {
        load_agents_md(action_dir, validate_path)
    };
    AgentsMdContent { global, local }
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

#[cfg(unix)]
fn open_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
fn open_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "symlink refused",
        ));
    }
    std::fs::File::open(path)
}

#[cfg(test)]
#[path = "agents_md_tests.rs"]
mod tests;
