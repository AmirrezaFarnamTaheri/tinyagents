//! Key hardening for the artifact-offload convention.
//!
//! Everything an agent offloads is a blob keyed `<outputs|workspace>/<name>`.
//! The resolver is deliberately fail-closed: it rejects absolute names, `..`
//! traversal and control characters, and — when the host supplies an
//! [`ArtifactPathPolicy`] — anything reaching the host's internal state.
//! There is no filesystem underneath, so there are no symlinks to escape
//! through; the key is the whole address.

use std::path::Path;

use super::policy::ArtifactPathPolicy;
use super::types::{ArtifactKind, OffloadError};

/// Maximum characters kept from a single path component when deriving a name
/// from an agent id / task id. Keeps generated names well inside filesystem
/// limits on every supported platform.
const MAX_COMPONENT_CHARS: usize = 80;

/// Reduce an arbitrary string to a safe single path component.
///
/// Anything that is not ASCII alphanumeric, `-`, or `_` becomes `_`, so a task
/// id like `sub-1a/2b` — or an agent id carrying a path separator — can never
/// introduce a directory level of its own.
pub fn sanitize_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len().min(MAX_COMPONENT_CHARS));
    for ch in value.chars().take(MAX_COMPONENT_CHARS) {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

/// Resolve `relative` into the blob key `<kind.subdir()>/<name>`, or explain
/// why it is refused.
///
/// `.` and empty segments are dropped. When `policy` is supplied the key is
/// additionally checked against the host's internal state:
///
/// * anything [`ArtifactPathPolicy::is_internal_state`] flags is refused with
///   [`OffloadError::InternalState`];
/// * anything under [`ArtifactPathPolicy::internal_root`] is refused with
///   [`OffloadError::InternalRoot`].
///
/// The specific check runs **first** so its more precise error survives.
///
/// Passing `policy: None` skips only those two checks; traversal and
/// containment always run.
pub fn resolve_artifact_key(
    policy: Option<&dyn ArtifactPathPolicy>,
    kind: ArtifactKind,
    relative: &str,
) -> Result<String, OffloadError> {
    let trimmed = relative.trim();
    if trimmed.is_empty() {
        return Err(OffloadError::EmptyName);
    }
    if trimmed.starts_with(['/', '\\']) || has_drive_prefix(trimmed) {
        return Err(OffloadError::AbsolutePath {
            path: trimmed.to_string(),
        });
    }
    let mut segments: Vec<&str> = Vec::new();
    for segment in trimmed.split(['/', '\\']) {
        match segment {
            "" | "." => {}
            ".." => {
                return Err(OffloadError::PathEscape {
                    root: kind.subdir().to_string(),
                    path: trimmed.to_string(),
                });
            }
            other if other.chars().any(char::is_control) => {
                return Err(OffloadError::PathEscape {
                    root: kind.subdir().to_string(),
                    path: trimmed.escape_debug().to_string(),
                });
            }
            other => segments.push(other),
        }
    }
    if segments.is_empty() {
        return Err(OffloadError::EmptyName);
    }
    let key = format!("{}/{}", kind.subdir(), segments.join("/"));

    if let Some(policy) = policy {
        let as_path = Path::new(&key);
        if policy.is_internal_state(as_path) {
            return Err(OffloadError::InternalState { path: key });
        }
        if policy
            .internal_root()
            .is_some_and(|internal_root| as_path.starts_with(internal_root))
        {
            return Err(OffloadError::InternalRoot { path: key });
        }
    }

    Ok(key)
}

/// `C:` style prefix, refused so a Windows-absolute name is never a key.
fn has_drive_prefix(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(
        (chars.next(), chars.next()),
        (Some(first), Some(':')) if first.is_ascii_alphabetic()
    )
}
