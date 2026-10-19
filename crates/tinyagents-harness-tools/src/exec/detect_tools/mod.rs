//! Tool: `detect_tools` — report which developer toolchains are installed on PATH.
//!
//! Lets the agent ground its plans in what the host actually has rather than
//! assuming. Read-only: it only scans `$PATH` for executables (no subprocesses,
//! no writes), so it is safe in every access mode.

use async_trait::async_trait;
use serde_json::json;
use std::path::PathBuf;
use tinytools::{PermissionLevel, Tool, ToolResult};

/// Common developer tools probed when the caller doesn't specify a list.
const DEFAULT_CANDIDATES: &[&str] = &[
    "node", "npm", "npx", "pnpm", "yarn", "bun", "deno", "python3", "python", "pip3", "pip", "uv",
    "pipx", "cargo", "rustc", "go", "gcc", "cc", "clang", "make", "git", "gh", "docker", "podman",
    "kubectl", "rg", "jq", "fd", "curl", "wget",
];

/// Read-only tool reporting which developer toolchains are on `PATH`.
#[derive(Debug)]
pub struct DetectToolsTool;

impl DetectToolsTool {
    #[must_use]
    /// Create the tool.
    pub fn new() -> Self {
        Self
    }
}

impl Default for DetectToolsTool {
    fn default() -> Self {
        Self::new()
    }
}

/// Fallback executable extensions when Windows has no `PATHEXT` set.
const DEFAULT_PATHEXT: &str = ".EXE;.CMD;.BAT";

/// Locate `name` on `$PATH`, honoring `PATHEXT` on Windows. Returns the first
/// matching executable path, or `None` if not found.
#[must_use]
pub fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let pathext = cfg!(windows)
        .then(|| std::env::var("PATHEXT").unwrap_or_else(|_| DEFAULT_PATHEXT.to_string()));
    let file_names = candidate_file_names(name, pathext.as_deref());
    for dir in std::env::split_paths(&path) {
        for file_name in &file_names {
            let candidate = dir.join(file_name);
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Whether `path` names a regular file the current process can execute.
fn is_executable_file(path: &std::path::Path) -> bool {
    if !path.is_file() {
        return false;
    }
    // On Unix, checking mode bits alone ignores the current process's
    // effective credentials and supplementary groups.
    #[cfg(unix)]
    {
        rustix::fs::accessat(
            rustix::fs::CWD,
            path.as_os_str().as_encoded_bytes(),
            rustix::fs::Access::EXEC_OK,
            rustix::fs::AtFlags::EACCESS,
        )
        .is_ok()
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// The file names to probe in each `PATH` directory for `name`.
///
/// `pathext` is the Windows `PATHEXT` list (`;`-separated), or `None` on
/// platforms that run a bare file name. A name that already ends in one of
/// those extensions (compared case-insensitively, as Windows does) is probed
/// unchanged; any other name is probed once per extension.
fn candidate_file_names(name: &str, pathext: Option<&str>) -> Vec<String> {
    let extensions: Vec<&str> = pathext
        .into_iter()
        .flat_map(|list| list.split(';'))
        .filter(|ext| !ext.is_empty())
        .collect();
    let lower_name = name.to_ascii_lowercase();
    let has_extension = extensions
        .iter()
        .any(|ext| lower_name.ends_with(&ext.to_ascii_lowercase()));
    if extensions.is_empty() || has_extension {
        return vec![name.to_string()];
    }
    extensions
        .iter()
        .map(|ext| format!("{name}{ext}"))
        .collect()
}

#[async_trait]
impl Tool for DetectToolsTool {
    fn name(&self) -> &'static str {
        "detect_tools"
    }

    fn description(&self) -> &'static str {
        "Detect which developer tools / language runtimes are installed on the host PATH \
         (e.g. node, python3, cargo, docker, git, rg). Use this before assuming a tool \
         exists or before proposing to install one. Read-only — scans PATH only."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "tools": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional list of tool names to probe. If omitted, a default \
                                    catalog of common developer tools is probed."
                }
            }
        })
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let requested: Vec<String> = args
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let candidates: Vec<String> = if requested.is_empty() {
            DEFAULT_CANDIDATES
                .iter()
                .map(|s| (*s).to_string())
                .collect()
        } else {
            requested
        };

        let mut available = Vec::new();
        let mut missing = Vec::new();
        for name in &candidates {
            match find_on_path(name) {
                Some(p) => available.push(json!({
                    "name": name,
                    "path": p.to_string_lossy(),
                })),
                None => missing.push(name.clone()),
            }
        }

        tracing::debug!(
            probed = candidates.len(),
            available = available.len(),
            "[detect_tools] PATH scan complete"
        );
        let payload = json!({
            "available": available,
            "missing": missing,
            "probed": candidates.len(),
        });
        Ok(ToolResult::success(serde_json::to_string_pretty(&payload)?))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;
