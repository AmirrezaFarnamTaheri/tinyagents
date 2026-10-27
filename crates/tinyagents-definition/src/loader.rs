//! Generic non-recursive TOML directory loader. Host roots stay outside this crate.
use crate::{AgentDefinition, DefinitionSource, PromptSource};
use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

/// Semantic checks performed after deserializing a definition.
#[derive(Debug, Clone, Copy, Default)]
pub enum DefinitionValidation {
    /// Require a nonblank prompt and all generic definition diagnostics.
    #[default]
    Diagnostics,
    /// Require an explicitly nonempty inline prompt. File and verbatim sources
    /// retain their authored contents, including empty strings. This policy
    /// supports hosts whose existing custom definitions predate diagnostics.
    NonemptyInlinePrompt,
}

/// Parse a TOML definition without filesystem access or semantic validation.
///
/// This is intended for host-owned embedded definitions: the host chooses
/// their provenance and remains responsible for any runtime-specific fields
/// or validation. Custom files should use [`load_file`] or
/// [`load_file_with_validation`].
pub fn parse_str<X: DeserializeOwned + Default>(
    content: &str,
    source: DefinitionSource,
) -> Result<AgentDefinition<X>> {
    let mut definition: AgentDefinition<X> =
        toml::from_str(content).context("parsing agent definition TOML")?;
    definition.source = source;
    Ok(definition)
}

/// Read and stamp a custom TOML definition with strict prompt and vocabulary checks.
pub fn load_file<X: DeserializeOwned + Default>(path: &Path) -> Result<AgentDefinition<X>> {
    load_file_with_validation(path, DefinitionValidation::default())
}

/// Read and stamp a custom definition using the selected validation policy.
pub fn load_file_with_validation<X: DeserializeOwned + Default>(
    path: &Path,
    validation: DefinitionValidation,
) -> Result<AgentDefinition<X>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let definition: AgentDefinition<X> =
        parse_str(&content, DefinitionSource::File(path.to_path_buf()))
            .with_context(|| format!("parsing {} as AgentDefinition TOML", path.display()))?;
    let prompt_is_blank = match &definition.system_prompt {
        PromptSource::Inline(body) | PromptSource::Verbatim(body) => body.trim().is_empty(),
        PromptSource::File { path } => path.trim().is_empty(),
        PromptSource::Dynamic(()) => true,
    };
    let rejected_prompt = match validation {
        DefinitionValidation::Diagnostics => prompt_is_blank,
        DefinitionValidation::NonemptyInlinePrompt => {
            matches!(&definition.system_prompt, PromptSource::Inline(body) if body.is_empty())
        }
    };
    if rejected_prompt {
        bail!(
            "{}: missing `system_prompt` — custom definitions must set an inline string or a file path",
            path.display()
        );
    }
    let diagnostics = definition.diagnostics();
    if matches!(validation, DefinitionValidation::Diagnostics) && !diagnostics.is_empty() {
        bail!(
            "{}: invalid definition: {}",
            path.display(),
            diagnostics
                .into_iter()
                .map(|diagnostic| diagnostic.message)
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    Ok(definition)
}

/// Append definitions from lowercase `.toml` files, skipping malformed or semantically invalid files,
/// non-files and unreadable entries. Failure to read the directory is an error.
pub fn load_dir<X: DeserializeOwned + Default>(
    dir: &Path,
    out: &mut Vec<AgentDefinition<X>>,
) -> Result<()> {
    load_dir_with_validation(dir, out, DefinitionValidation::default())
}

/// Load a directory with the same validation policy for every file.
pub fn load_dir_with_validation<X: DeserializeOwned + Default>(
    dir: &Path,
    out: &mut Vec<AgentDefinition<X>>,
    validation: DefinitionValidation,
) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("reading agents dir {}", dir.display()))?
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(%error, "failed to read definition entry; skipping");
                continue;
            }
        };
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }
        match load_file_with_validation(&path, validation) {
            Ok(definition) => out.push(definition),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "failed to load custom definition; skipping")
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "loader_policy_tests.rs"]
mod policy_tests;
