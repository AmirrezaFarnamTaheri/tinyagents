//! Generic non-recursive TOML directory loader. Host roots stay outside this crate.
use crate::{AgentDefinition, DefinitionSource, PromptSource};
use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

/// Read one custom TOML definition and stamp its source path.
/// Built-ins without prompts should be parsed directly and receive their prompt
/// separately; a custom file must supply an inline, file or verbatim prompt.
pub fn load_file<X: DeserializeOwned + Default>(path: &Path) -> Result<AgentDefinition<X>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut definition: AgentDefinition<X> = toml::from_str(&content)
        .with_context(|| format!("parsing {} as AgentDefinition TOML", path.display()))?;
    if matches!(&definition.system_prompt, PromptSource::Inline(body) if body.is_empty()) {
        bail!(
            "{}: missing `system_prompt` — custom definitions must set an inline string or a file path",
            path.display()
        );
    }
    definition.source = DefinitionSource::File(path.to_path_buf());
    Ok(definition)
}

/// Append definitions from lowercase `.toml` files, skipping malformed files,
/// non-files and unreadable entries. Failure to read the directory is an error.
pub fn load_dir<X: DeserializeOwned + Default>(
    dir: &Path,
    out: &mut Vec<AgentDefinition<X>>,
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
        match load_file(&path) {
            Ok(definition) => out.push(definition),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "failed to load custom definition; skipping")
            }
        }
    }
    Ok(())
}
