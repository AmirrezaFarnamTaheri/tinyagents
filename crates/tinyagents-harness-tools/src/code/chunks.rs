//! Content-bound chunk snapshots, including gated custom templates.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, ensure};
use tinytools_code::{ChunkManifest, revision};

use super::{io, types::CodeTool};
use crate::fs::filesystem::FsGate;

pub(super) struct LoadedChunk {
    pub(super) manifest: ChunkManifest,
    pub(super) template: String,
    pub(super) revision: String,
    pub(super) guards: Vec<io::ReadGuard>,
}

impl CodeTool {
    pub(super) async fn load_chunk(
        &self,
        gate: &Arc<dyn FsGate>,
        id: Option<&str>,
        manifest_path: Option<&str>,
    ) -> anyhow::Result<LoadedChunk> {
        ensure!(
            id.is_some() != manifest_path.is_some(),
            "provide exactly one of id or manifest_path"
        );
        if let Some(id) = id {
            let manifest = self.shared.catalog.manifest(id)?.clone();
            let template = self
                .shared
                .catalog
                .builtin_source(id)
                .context("bundled chunk has no template")?
                .to_owned();
            let encoded = serde_json::to_string(&manifest)?;
            return Ok(LoadedChunk {
                manifest,
                revision: chunk_revision(&encoded, &template),
                template,
                guards: Vec::new(),
            });
        }
        let original = manifest_path.context("missing custom manifest path")?;
        let options = self.shared.options;
        let snapshot = io::read(gate, original, options.max_manifest_bytes).await?;
        let manifest: ChunkManifest =
            serde_json::from_str(&snapshot.source).context("invalid custom chunk manifest")?;
        manifest.validate()?;
        let parent = snapshot
            .guard
            .resolved
            .parent()
            .context("manifest has no parent directory")?;
        let template_path = relative_template(parent, &manifest.template_path)?;
        let template = io::read(
            gate,
            template_path
                .to_str()
                .context("template path must be UTF-8")?,
            options.max_template_bytes,
        )
        .await?;
        ensure!(
            template.guard.resolved.starts_with(parent),
            "custom template must remain within the manifest directory"
        );
        Ok(LoadedChunk {
            manifest,
            revision: chunk_revision(&snapshot.source, &template.source),
            template: template.source,
            guards: vec![snapshot.guard, template.guard],
        })
    }
}

fn relative_template(parent: &Path, value: &str) -> anyhow::Result<std::path::PathBuf> {
    ensure!(
        !value.is_empty()
            && !value.contains(['\\', ':'])
            && !value
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."),
        "custom template path must be portable and relative to the manifest"
    );
    Ok(parent.join(value))
}

fn chunk_revision(manifest: &str, template: &str) -> String {
    // Hash each bounded component before combining so no delimiter ambiguity
    // or accidental cross-component concatenation can preserve the identity.
    revision(&format!("{}:{}", revision(manifest), revision(template)))
}
