//! Bounded regular-file reads and reauthorization of retained source guards.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow, ensure};
use tinytools_code::revision;
use tokio::io::AsyncReadExt;

use crate::fs::filesystem::FsGate;

#[derive(Debug, Clone)]
pub(super) struct ReadGuard {
    pub(super) original: String,
    pub(super) resolved: PathBuf,
    pub(super) revision: String,
    pub(super) maximum: usize,
}

pub(super) struct Snapshot {
    pub(super) guard: ReadGuard,
    pub(super) source: String,
}

pub(super) async fn read(
    gate: &Arc<dyn FsGate>,
    original: &str,
    maximum: usize,
) -> anyhow::Result<Snapshot> {
    let resolved = gate
        .validate_path(original)
        .await
        .map_err(|message| anyhow!(message))?;
    let source = read_bounded(&resolved, maximum).await?;
    Ok(Snapshot {
        guard: ReadGuard {
            original: original.into(),
            resolved,
            revision: revision(&source),
            maximum,
        },
        source,
    })
}

pub(super) async fn read_bounded(path: &Path, maximum: usize) -> anyhow::Result<String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .context("could not inspect structural input")?;
    ensure!(
        metadata.is_file(),
        "structural input must be a regular file"
    );
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    // The gate already resolved the path. Refuse a later symlink swap, and
    // never block on a FIFO substituted between the metadata and open calls.
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .await
        .context("could not open structural input")?;
    let metadata = file
        .metadata()
        .await
        .context("could not inspect structural input")?;
    ensure!(
        metadata.is_file(),
        "structural input must be a regular file"
    );
    let bound = u64::try_from(maximum)?.saturating_add(1);
    let mut bytes = Vec::new();
    file.take(bound)
        .read_to_end(&mut bytes)
        .await
        .context("could not read structural input")?;
    ensure!(
        bytes.len() <= maximum,
        "structural input exceeds the {maximum}-byte read budget"
    );
    String::from_utf8(bytes).context("structural input must be valid UTF-8")
}

pub(super) async fn verify(gate: &Arc<dyn FsGate>, guard: &ReadGuard) -> anyhow::Result<()> {
    let current = read(gate, &guard.original, guard.maximum).await?;
    ensure!(
        current.guard.resolved == guard.resolved,
        "structural input path changed during the call; inspect it again"
    );
    ensure!(
        current.guard.revision == guard.revision,
        "structural input revision changed during the call; inspect it again"
    );
    Ok(())
}

pub(super) async fn resolve_target(
    gate: &Arc<dyn FsGate>,
    original: &str,
) -> anyhow::Result<PathBuf> {
    let path = gate
        .validate_parent_path(original)
        .await
        .map_err(|message| anyhow!(message))?;
    reject_symlink(&path).await?;
    Ok(path)
}

pub(super) async fn reject_symlink(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink(),
                "refusing to replace a symlink"
            );
            ensure!(
                metadata.is_file(),
                "structural destination must be a regular file"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("could not inspect structural destination"),
    }
    Ok(())
}
