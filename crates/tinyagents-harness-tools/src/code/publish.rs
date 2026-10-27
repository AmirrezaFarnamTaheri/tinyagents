//! Same-directory staging and atomic publication after final source checks.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, ensure};
use tempfile::NamedTempFile;

use super::{io, types::CodeTool};
use crate::fs::{file_state, filesystem::FsGate};

pub(super) struct Destination {
    pub(super) original: String,
    pub(super) resolved: PathBuf,
    pub(super) before: Option<io::ReadGuard>,
}

impl CodeTool {
    pub(super) fn permit_write(&self, preview: bool) -> anyhow::Result<()> {
        if !preview {
            self.require_write_authority()?;
            ensure!(
                !self.gate.is_rate_limited(),
                "structural action budget exhausted"
            );
        }
        Ok(())
    }

    fn require_write_authority(&self) -> anyhow::Result<()> {
        ensure!(
            self.gate.can_act() && !self.gate.is_read_only(),
            "[policy-blocked] structural writes are read-only under the host policy"
        );
        Ok(())
    }

    pub(super) async fn publish(
        &self,
        gate: &Arc<dyn FsGate>,
        destination: &Destination,
        guards: &[io::ReadGuard],
        source: &str,
    ) -> anyhow::Result<()> {
        self.permit_write(false)?;
        ensure!(
            self.gate.record_action(),
            "structural action budget exhausted"
        );
        self.require_write_authority()?;
        let staged = stage(destination, source).await?;
        // Custom metadata and templates can change while parsing or staging.
        // Revalidate their original paths as well as their exact content.
        for guard in guards {
            io::verify(gate, guard).await?;
        }
        let resolved = io::resolve_target(gate, &destination.original).await?;
        ensure!(
            resolved == destination.resolved,
            "structural destination path changed during the call"
        );
        if let Some(before) = &destination.before {
            io::verify(gate, before).await?;
        } else {
            require_absent(&resolved).await?;
        }
        // Reauthorization can yield while the host revokes write access.
        // The action is already reserved: recheck authority, not its budget.
        self.require_write_authority()?;
        if destination.before.is_some() {
            staged
                .persist(&resolved)
                .map_err(|error| error.error)
                .context("atomic structural replacement failed")?;
        } else {
            staged
                .persist_noclobber(&resolved)
                .map_err(|error| error.error)
                .context("atomic structural creation failed")?;
        }
        if let Some(agent) = file_state::current_file_state_agent_id() {
            file_state::record_write(&agent, resolved);
        }
        Ok(())
    }
}

pub(super) async fn require_absent(path: &std::path::Path) -> anyhow::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("could not inspect creation destination"),
        Ok(_) => anyhow::bail!("create_new refuses an existing destination"),
    }
}

async fn stage(destination: &Destination, source: &str) -> anyhow::Result<NamedTempFile> {
    let parent = destination
        .resolved
        .parent()
        .context("destination has no parent")?
        .to_path_buf();
    let permissions = if destination.before.is_some() {
        Some(
            tokio::fs::metadata(&destination.resolved)
                .await
                .context("could not inspect destination permissions")?
                .permissions(),
        )
    } else {
        None
    };
    let source = source.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut file = tempfile::Builder::new()
            .prefix(".tinytools-code-")
            .tempfile_in(parent)
            .context("could not stage structural output; parent directory must exist")?;
        file.write_all(source.as_bytes())
            .context("could not stage complete structural output")?;
        if let Some(permissions) = permissions {
            file.as_file()
                .set_permissions(permissions)
                .context("could not preserve file permissions")?;
        }
        file.as_file()
            .sync_all()
            .context("could not sync staged structural output")?;
        Ok::<_, anyhow::Error>(file)
    })
    .await
    .context("structural staging task failed")?
}
