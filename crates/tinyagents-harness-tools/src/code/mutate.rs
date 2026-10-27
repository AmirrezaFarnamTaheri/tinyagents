//! Revision-checked edits and chunk rendering with receipt preflight.

use std::sync::Arc;

use anyhow::{Context, ensure};
use serde_json::{Value, json};
use tinytools::ToolResult;
use tinytools_code::{ChunkTarget, PatchOperation, PatchRequest, render_chunk, revision};

use super::{
    arguments, io,
    publish::{self, Destination},
    response,
    types::CodeTool,
};
use crate::fs::{file_state, filesystem::FsGate};

struct Change {
    destination: Destination,
    source: String,
    changed: bool,
    language: tinytools_code::Language,
}

impl CodeTool {
    pub(super) async fn patch(
        &self,
        args: arguments::Patch,
        gate: &Arc<dyn FsGate>,
    ) -> anyhow::Result<ToolResult> {
        arguments::require_revision(&args.expected_revision)?;
        self.permit_write(args.dry_run)?;
        let language = arguments::language(&args.path, args.language)?;
        let resolved = io::resolve_target(gate, &args.path).await?;
        let _group_lock = self.shared.write_lock.lock().await;
        let _path_lock = file_state::acquire_path_lock(&resolved).await;
        let snapshot = io::read(
            gate,
            &args.path,
            self.shared.options.limits.max_source_bytes,
        )
        .await?;
        ensure!(
            snapshot.guard.resolved == resolved,
            "structural destination resolves inconsistently"
        );
        let result = self.shared.engine.lock().patch(
            &resolved.to_string_lossy(),
            language,
            &snapshot.source,
            &PatchRequest {
                expected_revision: args.expected_revision,
                selector: args.selector,
                operation: args.operation,
                code: args.code,
            },
        )?;
        let change = Change {
            destination: Destination {
                original: args.path,
                resolved,
                before: Some(snapshot.guard),
            },
            source: result.source,
            changed: result.changed,
            language,
        };
        let receipt = self.receipt(
            &change,
            args.dry_run,
            args.start_line,
            args.line_count,
            None,
        )?;
        if change.changed && !args.dry_run {
            self.publish(gate, &change.destination, &[], &change.source)
                .await?;
        }
        Ok(receipt)
    }

    pub(super) async fn apply(
        &self,
        args: arguments::Apply,
        gate: &Arc<dyn FsGate>,
    ) -> anyhow::Result<ToolResult> {
        arguments::require_revision(&args.chunk_revision)?;
        ensure!(
            args.create_new != args.expected_revision.is_some(),
            "provide expected_revision for edits, or create_new: true without expected_revision"
        );
        if let Some(expected) = &args.expected_revision {
            arguments::require_revision(expected)?;
        }
        self.permit_write(args.dry_run)?;
        let chunk = self
            .load_chunk(gate, args.id.as_deref(), args.manifest_path.as_deref())
            .await?;
        ensure!(
            chunk.revision == args.chunk_revision,
            "chunk revision mismatch; inspect the current manifest and template"
        );
        ensure!(
            !args.create_new || chunk.manifest.target == ChunkTarget::File,
            "create_new requires a complete-file chunk"
        );
        let rendered = render_chunk(
            &mut self.shared.engine.lock(),
            &chunk.manifest,
            &chunk.template,
            &args.bindings,
        )?;
        let resolved = io::resolve_target(gate, &args.path).await?;
        let _group_lock = self.shared.write_lock.lock().await;
        let _path_lock = file_state::acquire_path_lock(&resolved).await;
        let snapshot = self.destination_snapshot(gate, &args, &resolved).await?;
        let change = self.chunk_change(&args, resolved, snapshot, &rendered)?;
        let extra = json!({
            "chunk_id": rendered.id,
            "chunk_revision": chunk.revision,
            "rendered_revision": rendered.revision,
            "dependencies": rendered.dependencies
        });
        let receipt = self.receipt(
            &change,
            args.dry_run,
            args.start_line,
            args.line_count,
            Some(extra),
        )?;
        if change.changed && !args.dry_run {
            self.publish(gate, &change.destination, &chunk.guards, &change.source)
                .await?;
        }
        Ok(receipt)
    }

    async fn destination_snapshot(
        &self,
        gate: &Arc<dyn FsGate>,
        args: &arguments::Apply,
        resolved: &std::path::Path,
    ) -> anyhow::Result<Option<io::Snapshot>> {
        if args.create_new {
            publish::require_absent(resolved).await?;
            return Ok(None);
        }
        let snapshot = io::read(
            gate,
            &args.path,
            self.shared.options.limits.max_source_bytes,
        )
        .await?;
        ensure!(
            snapshot.guard.resolved == resolved,
            "structural destination resolves inconsistently"
        );
        ensure!(
            Some(&snapshot.guard.revision) == args.expected_revision.as_ref(),
            "source revision mismatch; inspect the current destination"
        );
        Ok(Some(snapshot))
    }

    fn chunk_change(
        &self,
        args: &arguments::Apply,
        resolved: std::path::PathBuf,
        snapshot: Option<io::Snapshot>,
        rendered: &tinytools_code::RenderedChunk,
    ) -> anyhow::Result<Change> {
        let (source, changed) = if rendered.target == ChunkTarget::File {
            ensure!(
                args.selector.is_none(),
                "a complete-file chunk does not accept a selector"
            );
            if let Some(before) = &snapshot {
                self.shared
                    .engine
                    .lock()
                    .validate(rendered.language, &before.source)?;
            }
            (
                rendered.source.clone(),
                snapshot
                    .as_ref()
                    .is_none_or(|before| before.source != rendered.source),
            )
        } else {
            let before = snapshot
                .as_ref()
                .context("container chunks require an existing destination")?;
            let operation = if rendered.target == ChunkTarget::FunctionBody {
                PatchOperation::ReplaceBody
            } else {
                PatchOperation::AppendMember
            };
            let patched = self.shared.engine.lock().patch(
                &resolved.to_string_lossy(),
                rendered.language,
                &before.source,
                &PatchRequest {
                    expected_revision: before.guard.revision.clone(),
                    selector: args.selector.clone(),
                    operation,
                    code: rendered.source.clone(),
                },
            )?;
            (patched.source, patched.changed)
        };
        Ok(Change {
            destination: Destination {
                original: args.path.clone(),
                resolved,
                before: snapshot.map(|before| before.guard),
            },
            source,
            changed,
            language: rendered.language,
        })
    }

    fn receipt(
        &self,
        change: &Change,
        dry_run: bool,
        start_line: usize,
        line_count: Option<usize>,
        extra: Option<Value>,
    ) -> anyhow::Result<ToolResult> {
        let options = self.shared.options;
        let lines = arguments::page_size(line_count, options.max_page_lines)?;
        let mut value = json!({
            "path": change.destination.original,
            "language": change.language,
            "before_revision": change.destination.before.as_ref().map(|before| &before.revision),
            "revision": revision(&change.source),
            "changed": change.changed,
            "written": change.changed && !dry_run,
            "dry_run": dry_run,
            "source": response::source_page(&change.source, start_line, lines)?
        });
        if let (Some(object), Some(Value::Object(extra))) = (value.as_object_mut(), extra) {
            object.extend(extra);
        }
        response::bounded_json(value, options.max_output_bytes)
    }
}
