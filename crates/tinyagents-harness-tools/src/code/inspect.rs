//! Bounded structural and catalog inspection with explicit continuation fields.

use std::sync::Arc;

use anyhow::{Context, ensure};
use serde_json::json;
use tinytools::ToolResult;
use tinytools_code::select_symbol;

use super::{arguments, io, response, types::CodeTool};
use crate::fs::filesystem::FsGate;

impl CodeTool {
    pub(super) async fn outline(
        &self,
        args: arguments::Outline,
        gate: &Arc<dyn FsGate>,
    ) -> anyhow::Result<ToolResult> {
        let options = self.shared.options;
        ensure!(args.level <= 1, "outline level must be 0 or 1");
        ensure!(args.start_line > 0, "start_line must be at least 1");
        let limit = arguments::page_size(args.limit, options.max_page_items)?;
        let lines = arguments::page_size(args.line_count, options.max_page_lines)?;
        let language = arguments::language(&args.path, args.language)?;
        let snapshot = io::read(gate, &args.path, options.limits.max_source_bytes).await?;
        let report = self.shared.engine.lock().analyze(
            &snapshot.guard.resolved.to_string_lossy(),
            language,
            &snapshot.source,
        )?;
        let mut result = json!({
            "path": args.path,
            "language": language,
            "revision": report.revision,
            "symbols": response::records(&report.symbols, args.offset, limit),
            "references": response::records(&report.references, args.offset, limit),
            "diagnostics": response::records(&report.diagnostics, args.offset, limit),
            "symbols_truncated": report.symbols_truncated,
            "references_truncated": report.references_truncated,
            "diagnostics_truncated": report.diagnostics_truncated,
            "cache_status": report.cache_status
        });
        if args.level == 1 {
            result["skeleton"] = response::source_page(&report.skeleton, args.start_line, lines)?;
        }
        response::bounded_json(result, options.max_output_bytes)
    }

    pub(super) async fn symbol(
        &self,
        args: arguments::Symbol,
        gate: &Arc<dyn FsGate>,
    ) -> anyhow::Result<ToolResult> {
        let options = self.shared.options;
        let lines = arguments::page_size(args.line_count, options.max_page_lines)?;
        let language = arguments::language(&args.path, args.language)?;
        let snapshot = io::read(gate, &args.path, options.limits.max_source_bytes).await?;
        let report = self.shared.engine.lock().analyze(
            &snapshot.guard.resolved.to_string_lossy(),
            language,
            &snapshot.source,
        )?;
        let symbol = select_symbol(&report, &args.selector)?;
        let source = snapshot
            .source
            .get(symbol.range.start_byte..symbol.range.end_byte)
            .context("symbol range is outside its source snapshot")?;
        response::bounded_json(
            json!({
                "path": args.path,
                "language": language,
                "revision": report.revision,
                "symbol": symbol,
                "source": response::source_page(source, args.start_line, lines)?
            }),
            options.max_output_bytes,
        )
    }

    pub(super) fn search(&self, args: &arguments::Search) -> anyhow::Result<ToolResult> {
        let options = self.shared.options;
        let limit = arguments::page_size(
            Some(args.limit.unwrap_or(options.max_page_items.min(5))),
            options.max_page_items,
        )?;
        ensure!(
            args.query.len() <= 1024,
            "chunk search query exceeds 1024 bytes"
        );
        // The built-in catalog contains twelve entries; fifty is the pure
        // search cap, so the adapter can page every bundled match exactly.
        let matches = self.shared.catalog.search(&args.query, args.language, 50);
        response::bounded_json(
            json!({
                "query": args.query,
                "chunks": response::records(&matches, args.offset, limit)
            }),
            options.max_output_bytes,
        )
    }

    pub(super) async fn inspect(
        &self,
        args: arguments::Inspect,
        gate: &Arc<dyn FsGate>,
    ) -> anyhow::Result<ToolResult> {
        let options = self.shared.options;
        let limit = arguments::page_size(args.limit, options.max_page_items)?;
        let chunk = self
            .load_chunk(gate, args.id.as_deref(), args.manifest_path.as_deref())
            .await?;
        let manifest = &chunk.manifest;
        response::bounded_json(
            json!({
                "id": manifest.id,
                "summary": manifest.summary,
                "language": manifest.language,
                "target": manifest.target,
                "chunk_revision": chunk.revision,
                "manifest_revision": manifest.revision()?,
                "template_revision": tinytools_code::revision(&chunk.template),
                "template_path": manifest.template_path,
                "parameters": response::records(&manifest.parameters, args.offset, limit),
                "slots": response::records(&manifest.slots, args.offset, limit),
                "tags": response::records(&manifest.tags, args.offset, limit),
                "dependencies": response::records(&manifest.dependencies, args.offset, limit),
                "forbidden_nodes": response::records(&manifest.forbidden_nodes, args.offset, limit)
            }),
            options.max_output_bytes,
        )
    }
}
