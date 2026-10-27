//! JSON receipt preflight and byte-safe model-visible paging.

use anyhow::ensure;
use serde::Serialize;
use serde_json::{Value, json};
use tinytools::ToolResult;

pub(super) fn bounded_json(value: Value, maximum: usize) -> anyhow::Result<ToolResult> {
    let result = ToolResult::json(value);
    ensure!(
        result.output().len() <= maximum,
        "structural result exceeds the {maximum}-byte output budget; reduce the requested page"
    );
    Ok(result)
}

pub(super) fn bounded_error(message: &str, maximum: usize) -> ToolResult {
    let mut end = message.len().min(maximum);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    ToolResult::error(&message[..end])
}

pub(super) fn records<T: Serialize>(items: &[T], offset: usize, limit: usize) -> Value {
    let start = offset.min(items.len());
    let end = start.saturating_add(limit).min(items.len());
    json!({
        "items": &items[start..end],
        "offset": start,
        "total": items.len(),
        "next_offset": (end < items.len()).then_some(end)
    })
}

pub(super) fn source_page(
    source: &str,
    start_line: usize,
    line_count: usize,
) -> anyhow::Result<Value> {
    ensure!(start_line > 0, "start_line must be at least 1");
    let total = source.lines().count().max(1);
    let start = start_line.saturating_sub(1).min(total);
    let end = start.saturating_add(line_count).min(total);
    let text: String = source
        .split_inclusive('\n')
        .skip(start)
        .take(end - start)
        .collect();
    Ok(json!({
        "text": text,
        "start_line": start + 1,
        "total_lines": total,
        "next_start_line": (end < total).then_some(end + 1),
        "truncated": start > 0 || end < total
    }))
}
