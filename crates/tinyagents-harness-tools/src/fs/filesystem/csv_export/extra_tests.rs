//! Refusal, size-label and failure-path tests for `csv_export`.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{AutonomyLevel, TestGate, WrapGate};
use tempfile::TempDir;

fn tool_in(dir: &TempDir) -> CsvExportTool {
    CsvExportTool::new(TestGate::supervised(dir.path().to_path_buf()))
}

fn data(v: &serde_json::Value) -> String {
    serde_json::to_string(v).unwrap()
}

#[tokio::test]
async fn read_only_autonomy_is_refused() {
    let dir = TempDir::new().unwrap();
    let gate = TestGate::with(dir.path().to_path_buf(), AutonomyLevel::ReadOnly, 100);
    let r = CsvExportTool::new(gate)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "x.csv"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("autonomy is read-only"));
}

#[tokio::test]
async fn exhausted_hourly_limit_is_refused() {
    let dir = TempDir::new().unwrap();
    let gate = TestGate::with(dir.path().to_path_buf(), AutonomyLevel::Supervised, 0);
    let r = CsvExportTool::new(gate)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "x.csv"}))
        .await
        .unwrap();
    assert!(r.output().contains("too many actions in the last hour"));
}

#[tokio::test]
async fn exhausted_action_budget_is_refused_before_writing() {
    let dir = TempDir::new().unwrap();
    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .no_budget()
        .arc();
    let r = CsvExportTool::new(gate)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "x.csv"}))
        .await
        .unwrap();
    assert!(r.output().contains("action budget exhausted"));
    assert!(!dir.path().join("exports/x.csv").exists());
}

#[tokio::test]
async fn invalid_json_is_reported() {
    let dir = TempDir::new().unwrap();
    let r = tool_in(&dir)
        .execute(json!({"data": "{not json", "filename": "x.csv"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("Failed to parse data as JSON"));
}

#[tokio::test]
async fn empty_array_is_reported() {
    let dir = TempDir::new().unwrap();
    let r = tool_in(&dir)
        .execute(json!({"data": "[]", "filename": "x.csv"}))
        .await
        .unwrap();
    assert!(r.output().contains("nothing to export"));
}

#[tokio::test]
async fn a_path_the_gate_refuses_is_reported() {
    let dir = TempDir::new().unwrap();
    let r = tool_in(&dir)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "../escape.csv"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("Path not allowed"));
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_target_is_refused() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("exports")).unwrap();
    std::fs::write(dir.path().join("real.txt"), "keep").unwrap();
    std::os::unix::fs::symlink(
        dir.path().join("real.txt"),
        dir.path().join("exports/link.csv"),
    )
    .unwrap();
    let r = tool_in(&dir)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "link.csv"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("Refusing to write through symlink"));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("real.txt")).unwrap(),
        "keep"
    );
}

#[tokio::test]
async fn a_directory_in_the_way_reports_the_write_failure() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("exports/out.csv")).unwrap();
    let r = tool_in(&dir)
        .execute(json!({"data": data(&json!([{"a": 1}])), "filename": "out.csv"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("Failed to write CSV file"));
}

#[tokio::test]
async fn size_labels_scale_to_kb_and_mb() {
    let dir = TempDir::new().unwrap();
    let kb_rows: Vec<_> = (0..200)
        .map(|i| json!({"n": format!("row-{i:04}-padding")}))
        .collect();
    let r = tool_in(&dir)
        .execute(json!({"data": data(&json!(kb_rows)), "filename": "kb.csv"}))
        .await
        .unwrap();
    assert!(r.output().contains(" KB)"), "{}", r.output());

    let big = "x".repeat(1024 * 1024 + 10);
    let r = tool_in(&dir)
        .execute(json!({"data": data(&json!([{"blob": big}])), "filename": "mb.csv"}))
        .await
        .unwrap();
    assert!(r.output().contains(" MB)"), "{}", r.output());
}

#[test]
fn cells_and_columns_cover_bools_and_non_object_rows() {
    assert_eq!(value_to_cell(&json!(true)), "true");
    assert_eq!(value_to_cell(&json!(null)), "");
    assert_eq!(value_to_cell(&json!([1, 2])), "[1,2]");
    assert_eq!(resolve_columns(&[json!(1), json!(2)], None).len(), 0);
}

#[tokio::test]
async fn execute_with_context_delegates() {
    let dir = TempDir::new().unwrap();
    let r = tool_in(&dir)
        .execute_with_context(
            json!({"data": data(&json!([{"ok": true}])), "filename": "c.csv"}),
            ToolCallOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert!(!r.is_error, "{}", r.output());
}
