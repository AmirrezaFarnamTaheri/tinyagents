//! Budget, metadata-failure, file-state and paging-marker tests for `file_read`.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{TestGate, WorkspaceContext, WrapGate};
use tempfile::TempDir;

#[test]
fn reads_are_concurrency_safe() {
    let dir = TempDir::new().unwrap();
    let tool = FileReadTool::new(TestGate::supervised(dir.path().to_path_buf()));
    assert!(tool.is_concurrency_safe(&json!({})));
}

#[tokio::test]
async fn exhausted_action_budget_is_refused() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("a.txt"), "x").unwrap();
    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .no_budget()
        .arc();
    let r = FileReadTool::new(gate)
        .execute(json!({"path": "a.txt"}))
        .await
        .unwrap();
    assert!(r.output().contains("action budget exhausted"));
}

#[tokio::test]
async fn missing_metadata_is_reported() {
    let dir = TempDir::new().unwrap();
    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .fixed_path(dir.path().join("vanished.txt"))
        .arc();
    let r = FileReadTool::new(gate)
        .execute(json!({"path": "vanished.txt"}))
        .await
        .unwrap();
    assert!(r.is_error);
    assert!(r.output().contains("Failed to read file metadata"));
}

#[tokio::test]
async fn a_read_under_an_agent_id_records_file_state() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("a.txt"), "tracked").unwrap();
    let tool = FileReadTool::new(TestGate::supervised(dir.path().to_path_buf()));
    let r = crate::file_state::with_file_state_agent_id(
        "agent-file-read-extra".to_string(),
        tool.execute(json!({"path": "a.txt"})),
    )
    .await
    .unwrap();
    assert_eq!(r.output(), "tracked");
}

#[tokio::test]
async fn execute_with_context_delegates() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("a.txt"), "ctx").unwrap();
    let tool = FileReadTool::new(TestGate::supervised(dir.path().to_path_buf()));
    let ctx = WorkspaceContext::at(dir.path());
    let r = tool
        .execute_with_context(
            json!({"path": "a.txt"}),
            ToolCallOptions::default(),
            Some(&ctx),
        )
        .await
        .unwrap();
    assert_eq!(r.output(), "ctx");
}

#[tokio::test]
async fn an_oversized_path_drops_out_of_the_continuation_marker() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("big.txt");
    std::fs::write(&file, "a".repeat(40 * 1024)).unwrap();
    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .fixed_path(file)
        .arc();
    let long_path = "p".repeat(20 * 1024);
    let r = FileReadTool::new(gate)
        .execute(json!({"path": long_path}))
        .await
        .unwrap();
    assert!(!r.is_error);
    let out = r.output();
    assert!(
        out.contains("continue with file_read at \"offset\":"),
        "marker missing"
    );
    assert!(!out.contains("ppppp"));
    assert!(out.len() <= 16 * 1024);
}
