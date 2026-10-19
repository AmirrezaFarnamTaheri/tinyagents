//! Budget, delegation and unreadable-file tests for `grep`.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{TestGate, WorkspaceContext, WrapGate};
use tempfile::TempDir;

#[test]
fn grep_is_concurrency_safe() {
    let dir = TempDir::new().unwrap();
    let tool = GrepTool::new(TestGate::supervised(dir.path().to_path_buf()));
    assert!(tool.is_concurrency_safe(&json!({})));
}

#[tokio::test]
async fn rate_limit_and_exhausted_budget_are_refused() {
    let dir = TempDir::new().unwrap();
    let limited = GrepTool::new(TestGate::with(
        dir.path().to_path_buf(),
        crate::filesystem::test_support::AutonomyLevel::Supervised,
        0,
    ));
    let r = limited.execute(json!({"pattern": "x"})).await.unwrap();
    assert!(r.output().contains("too many actions in the last hour"));

    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .no_budget()
        .arc();
    let r = GrepTool::new(gate)
        .execute(json!({"pattern": "x"}))
        .await
        .unwrap();
    assert!(r.output().contains("action budget exhausted"));
}

#[tokio::test]
async fn non_utf8_files_are_skipped_and_others_still_match() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("bin.dat"), [0xff, 0xfe, 0x00, 0xc3]).unwrap();
    std::fs::write(dir.path().join("t.txt"), "needle here\n").unwrap();
    let tool = GrepTool::new(TestGate::supervised(dir.path().to_path_buf()));
    let ctx = WorkspaceContext::at(dir.path());
    let r = tool
        .execute_with_context(
            json!({"pattern": "needle"}),
            ToolCallOptions::default(),
            Some(&ctx),
        )
        .await
        .unwrap();
    assert!(r.output().contains("t.txt"), "{}", r.output());
    assert!(!r.output().contains("bin.dat"));
}
