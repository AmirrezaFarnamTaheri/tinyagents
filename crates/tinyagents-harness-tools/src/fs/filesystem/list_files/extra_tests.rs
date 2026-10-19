//! Budget, delegation and entry-cap tests for `list_files`.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{TestGate, WorkspaceContext, WrapGate};
use tempfile::TempDir;

#[tokio::test]
async fn rate_limit_and_exhausted_budget_are_refused() {
    let dir = TempDir::new().unwrap();
    let limited = ListFilesTool::new(TestGate::with(
        dir.path().to_path_buf(),
        crate::filesystem::test_support::AutonomyLevel::Supervised,
        0,
    ));
    let r = limited.execute(json!({})).await.unwrap();
    assert!(r.output().contains("too many actions in the last hour"));

    let gate = WrapGate::new(TestGate::supervised(dir.path().to_path_buf()))
        .no_budget()
        .arc();
    let r = ListFilesTool::new(gate).execute(json!({})).await.unwrap();
    assert!(r.output().contains("action budget exhausted"));
}

#[tokio::test]
async fn execute_with_context_delegates() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("f.txt"), "").unwrap();
    let tool = ListFilesTool::new(TestGate::supervised(dir.path().to_path_buf()));
    let ctx = WorkspaceContext::at(dir.path());
    let r = tool
        .execute_with_context(json!({}), ToolCallOptions::default(), Some(&ctx))
        .await
        .unwrap();
    assert!(r.output().contains("file\tf.txt"));
}

#[tokio::test]
async fn listing_stops_at_the_entry_cap() {
    let dir = TempDir::new().unwrap();
    for i in 0..(MAX_ENTRIES + 5) {
        std::fs::write(dir.path().join(format!("f{i:05}")), "").unwrap();
    }
    let tool = ListFilesTool::new(TestGate::supervised(dir.path().to_path_buf()));
    let r = tool.execute(json!({})).await.unwrap();
    assert!(
        r.output()
            .starts_with(&format!("{MAX_ENTRIES} entr(ies) in ."))
    );
}
