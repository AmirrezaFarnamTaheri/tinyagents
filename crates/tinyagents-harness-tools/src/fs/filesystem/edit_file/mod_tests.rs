//! Behavior tests for the `edit_file` tool, driven through a fake [`FsGate`].

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{AutonomyLevel, TestGate};

fn test_security(workspace: std::path::PathBuf) -> Arc<TestGate> {
    TestGate::supervised(workspace)
}

fn test_security_readonly(workspace: std::path::PathBuf) -> Arc<TestGate> {
    TestGate::with(workspace, AutonomyLevel::ReadOnly, 1_000_000)
}

#[test]
fn edit_name() {
    let tool = EditFileTool::new(test_security(std::env::temp_dir()));
    assert_eq!(tool.name(), "edit");
}

#[tokio::test]
async fn edit_replaces_unique_match() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_unique");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "alpha bravo")
        .await
        .unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "bravo", "new_string": "charlie"}))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    let updated = tokio::fs::read_to_string(dir.join("f.txt")).await.unwrap();
    assert_eq!(updated, "alpha charlie");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_rejects_ambiguous_match() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_ambig");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "x x x").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "x", "new_string": "y"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("matches 3 times"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_replace_all() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_all");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "x x x").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(
            json!({"path": "f.txt", "old_string": "x", "new_string": "y", "replace_all": true}),
        )
        .await
        .unwrap();
    assert!(!result.is_error);
    let updated = tokio::fs::read_to_string(dir.join("f.txt")).await.unwrap();
    assert_eq!(updated, "y y y");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_no_match() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_nomatch");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "alpha").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "zulu", "new_string": "x"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("not found"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_blocks_readonly_mode() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_ro");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "abc").await.unwrap();

    let tool = EditFileTool::new(test_security_readonly(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "abc", "new_string": "xyz"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("read-only"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_rejects_empty_old_string() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_empty_old");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "abc").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "", "new_string": "x"}))
        .await
        .unwrap();
    assert!(result.is_error);

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_rejects_identical_strings() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_same");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("f.txt"), "abc").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "abc", "new_string": "abc"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("identical"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn edit_reports_an_os_write_failure_rather_than_a_silent_success() {
    let dir = std::env::temp_dir().join("openhuman_test_edit_os_write_fail");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    let file = dir.join("f.txt");
    tokio::fs::write(&file, "abc").await.unwrap();

    let tool = EditFileTool::new(test_security(dir.clone())).with_sink(Arc::new(
        crate::filesystem::file_sink::RefusingSink(std::io::ErrorKind::PermissionDenied),
    ));
    let result = tool
        .execute(json!({"path": "f.txt", "old_string": "abc", "new_string": "xyz"}))
        .await
        .unwrap();

    assert!(
        result.is_error,
        "a refused write must surface as an error, not a fabricated success"
    );
    assert!(result.output().contains("Failed to write file"));
    assert_eq!(
        tokio::fs::read_to_string(&file).await.unwrap(),
        "abc",
        "the file must be left as it was when the write did not happen"
    );

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

// ── Coverage of the budget, symlink, size, read and guard branches ──────

use crate::filesystem::test_support::{RacyGate, WorkspaceContext};

fn edit_args(path: &str) -> serde_json::Value {
    json!({"path": path, "old_string": "aaa", "new_string": "bbb"})
}

#[tokio::test]
async fn edit_reports_rate_limits() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("f.txt"), "aaa").unwrap();

    let limited = EditFileTool::new(TestGate::with(
        dir.path().to_path_buf(),
        AutonomyLevel::Supervised,
        0,
    ));
    let result = limited.execute(edit_args("f.txt")).await.unwrap();
    assert!(
        result
            .output()
            .contains("too many actions in the last hour")
    );

    let racy = EditFileTool::new(Arc::new(RacyGate(test_security(dir.path().to_path_buf()))));
    let result = racy.execute(edit_args("f.txt")).await.unwrap();
    assert!(result.output().contains("action budget exhausted"));
}

#[cfg(unix)]
#[tokio::test]
async fn edit_refuses_to_edit_through_a_symlink() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real.txt"), "aaa").unwrap();
    std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt")).unwrap();
    let tool = EditFileTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(edit_args("link.txt")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Refusing to edit through symlink"));
}

#[tokio::test]
async fn edit_reports_a_missing_file_and_unreadable_contents() {
    let dir = tempfile::tempdir().unwrap();
    let tool = EditFileTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(edit_args("absent.txt")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Failed to resolve"));

    std::fs::write(dir.path().join("bin.dat"), [0xff_u8, 0xfe, 0xfd]).unwrap();
    let result = tool.execute(edit_args("bin.dat")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Failed to read file"));
}

#[tokio::test]
async fn edit_refuses_an_oversized_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("big.txt")).unwrap();
    file.set_len(MAX_FILE_BYTES + 1).unwrap();
    let tool = EditFileTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(edit_args("big.txt")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("File too large"));
}

#[tokio::test]
async fn edit_uses_the_context_workspace_when_one_is_threaded() {
    let home = tempfile::tempdir().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    std::fs::write(isolated.path().join("w.txt"), "aaa").unwrap();
    let tool = EditFileTool::new(test_security(home.path().to_path_buf()));
    let context = WorkspaceContext::at(isolated.path());
    let result = tool
        .execute_with_context(
            edit_args("w.txt"),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        std::fs::read_to_string(isolated.path().join("w.txt")).unwrap(),
        "bbb"
    );
}

#[tokio::test]
async fn edit_honours_the_file_state_guard_and_records_its_write() {
    crate::file_state::init_global(true);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().canonicalize().unwrap().join("g.txt");
    std::fs::write(&target, "aaa").unwrap();
    let tool = EditFileTool::new(test_security(dir.path().to_path_buf()));
    let agent = format!("ed-agent-{}", dir.path().display());
    let other = format!("ed-other-{}", dir.path().display());
    let run = |agent: String| {
        crate::file_state::with_file_state_agent_id(agent, tool.execute(edit_args("g.txt")))
    };

    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        true,
        std::time::Instant::now(),
    );
    let result = run(agent.clone()).await.unwrap();
    assert!(result.output().contains("Partial read"));

    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    crate::file_state::record_write(&other, target.clone());
    let result = run(agent.clone()).await.unwrap();
    assert!(result.output().contains("Stale read"));

    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    let result = run(agent.clone()).await.unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert!(crate::file_state::check_stale_read(&agent, &target).is_none());
}
