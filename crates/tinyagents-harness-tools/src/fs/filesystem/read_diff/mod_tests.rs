#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use serde_json::json;
use tempfile::TempDir;

fn make_tool(dir: &TempDir) -> ReadDiffTool {
    ReadDiffTool::new(dir.path().to_path_buf())
}

#[test]
fn name_is_correct() {
    let tmp = TempDir::new().unwrap();
    assert_eq!(make_tool(&tmp).name(), "read_diff");
}

#[test]
fn description_is_non_empty() {
    let tmp = TempDir::new().unwrap();
    assert_ne!(make_tool(&tmp).description().len(), 0);
}

#[test]
fn schema_is_object_type() {
    let tmp = TempDir::new().unwrap();
    let schema = make_tool(&tmp).parameters_schema();
    assert_eq!(schema["type"], "object");
}

#[test]
fn permission_level_is_read_only() {
    let tmp = TempDir::new().unwrap();
    assert_eq!(
        make_tool(&tmp).permission_level(),
        PermissionLevel::ReadOnly
    );
}

#[tokio::test]
async fn execute_returns_error_for_non_git_dir() {
    let tmp = TempDir::new().unwrap();
    let result = make_tool(&tmp).execute(json!({})).await.unwrap();
    // Non-git dir: git will fail, tool returns error
    assert!(result.is_error);
}

#[tokio::test]
async fn execute_no_changes_in_clean_git_repo() {
    let tmp = TempDir::new().unwrap();
    // Init a git repo and make an initial commit so there's nothing to diff
    let _ = std::process::Command::new("git")
        .args(["init"])
        .current_dir(tmp.path())
        .output();
    let _ = std::process::Command::new("git")
        .args(["commit", "--allow-empty", "-m", "init"])
        .current_dir(tmp.path())
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "t@t.com")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "t@t.com")
        .output();
    let result = make_tool(&tmp).execute(json!({})).await.unwrap();
    assert!(!result.is_error);
    assert!(result.output().contains("No changes found."));
}

fn git_in(dir: &std::path::Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(["-c", "user.email=t@example.invalid", "-c", "user.name=T"])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn repo_with_change() -> TempDir {
    let tmp = TempDir::new().unwrap();
    git_in(tmp.path(), &["init"]);
    std::fs::write(tmp.path().join("a.txt"), "one\n").unwrap();
    std::fs::write(tmp.path().join("b.txt"), "one\n").unwrap();
    git_in(tmp.path(), &["add", "."]);
    git_in(tmp.path(), &["commit", "-m", "init"]);
    std::fs::write(tmp.path().join("a.txt"), "two\n").unwrap();
    std::fs::write(tmp.path().join("b.txt"), "two\n").unwrap();
    tmp
}

#[tokio::test]
async fn path_filter_limits_the_diff() {
    let tmp = repo_with_change();
    let result = make_tool(&tmp)
        .execute(json!({"path_filter": "a.txt"}))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert!(result.output().contains("a.txt"));
    assert!(!result.output().contains("b.txt"));
}

#[tokio::test]
async fn base_ref_and_staged_flags_are_passed_to_git() {
    let tmp = repo_with_change();
    git_in(tmp.path(), &["add", "a.txt"]);
    let staged = make_tool(&tmp)
        .execute(json!({"staged": true}))
        .await
        .unwrap();
    assert!(staged.output().contains("a.txt"));
    assert!(!staged.output().contains("b.txt"));

    let against_head = make_tool(&tmp)
        .execute(json!({"base": "HEAD"}))
        .await
        .unwrap();
    assert!(against_head.output().contains("b.txt"));

    let bad_base = make_tool(&tmp)
        .execute(json!({"base": "no-such-ref"}))
        .await
        .unwrap();
    assert!(bad_base.is_error);
}

#[tokio::test]
async fn a_workspace_descriptor_overrides_the_configured_directory() {
    use crate::filesystem::test_support::WorkspaceContext;
    let elsewhere = TempDir::new().unwrap();
    let repo = repo_with_change();
    let context = WorkspaceContext::at(repo.path());
    let result = make_tool(&elsewhere)
        .execute_with_context(json!({}), ToolCallOptions::default(), Some(&context))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert!(result.output().contains("a.txt"));
}
