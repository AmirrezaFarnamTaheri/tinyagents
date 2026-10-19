//! End-to-end tests of the `git_operations` sub-operations against real,
//! hermetic repositories: parsing, success paths, failure paths and gate
//! refusals.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::test::{hermetic, init_git_repo, test_tool};
use super::*;
use crate::filesystem::test_support::{AutonomyLevel, TestGate, WorkspaceContext};
use tempfile::TempDir;

fn git(dir: &std::path::Path, args: &[&str]) {
    let out = hermetic(
        std::process::Command::new("git")
            .args(args)
            .current_dir(dir),
    )
    .output()
    .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A repo with one commit of `a.txt` and identity configured locally.
fn repo_with_commit() -> TempDir {
    let tmp = TempDir::new().unwrap();
    init_git_repo(tmp.path());
    git(tmp.path(), &["config", "user.email", "t@example.invalid"]);
    git(tmp.path(), &["config", "user.name", "Tester"]);
    git(tmp.path(), &["checkout", "-b", "main"]);
    std::fs::write(tmp.path().join("a.txt"), "one\ntwo\nthree\n").unwrap();
    git(tmp.path(), &["add", "a.txt"]);
    git(tmp.path(), &["commit", "-m", "first commit"]);
    tmp
}

async fn run(tool: &GitOperationsTool, args: serde_json::Value) -> ToolResult {
    tool.execute(args).await.unwrap()
}

#[test]
fn supports_markdown_is_advertised() {
    let tmp = TempDir::new().unwrap();
    assert!(test_tool(tmp.path()).supports_markdown());
}

#[tokio::test]
async fn status_parses_staged_unstaged_and_untracked() {
    let tmp = repo_with_commit();
    std::fs::write(tmp.path().join("a.txt"), "one\nTWO\nthree\n").unwrap();
    git(tmp.path(), &["add", "a.txt"]);
    std::fs::write(tmp.path().join("a.txt"), "changed again\n").unwrap();
    std::fs::write(tmp.path().join("new.txt"), "n").unwrap();
    let tool = test_tool(tmp.path());

    let result = run(&tool, json!({"operation": "status"})).await;
    assert!(!result.is_error, "{}", result.output());
    let parsed: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_ne!(parsed["staged"].as_array().unwrap().len(), 0);
    assert_ne!(parsed["unstaged"].as_array().unwrap().len(), 0);
    assert!(
        parsed["untracked"]
            .as_array()
            .unwrap()
            .contains(&json!("new.txt"))
    );
    assert!(result.markdown_formatted.is_some());
}

#[tokio::test]
async fn diff_reports_hunks_for_unstaged_and_cached_changes() {
    let tmp = repo_with_commit();
    std::fs::write(tmp.path().join("a.txt"), "one\nTWO\nthree\n").unwrap();
    std::fs::write(tmp.path().join("b.txt"), "x\n").unwrap();
    git(tmp.path(), &["add", "b.txt"]);
    let tool = test_tool(tmp.path());

    let unstaged = run(&tool, json!({"operation": "diff"})).await;
    let parsed: serde_json::Value = serde_json::from_str(&unstaged.output()).unwrap();
    assert!(parsed["file_count"].as_u64().unwrap() >= 1);
    assert_eq!(parsed["hunks"][0]["file"], "a.txt");
    let kinds: Vec<&str> = parsed["hunks"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|h| h["lines"].as_array().unwrap().iter())
        .map(|l| l["type"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"add") && kinds.contains(&"delete") && kinds.contains(&"context"));

    let cached = run(&tool, json!({"operation": "diff", "cached": true})).await;
    let parsed: serde_json::Value = serde_json::from_str(&cached.output()).unwrap();
    assert_eq!(parsed["hunks"][0]["file"], "b.txt");
}

#[tokio::test]
async fn diff_with_several_files_and_hunks_splits_them() {
    let tmp = repo_with_commit();
    let long: String = (0..40).fold(String::new(), |acc, i| acc + &format!("line{i}\n"));
    std::fs::write(tmp.path().join("big.txt"), &long).unwrap();
    git(tmp.path(), &["add", "big.txt"]);
    git(tmp.path(), &["commit", "-m", "big"]);
    let edited = long
        .replace("line1\n", "LINE1\n")
        .replace("line38\n", "LINE38\n");
    std::fs::write(tmp.path().join("big.txt"), edited).unwrap();
    std::fs::write(tmp.path().join("a.txt"), "different\n").unwrap();
    let tool = test_tool(tmp.path());

    let result = run(&tool, json!({"operation": "diff"})).await;
    let parsed: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert!(parsed["file_count"].as_u64().unwrap() >= 3);
    assert!(
        parsed["hunks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["header"].is_string())
    );
}

#[tokio::test]
async fn diff_rejects_injected_file_arguments() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = tool
        .execute(json!({"operation": "diff", "files": "--exec=evil"}))
        .await;
    assert!(result.is_err() || result.unwrap().is_error);
}

#[tokio::test]
async fn log_lists_commits_with_markdown() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = run(&tool, json!({"operation": "log", "limit": 5})).await;
    assert!(!result.is_error, "{}", result.output());
    let parsed: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(parsed["commits"][0]["message"], "first commit");
    assert_eq!(parsed["commits"][0]["author"], "Tester");
    assert!(result.markdown_formatted.unwrap().contains("first commit"));
}

#[tokio::test]
async fn branch_lists_the_current_branch() {
    let tmp = repo_with_commit();
    git(tmp.path(), &["branch", "side"]);
    let tool = test_tool(tmp.path());
    let result = run(&tool, json!({"operation": "branch"})).await;
    let parsed: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(parsed["current"], "main");
    assert_eq!(parsed["branches"].as_array().unwrap().len(), 2);
    assert!(result.markdown_formatted.unwrap().contains("← current"));
}

#[tokio::test]
async fn add_then_commit_succeeds() {
    let tmp = repo_with_commit();
    std::fs::write(tmp.path().join("c.txt"), "c").unwrap();
    let tool = test_tool(tmp.path());

    let added = run(&tool, json!({"operation": "add", "paths": "c.txt"})).await;
    assert_eq!(added.output(), "Staged: c.txt");

    let committed = run(
        &tool,
        json!({"operation": "commit", "message": "  add c  \n\n  more  "}),
    )
    .await;
    assert!(!committed.is_error, "{}", committed.output());
    assert_eq!(committed.output(), "Committed: add c\nmore");
}

#[tokio::test]
async fn add_of_a_missing_path_reports_failure() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = run(&tool, json!({"operation": "add", "paths": "nope.txt"})).await;
    assert!(result.is_error);
    assert!(result.output().starts_with("Add failed:"));
}

#[tokio::test]
async fn commit_with_nothing_staged_reports_failure() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = run(&tool, json!({"operation": "commit", "message": "empty"})).await;
    assert!(result.is_error);
    assert!(result.output().starts_with("Commit failed:"));
}

#[tokio::test]
async fn commit_with_blank_message_is_an_error() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = tool
        .execute(json!({"operation": "commit", "message": " \n "}))
        .await;
    assert!(result.unwrap_err().to_string().contains("cannot be empty"));
}

#[test]
fn overlong_commit_messages_are_truncated() {
    let long = "x".repeat(2500);
    let out = GitOperationsTool::truncate_commit_message(&long);
    assert_eq!(out.chars().count(), 2000);
    assert!(out.ends_with("..."));
    assert_eq!(GitOperationsTool::truncate_commit_message("short"), "short");
}

#[tokio::test]
async fn checkout_switches_and_reports_failures() {
    let tmp = repo_with_commit();
    git(tmp.path(), &["branch", "side"]);
    let tool = test_tool(tmp.path());

    let ok = run(&tool, json!({"operation": "checkout", "branch": "side"})).await;
    assert_eq!(ok.output(), "Switched to branch: side");

    let missing = run(&tool, json!({"operation": "checkout", "branch": "ghost"})).await;
    assert!(missing.is_error);
    assert!(missing.output().starts_with("Checkout failed:"));

    let invalid = tool
        .execute(json!({"operation": "checkout", "branch": "--evil"}))
        .await;
    assert!(invalid.is_err() || invalid.unwrap().is_error);
}

#[tokio::test]
async fn stash_push_list_pop_and_drop() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    std::fs::write(tmp.path().join("a.txt"), "dirty\n").unwrap();

    let push = run(&tool, json!({"operation": "stash", "action": "push"})).await;
    assert!(!push.is_error, "{}", push.output());
    let list = run(&tool, json!({"operation": "stash", "action": "list"})).await;
    assert!(list.output().contains("auto-stash"));
    let pop = run(&tool, json!({"operation": "stash", "action": "pop"})).await;
    assert!(!pop.is_error, "{}", pop.output());

    git(tmp.path(), &["stash", "push", "-m", "again"]);
    let drop = run(
        &tool,
        json!({"operation": "stash", "action": "drop", "index": 0}),
    )
    .await;
    assert!(!drop.is_error, "{}", drop.output());

    let bad = run(
        &tool,
        json!({"operation": "stash", "action": "drop", "index": 7}),
    )
    .await;
    assert!(bad.is_error);
    assert!(bad.output().starts_with("Stash drop failed:"));

    let huge = tool
        .execute(json!({"operation": "stash", "action": "drop", "index": 4_000_000_000u64}))
        .await;
    assert!(huge.unwrap_err().to_string().contains("too large"));
}

#[tokio::test]
async fn pop_with_no_stash_reports_failure() {
    let tmp = repo_with_commit();
    let tool = test_tool(tmp.path());
    let result = run(&tool, json!({"operation": "stash", "action": "pop"})).await;
    assert!(result.is_error);
    assert!(result.output().starts_with("Stash pop failed:"));
}

#[tokio::test]
async fn write_operations_are_refused_when_the_gate_is_read_only() {
    let tmp = repo_with_commit();
    let gate = TestGate::with(tmp.path().to_path_buf(), AutonomyLevel::ReadOnly, 1_000_000);
    let tool = GitOperationsTool::new(gate, tmp.path().to_path_buf());
    let result = run(&tool, json!({"operation": "add", "paths": "a.txt"})).await;
    assert!(result.is_error);
    assert!(result.output().contains("higher autonomy level"));
}

/// A gate that permits acting but reports strict read-only mode.
#[derive(Debug)]
struct ReadOnlyButActing(std::path::PathBuf);

#[async_trait]
impl FsGate for ReadOnlyButActing {
    fn can_act(&self) -> bool {
        true
    }
    fn is_read_only(&self) -> bool {
        true
    }
    fn is_rate_limited(&self) -> bool {
        false
    }
    fn record_action(&self) -> bool {
        true
    }
    fn write_needs_approval(&self) -> bool {
        false
    }
    fn action_dir(&self) -> &Path {
        &self.0
    }
    fn is_path_string_allowed(&self, _path: &str) -> bool {
        true
    }
    async fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        Ok(PathBuf::from(path))
    }
    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String> {
        Ok(PathBuf::from(path))
    }
    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate> {
        Arc::new(Self(root.to_path_buf()))
    }
}

#[tokio::test]
async fn read_only_mode_blocks_writes_even_when_the_gate_allows_acting() {
    let tmp = repo_with_commit();
    let gate: Arc<dyn FsGate> = Arc::new(ReadOnlyButActing(tmp.path().to_path_buf()));
    let tool = GitOperationsTool::new(gate, tmp.path().to_path_buf());
    let result = run(&tool, json!({"operation": "add", "paths": "a.txt"})).await;
    assert_eq!(
        result.output(),
        "[policy-blocked] Action blocked: read-only mode"
    );
}

#[tokio::test]
async fn an_exhausted_action_budget_blocks_the_operation() {
    let tmp = repo_with_commit();
    let gate = TestGate::with(tmp.path().to_path_buf(), AutonomyLevel::Supervised, 0);
    let tool = GitOperationsTool::new(gate, tmp.path().to_path_buf());
    let result = run(&tool, json!({"operation": "status"})).await;
    assert_eq!(result.output(), "Action blocked: rate limit exceeded");
}

#[tokio::test]
async fn the_execute_wrappers_agree_and_honour_a_workspace_descriptor() {
    let unrelated = TempDir::new().unwrap();
    let repo = repo_with_commit();
    let tool = test_tool(unrelated.path());
    let context = WorkspaceContext::at(repo.path());

    let with_options = tool
        .execute_with_options(json!({"operation": "branch"}), ToolCallOptions::default())
        .await
        .unwrap();
    assert!(with_options.is_error, "the unrelated dir is not a repo");

    let with_context = tool
        .execute_with_context(
            json!({"operation": "branch"}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!with_context.is_error, "{}", with_context.output());
}
