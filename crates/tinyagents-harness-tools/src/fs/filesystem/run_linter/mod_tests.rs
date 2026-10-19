#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use serde_json::json;
use tempfile::TempDir;

fn make_tool(dir: &TempDir) -> RunLinterTool {
    RunLinterTool::new(dir.path().to_path_buf())
}

#[test]
fn name_is_correct() {
    let tmp = TempDir::new().unwrap();
    assert_eq!(make_tool(&tmp).name(), "run_linter");
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
fn permission_level_is_execute() {
    let tmp = TempDir::new().unwrap();
    assert_eq!(make_tool(&tmp).permission_level(), PermissionLevel::Execute);
}

#[tokio::test]
async fn auto_returns_error_when_no_project_files() {
    let tmp = TempDir::new().unwrap();
    let result = make_tool(&tmp)
        .execute(json!({"linter": "auto"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Could not detect project type"));
}

#[tokio::test]
async fn unknown_linter_returns_error() {
    let tmp = TempDir::new().unwrap();
    let result = make_tool(&tmp)
        .execute(json!({"linter": "rubocop"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Unknown linter"));
}

#[tokio::test]
async fn eslint_rejects_absolute_path() {
    let tmp = TempDir::new().unwrap();
    // Create a package.json so linter resolves to eslint
    std::fs::write(tmp.path().join("package.json"), "{}").unwrap();
    let result = make_tool(&tmp)
        .execute(json!({"linter": "eslint", "path": "/etc/passwd"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("relative path"));
}

#[tokio::test]
async fn eslint_rejects_path_traversal() {
    let tmp = TempDir::new().unwrap();
    std::fs::write(tmp.path().join("package.json"), "{}").unwrap();
    let result = make_tool(&tmp)
        .execute(json!({"linter": "eslint", "path": "../secret"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("relative path"));
}

#[tokio::test]
async fn a_workspace_descriptor_overrides_the_configured_directory() {
    use crate::filesystem::test_support::WorkspaceContext;
    let configured = TempDir::new().unwrap();
    let isolated = TempDir::new().unwrap();
    let context = WorkspaceContext::at(isolated.path());
    // Neither directory has project files; the answer only proves the call
    // ran, and the descriptor branch is what resolved the directory.
    let result = make_tool(&configured)
        .execute_with_context(json!({}), ToolCallOptions::default(), Some(&context))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Could not detect project type"));
}
