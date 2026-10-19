//! Behavior tests for the `run_tests` tool.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::WorkspaceContext;
use serde_json::json;
use tempfile::TempDir;

fn make_tool(dir: &TempDir) -> RunTestsTool {
    RunTestsTool::new(dir.path().to_path_buf())
}

/// A throwaway cargo package whose single test body is `test_body`.
fn cargo_project(test_body: &str) -> TempDir {
    let tmp = TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[workspace]\n",
    )
    .unwrap();
    std::fs::create_dir(tmp.path().join("src")).unwrap();
    std::fs::write(
        tmp.path().join("src/lib.rs"),
        format!("#[test]\nfn fixture_test() {{\n{test_body}\n}}\n"),
    )
    .unwrap();
    tmp
}

#[test]
fn name_and_description_are_pinned() {
    let tmp = TempDir::new().unwrap();
    let tool = make_tool(&tmp);
    assert_eq!(tool.name(), "run_tests");
    assert_eq!(
        tool.description(),
        "Run the project test suite. Supports 'cargo_test' for Rust and 'vitest' for \
         TypeScript/JavaScript. Returns pass/fail results with output."
    );
}

#[test]
fn schema_is_pinned() {
    let tmp = TempDir::new().unwrap();
    assert_eq!(
        make_tool(&tmp).parameters_schema(),
        json!({
            "type": "object",
            "properties": {
                "runner": {
                    "type": "string",
                    "enum": ["cargo_test", "vitest", "auto"],
                    "description": "Which test runner to use. 'auto' detects from project files.",
                    "default": "auto"
                },
                "filter": {
                    "type": "string",
                    "description": "Filter to run specific tests (e.g. test name or module)."
                },
                "timeout_secs": {
                    "type": "integer",
                    "description": "Timeout in seconds (default: 120).",
                    "default": 120
                }
            }
        })
    );
}

#[test]
fn permission_level_and_exposure() {
    let tmp = TempDir::new().unwrap();
    let tool = make_tool(&tmp);
    assert_eq!(tool.permission_level(), PermissionLevel::Execute);
    assert_eq!(tool.exposure(), tinytools::ToolExposure::Deferred);
}

#[tokio::test]
async fn auto_returns_error_when_no_project_files() {
    let tmp = TempDir::new().unwrap();
    let result = make_tool(&tmp).execute(json!({})).await.unwrap();
    assert!(result.is_error);
    assert!(
        result
            .output()
            .contains("Could not detect project type for testing.")
    );
}

#[tokio::test]
async fn unknown_runner_returns_error() {
    let tmp = TempDir::new().unwrap();
    let result = make_tool(&tmp)
        .execute(json!({"runner": "pytest"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(result.output(), "Unknown test runner: pytest");
}

#[tokio::test]
async fn auto_detects_a_node_project_as_vitest() {
    let tmp = TempDir::new().unwrap();
    std::fs::write(tmp.path().join("package.json"), "{}").unwrap();
    // The runner may or may not be installed; either way the tool must have
    // picked `vitest` rather than reporting an undetectable project.
    let result = make_tool(&tmp)
        .execute(json!({"timeout_secs": 1}))
        .await
        .unwrap();
    assert!(!result.output().contains("Could not detect project type"));
}

#[tokio::test]
async fn passing_cargo_project_reports_success() {
    let tmp = cargo_project("assert_eq!(1 + 1, 2);");
    let result = make_tool(&tmp)
        .execute(json!({"runner": "cargo_test", "filter": "fixture_test"}))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert!(result.output().contains("test result: ok"));
}

#[tokio::test]
async fn failing_cargo_project_reports_the_exit_code_and_truncates_output() {
    let tmp = cargo_project("panic!(\"{}\", \"x\".repeat(20_000));");
    let result = make_tool(&tmp).execute(json!({})).await.unwrap();
    assert!(result.is_error);
    let output = result.output();
    assert!(output.starts_with("Tests exited with code Some(101)"));
    assert!(output.contains("[truncated, "));
}

#[tokio::test]
async fn a_timeout_is_reported() {
    let tmp = cargo_project("assert!(true);");
    let result = make_tool(&tmp)
        .execute(json!({"runner": "cargo_test", "timeout_secs": 0}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(result.output(), "test execution timed out after 0s");
}

#[tokio::test]
async fn a_workspace_descriptor_overrides_the_configured_directory() {
    let configured = TempDir::new().unwrap();
    let isolated = TempDir::new().unwrap();
    std::fs::write(isolated.path().join("package.json"), "{}").unwrap();
    let context = WorkspaceContext::at(isolated.path());
    // The configured directory has no project files; only the isolated
    // workspace does, so detection succeeding proves the override was used.
    let result = make_tool(&configured)
        .execute_with_context(
            json!({"timeout_secs": 1}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.output().contains("Could not detect project type"));
}
