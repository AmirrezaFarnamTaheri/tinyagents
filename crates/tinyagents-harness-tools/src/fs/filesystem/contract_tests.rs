//! Wire-contract fixtures: the name, description, permission level, exposure
//! and JSON Schema each filesystem tool advertises to the model.
//!
//! These are what a model is prompted with, and what existing transcripts were
//! produced against, so they are pinned against literal fixture files rather
//! than re-derived from the code under test. A change here is a change to the
//! tool's public contract and must be made on purpose.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use tinytools::Tool;

use super::*;
use crate::filesystem::test_support::{AutonomyLevel, TestGate};

fn tools() -> Vec<(&'static str, Box<dyn Tool>)> {
    let gate = TestGate::supervised(std::env::temp_dir());
    let workspace = std::env::temp_dir();
    vec![
        (
            "apply_patch",
            Box::new(ApplyPatchTool::new(gate.clone())) as Box<dyn Tool>,
        ),
        ("csv_export", Box::new(CsvExportTool::new(gate.clone()))),
        ("edit_file", Box::new(EditFileTool::new(gate.clone()))),
        ("file_read", Box::new(FileReadTool::new(gate.clone()))),
        ("file_write", Box::new(FileWriteTool::new(gate.clone()))),
        (
            "git_operations",
            Box::new(GitOperationsTool::new(gate.clone(), workspace.clone())),
        ),
        ("glob_search", Box::new(GlobTool::new(gate.clone()))),
        ("grep", Box::new(GrepTool::new(gate.clone()))),
        ("image_info", Box::new(ImageInfoTool::new(gate.clone()))),
        ("list_files", Box::new(ListFilesTool::new(gate))),
        ("read_diff", Box::new(ReadDiffTool::new(workspace.clone()))),
        (
            "run_linter",
            Box::new(RunLinterTool::new(workspace.clone())),
        ),
        ("run_tests", Box::new(RunTestsTool::new(workspace.clone()))),
        (
            "update_memory_md",
            Box::new(UpdateMemoryMdTool::new(workspace.clone())),
        ),
        (
            "read_workspace_state",
            Box::new(WorkspaceStateTool::new(workspace)),
        ),
    ]
}

fn describe(tool: &dyn Tool) -> Value {
    json!({
        "name": tool.name(),
        "description": tool.description(),
        "permission_level": format!("{:?}", tool.permission_level()),
        "exposure": format!("{:?}", tool.exposure()),
        "schema": tool.parameters_schema(),
    })
}

fn fixture(name: &str) -> Value {
    let raw = match name {
        "apply_patch" => include_str!("fixtures/apply_patch.json"),
        "csv_export" => include_str!("fixtures/csv_export.json"),
        "edit_file" => include_str!("fixtures/edit_file.json"),
        "file_read" => include_str!("fixtures/file_read.json"),
        "file_write" => include_str!("fixtures/file_write.json"),
        "git_operations" => include_str!("fixtures/git_operations.json"),
        "glob_search" => include_str!("fixtures/glob_search.json"),
        "grep" => include_str!("fixtures/grep.json"),
        "image_info" => include_str!("fixtures/image_info.json"),
        "list_files" => include_str!("fixtures/list_files.json"),
        "read_diff" => include_str!("fixtures/read_diff.json"),
        "run_linter" => include_str!("fixtures/run_linter.json"),
        "run_tests" => include_str!("fixtures/run_tests.json"),
        "update_memory_md" => include_str!("fixtures/update_memory_md.json"),
        "read_workspace_state" => include_str!("fixtures/read_workspace_state.json"),
        other => panic!("no fixture for {other}"),
    };
    serde_json::from_str(raw).expect("fixture is valid JSON")
}

#[test]
fn every_tool_advertises_exactly_its_pinned_contract() {
    for (fixture_name, tool) in tools() {
        assert_eq!(
            describe(tool.as_ref()),
            fixture(fixture_name),
            "contract drift in `{fixture_name}`"
        );
    }
}

#[test]
fn tool_names_are_unique() {
    let mut names: Vec<String> = tools().iter().map(|(_, t)| t.name().to_string()).collect();
    names.sort();
    let before = names.len();
    names.dedup();
    assert_eq!(before, names.len());
}

#[test]
fn a_gate_can_be_shared_across_tools() {
    let gate: Arc<dyn FsGate> = TestGate::supervised(std::env::temp_dir());
    let _read = FileReadTool::new(Arc::clone(&gate));
    let _write = FileWriteTool::new(gate);
}

fn gate_at(autonomy: AutonomyLevel) -> Arc<TestGate> {
    TestGate::with(std::env::temp_dir(), autonomy, 1_000_000)
}

#[test]
fn write_tools_route_through_approval_only_when_the_gate_asks() {
    let existing = tempfile::tempdir().unwrap();
    std::fs::write(existing.path().join("f.txt"), "x").unwrap();
    let existing_arg = json!({"path": existing.path().join("f.txt").to_string_lossy()});
    let new_arg = json!({"path": existing.path().join("missing.txt").to_string_lossy()});

    for (autonomy, prompts) in [
        (AutonomyLevel::Supervised, true),
        (AutonomyLevel::Full, false),
        (AutonomyLevel::ReadOnly, false),
    ] {
        let gate = gate_at(autonomy);
        assert_eq!(
            EditFileTool::new(gate.clone()).external_effect_with_args(&json!({})),
            prompts
        );
        assert_eq!(
            ApplyPatchTool::new(gate.clone()).external_effect_with_args(&json!({})),
            prompts
        );
        let write = FileWriteTool::new(gate.clone());
        // Overwriting prompts, creating does not, an unknown path fails safe.
        assert_eq!(write.external_effect_with_args(&existing_arg), prompts);
        assert!(!write.external_effect_with_args(&new_arg));
        assert_eq!(write.external_effect_with_args(&json!({})), prompts);
        let git = GitOperationsTool::new(gate, std::env::temp_dir());
        assert_eq!(
            git.external_effect_with_args(&json!({"operation": "commit"})),
            prompts
        );
        assert!(!git.external_effect_with_args(&json!({"operation": "status"})));
    }
}
