//! Custom catalog provenance, native bounds and destination regressions.

#![allow(clippy::unwrap_used)]

use super::test_support::*;
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;
use tinytools::{ToolCallOptions, WorkspaceDescriptor};

#[tokio::test]
async fn a_custom_chunk_revision_binds_raw_metadata_and_template_content() {
    let dir = tempfile::tempdir().unwrap();
    custom_manifest(dir.path(), "file", "fn created() {}\n");
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    let entry = tool(&tools, "chunk_inspect");
    let original = success(entry, json!({"manifest_path":"manifest.json"})).await;
    let path = dir.path().join("manifest.json");
    std::fs::write(
        &path,
        format!("{}\n", std::fs::read_to_string(&path).unwrap()),
    )
    .unwrap();
    let reformatted = success(entry, json!({"manifest_path":"manifest.json"})).await;
    assert_ne!(original["chunk_revision"], reformatted["chunk_revision"]);
    assert_eq!(
        original["manifest_revision"],
        reformatted["manifest_revision"]
    );
    std::fs::write(dir.path().join("template.rs.txt"), "fn different() {}\n").unwrap();
    let edited = success(entry, json!({"manifest_path":"manifest.json"})).await;
    assert_ne!(reformatted["chunk_revision"], edited["chunk_revision"]);
    assert_eq!(
        reformatted["manifest_revision"],
        edited["manifest_revision"]
    );
    assert!(
        failure(
            tool(&tools, "chunk_apply"),
            json!({"manifest_path":"manifest.json", "path":"new.rs", "create_new":true,
        "chunk_revision":original["chunk_revision"]})
        )
        .await
        .contains("chunk revision")
    );
    assert!(!dir.path().join("new.rs").exists());
}

#[tokio::test]
async fn malformed_manifests_and_nonportable_templates_fail_before_any_effect() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    for path in [
        "../outside.txt",
        "/absolute.txt",
        "dir\\template.txt",
        "C:template.txt",
        "a//b",
        "",
    ] {
        std::fs::write(dir.path().join("manifest.json"), serde_json::to_vec(&json!({
            "id":"custom.pattern.v1", "summary":"Pattern", "language":"rust", "template_path":path
        })).unwrap()).unwrap();
        assert!(
            failure(
                tool(&tools, "chunk_inspect"),
                json!({"manifest_path":"manifest.json"})
            )
            .await
            .contains("template_path must be a portable, relative path")
        );
    }
    std::fs::write(dir.path().join("manifest.json"), "{not json}").unwrap();
    assert!(
        failure(
            tool(&tools, "chunk_inspect"),
            json!({"manifest_path":"manifest.json"})
        )
        .await
        .contains("manifest")
    );
    assert!(
        failure(
            tool(&tools, "chunk_inspect"),
            json!({"id":"rust.fallible-function.v1","manifest_path":"manifest.json"})
        )
        .await
        .contains("exactly one")
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn manifest_and_template_reads_each_enforce_their_own_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    custom_manifest(dir.path(), "file", "fn created() {}\n");
    let gate = TestGate::new(dir.path());
    let manifest_limited = code_tools_with_options(
        gate.clone(),
        CodeToolOptions {
            max_manifest_bytes: 8,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        failure(
            tool(&manifest_limited, "chunk_inspect"),
            json!({"manifest_path":"manifest.json"})
        )
        .await
        .contains("read budget")
    );
    let template_limited = code_tools_with_options(
        gate,
        CodeToolOptions {
            max_template_bytes: 8,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        failure(
            tool(&template_limited, "chunk_inspect"),
            json!({"manifest_path":"manifest.json"})
        )
        .await
        .contains("read budget")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_template_symlink_cannot_escape_its_manifest_directory_inside_the_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let patterns = dir.path().join("patterns");
    std::fs::create_dir(&patterns).unwrap();
    custom_manifest(&patterns, "file", "fn original() {}\n");
    std::fs::remove_file(patterns.join("template.rs.txt")).unwrap();
    std::fs::write(dir.path().join("outside.rs"), "fn outside() {}\n").unwrap();
    std::os::unix::fs::symlink(
        dir.path().join("outside.rs"),
        patterns.join("template.rs.txt"),
    )
    .unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    assert!(
        failure(
            tool(&tools, "chunk_inspect"),
            json!({"manifest_path":"patterns/manifest.json"})
        )
        .await
        .contains("manifest directory")
    );
}

#[tokio::test]
async fn missing_parent_directories_are_not_created_by_a_failed_chunk_publication() {
    let dir = tempfile::tempdir().unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    assert!(
        failure(
            tool(&tools, "chunk_apply"),
            json!({"id":"rust.fallible-function.v1", "path":"missing/new.rs", "create_new":true,
        "chunk_revision":inspected["chunk_revision"]})
        )
        .await
        .contains("parent directory")
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn an_unchanged_patch_has_a_receipt_but_does_not_consume_an_action() {
    let dir = tempfile::tempdir().unwrap();
    let source = "fn value() -> i32 { 1 }\n";
    std::fs::write(dir.path().join("source.rs"), source).unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    let mut args = patch_args(source);
    args["operation"] = json!("replace_symbol");
    args["code"] = json!(source.trim_end());
    let receipt = success(tool(&tools, "ast_patch"), args).await;
    assert_eq!(receipt["changed"], false);
    assert_eq!(receipt["written"], false);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn chunk_receipt_overflow_never_creates_a_destination() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    let bounded = code_tools_with_options(
        gate.clone(),
        CodeToolOptions {
            max_output_bytes: 160,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        failure(
            tool(&bounded, "chunk_apply"),
            json!({"id":"rust.fallible-function.v1", "path":"new.rs", "create_new":true,
        "chunk_revision":inspected["chunk_revision"]})
        )
        .await
        .contains("output budget")
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn isolated_workspace_writes_land_in_the_child_and_charge_the_original_gate() {
    let parent = tempfile::tempdir().unwrap();
    let child = tempfile::tempdir().unwrap();
    let source = "fn value() -> i32 { 1 }\n";
    std::fs::write(parent.path().join("source.rs"), source).unwrap();
    std::fs::write(child.path().join("source.rs"), source).unwrap();
    let gate = TestGate::new(parent.path());
    let tools = code_tools(gate.clone()).unwrap();
    let context = Workspace(WorkspaceDescriptor::new(child.path().to_path_buf()));
    let result = tool(&tools, "ast_patch")
        .execute_with_context(
            patch_args(source),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        std::fs::read_to_string(parent.path().join("source.rs")).unwrap(),
        source
    );
    assert!(
        std::fs::read_to_string(child.path().join("source.rs"))
            .unwrap()
            .contains('2')
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
}
