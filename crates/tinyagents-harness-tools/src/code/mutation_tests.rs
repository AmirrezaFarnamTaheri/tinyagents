//! Preview, revision, typed-chunk and actual filesystem mutation regressions.

#![allow(clippy::unwrap_used)]

use super::test_support::*;
use super::*;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use tinytools_code::revision;

const SOURCE: &str = "fn value() -> i32 { 1 }\n";

#[tokio::test]
async fn literal_previews_work_in_read_only_mode_and_do_not_consume_actions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    gate.state.read_only.store(true, Ordering::SeqCst);
    let tools = code_tools(gate.clone()).unwrap();
    let mut args = patch_args(SOURCE);
    args["dry_run"] = json!(true);
    let preview = success(tool(&tools, "ast_patch"), args.clone()).await;
    assert_eq!(preview["written"], false);
    assert_eq!(preview["dry_run"], true);
    assert_eq!(preview["changed"], true);
    assert_eq!(preview["before_revision"], revision(SOURCE));
    assert_ne!(preview["revision"], revision(SOURCE));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), SOURCE);
    args["dry_run"] = json!(false);
    assert!(
        failure(tool(&tools, "ast_patch"), args.clone())
            .await
            .contains("policy-blocked")
    );
    for invalid in [json!("true"), Value::Null, json!(1)] {
        args["dry_run"] = invalid;
        assert!(
            failure(tool(&tools, "ast_patch"), args.clone())
                .await
                .contains("arguments")
        );
    }
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn a_real_patch_requires_the_current_revision_and_records_the_actual_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    crate::fs::file_state::init_global(true);
    let resolved = path.canonicalize().unwrap();
    crate::fs::file_state::record_read(
        "code-parent",
        resolved.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    let receipt = crate::fs::file_state::with_file_state_agent_id(
        "code-writer".into(),
        success(tool(&tools, "ast_patch"), patch_args(SOURCE)),
    )
    .await;
    let changed = std::fs::read_to_string(&path).unwrap();
    assert!(changed.contains('2'));
    assert_eq!(receipt["written"], true);
    assert_eq!(receipt["dry_run"], false);
    assert_eq!(receipt["revision"], revision(&changed));
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
    assert!(crate::fs::file_state::check_stale_read("code-parent", &resolved).is_some());
    assert!(
        failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
            .await
            .contains("revision")
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), changed);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_edits_from_one_snapshot_cannot_overwrite_each_other() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("source.rs"), SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    let entry = tool(&tools, "ast_patch");
    let mut second = patch_args(SOURCE);
    second["code"] = json!("3");
    let (first, second) = tokio::join!(entry.execute(patch_args(SOURCE)), entry.execute(second));
    assert_ne!(first.unwrap().is_error, second.unwrap().is_error);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn chunks_require_inspected_content_and_create_without_clobbering() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    let mut args = json!({"id":"rust.fallible-function.v1", "path":"new.rs", "create_new":true,
        "chunk_revision":inspected["chunk_revision"], "bindings":{"parameters":{"name":"created"}}, "dry_run":true});
    let preview = success(tool(&tools, "chunk_apply"), args.clone()).await;
    assert_eq!(preview["before_revision"], Value::Null);
    assert_eq!(preview["written"], false);
    assert!(
        preview["source"]["text"]
            .as_str()
            .unwrap()
            .contains("fn created")
    );
    assert!(!dir.path().join("new.rs").exists());
    args["dry_run"] = json!(false);
    let receipt = success(tool(&tools, "chunk_apply"), args.clone()).await;
    assert_eq!(receipt["written"], true);
    let original = std::fs::read_to_string(dir.path().join("new.rs")).unwrap();
    assert_eq!(receipt["revision"], revision(&original));
    assert!(
        failure(tool(&tools, "chunk_apply"), args.clone())
            .await
            .contains("existing")
    );
    args["path"] = json!("another.rs");
    args["chunk_revision"] = json!("a".repeat(64));
    assert!(
        failure(tool(&tools, "chunk_apply"), args)
            .await
            .contains("chunk revision")
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("new.rs")).unwrap(),
        original
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn chunks_edit_current_files_bodies_and_members_through_the_same_revision_guard() {
    let dir = tempfile::tempdir().unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    std::fs::write(dir.path().join("source.rs"), SOURCE).unwrap();
    let file = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    let receipt = success(
        tool(&tools, "chunk_apply"),
        json!({"id":"rust.fallible-function.v1", "path":"source.rs",
        "chunk_revision":file["chunk_revision"], "expected_revision":revision(SOURCE)}),
    )
    .await;
    assert_eq!(receipt["written"], true);
    let body_source = "fn sum(left: u32, right: u32) -> Result<u32, String> { Ok(0) }";
    std::fs::write(dir.path().join("source.rs"), body_source).unwrap();
    let body = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.checked-add.v1"}),
    )
    .await;
    let receipt = success(
        tool(&tools, "chunk_apply"),
        json!({"id":"rust.checked-add.v1", "path":"source.rs", "selector":"sum",
        "chunk_revision":body["chunk_revision"], "expected_revision":revision(body_source)}),
    )
    .await;
    assert!(
        receipt["source"]["text"]
            .as_str()
            .unwrap()
            .contains("checked_add")
    );
    custom_manifest(dir.path(), "member", "fn added(&self) {}\n");
    let member_source = "struct Thing;\nimpl Thing {}\n";
    std::fs::write(dir.path().join("source.rs"), member_source).unwrap();
    let outline = success(tool(&tools, "ast_outline"), json!({"path":"source.rs"})).await;
    let selected = outline["symbols"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["kind"] == "impl_item")
        .unwrap()["id"]
        .clone();
    let member = success(
        tool(&tools, "chunk_inspect"),
        json!({"manifest_path":"manifest.json"}),
    )
    .await;
    let receipt = success(
        tool(&tools, "chunk_apply"),
        json!({"manifest_path":"manifest.json", "path":"source.rs", "selector":selected,
        "chunk_revision":member["chunk_revision"], "expected_revision":revision(member_source)}),
    )
    .await;
    assert!(
        receipt["source"]["text"]
            .as_str()
            .unwrap()
            .contains("fn added")
    );
}

#[tokio::test]
async fn inconsistent_chunk_destinations_and_invalid_bindings_never_write() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools(gate.clone()).unwrap();
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    let base = json!({"id":"rust.fallible-function.v1", "path":"source.rs", "chunk_revision":inspected["chunk_revision"]});
    assert!(
        failure(tool(&tools, "chunk_apply"), base.clone())
            .await
            .contains("expected_revision")
    );
    let mut args = base;
    args["create_new"] = json!(true);
    args["expected_revision"] = json!(revision(SOURCE));
    assert!(
        failure(tool(&tools, "chunk_apply"), args.clone())
            .await
            .contains("without expected_revision")
    );
    args.as_object_mut().unwrap().remove("expected_revision");
    args["bindings"] = json!({"parameters":{"name":"not an identifier"}});
    assert!(
        failure(tool(&tools, "chunk_apply"), args.clone())
            .await
            .contains("identifier parameter violates its declared kind")
    );
    args["bindings"] = json!({});
    args["selector"] = json!("unnecessary");
    assert!(
        failure(tool(&tools, "chunk_apply"), args)
            .await
            .contains("selector")
    );
    let member = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.public-field.v1"}),
    )
    .await;
    assert!(
        failure(
            tool(&tools, "chunk_apply"),
            json!({"id":"rust.public-field.v1", "path":"new.rs", "create_new":true,
        "chunk_revision":member["chunk_revision"]})
        )
        .await
        .contains("complete-file")
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_receipts_are_refused_before_any_write_or_budget_consumption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools_with_options(
        gate.clone(),
        CodeToolOptions {
            max_output_bytes: 160,
            ..Default::default()
        },
    )
    .unwrap();
    let error = failure(tool(&tools, "ast_patch"), patch_args(SOURCE)).await;
    assert!(error.contains("output budget"));
    assert!(error.len() <= 160);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), SOURCE);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}
