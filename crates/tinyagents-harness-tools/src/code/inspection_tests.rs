//! Real filesystem inspection, source paging and catalog-boundary regressions.

#![allow(clippy::unwrap_used)]

use super::test_support::*;
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;
use tinytools::{ToolCallOptions, WorkspaceDescriptor};

#[tokio::test]
async fn outline_and_symbol_pages_identify_the_exact_source_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let source = "fn first() -> i32 {\n    1\n}\nfn second() {}\n";
    std::fs::write(dir.path().join("source.rs"), source).unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    let outline = success(
        tool(&tools, "ast_outline"),
        json!({"path":"source.rs","limit":1,"line_count":1}),
    )
    .await;
    assert_eq!(outline["revision"], tinytools_code::revision(source));
    assert_eq!(outline["symbols"]["items"][0]["name"], "first");
    assert_eq!(outline["symbols"]["next_offset"], 1);
    assert_eq!(outline["cache_status"], "fresh");
    let second = success(
        tool(&tools, "ast_outline"),
        json!({"path":"source.rs","offset":1,"limit":1}),
    )
    .await;
    assert_eq!(second["symbols"]["items"][0]["name"], "second");
    assert_eq!(second["cache_status"], "cached");
    let symbol = success(tool(&tools, "ast_symbol"), json!({"path":"source.rs","selector":outline["symbols"]["items"][0]["id"],"start_line":2,"line_count":1})).await;
    assert_eq!(symbol["source"]["text"], "    1\n");
    assert_eq!(symbol["source"]["next_start_line"], 3);
    assert_eq!(symbol["symbol"]["name"], "first");
    std::fs::write(dir.path().join("source.rs"), source.replace('1', "2")).unwrap();
    let changed = success(tool(&tools, "ast_outline"), json!({"path":"source.rs"})).await;
    assert_ne!(changed["revision"], outline["revision"]);
    assert_eq!(changed["cache_status"], "incremental");
}

#[tokio::test]
async fn outline_levels_preserve_metadata_and_default_to_paged_folded_source() {
    let dir = tempfile::tempdir().unwrap();
    let source = "fn first() -> i32 {\n    1\n}\nfn second() {}\n";
    std::fs::write(dir.path().join("source.rs"), source).unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    let entry = tool(&tools, "ast_outline");
    let metadata = success(entry, json!({"path":"source.rs","level":0,"limit":1})).await;
    let default = success(entry, json!({"path":"source.rs","limit":1,"line_count":1})).await;
    let explicit = success(
        entry,
        json!({"path":"source.rs","level":1,"limit":1,"line_count":1}),
    )
    .await;
    assert!(metadata.get("skeleton").is_none());
    assert_eq!(metadata["revision"], tinytools_code::revision(source));
    assert_eq!(metadata["symbols"], default["symbols"]);
    assert_eq!(metadata["symbols"]["next_offset"], 1);
    assert_eq!(default["skeleton"], explicit["skeleton"]);
    assert_eq!(default["skeleton"]["start_line"], 1);
    assert_eq!(default["skeleton"]["next_start_line"], 2);
    assert_eq!(
        entry.parameters_schema()["properties"]["level"]["default"],
        1
    );
    assert_eq!(
        entry.parameters_schema()["properties"]["level"]["enum"],
        json!([0, 1])
    );
}

#[tokio::test]
async fn outline_rejects_unsupported_or_malformed_levels_before_reading_a_path() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    gate.state.denied.store(true, Ordering::SeqCst);
    let tools = code_tools(gate.clone()).unwrap();
    for level in [
        json!(2),
        json!(255),
        json!(-1),
        json!(256),
        json!(1.5),
        json!("1"),
        json!(true),
        json!(null),
    ] {
        let error = failure(
            tool(&tools, "ast_outline"),
            json!({"path":"missing.rs","level":level}),
        )
        .await;
        assert!(
            error.contains("outline level") || error.contains("invalid structural tool arguments")
        );
        assert!(!error.contains("policy-blocked"));
    }
    assert!(
        failure(
            tool(&tools, "ast_outline"),
            json!({"path":"missing.rs","level":0,"start_line":0})
        )
        .await
        .contains("start_line")
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn chunk_search_defaults_to_five_bounded_results_with_explicit_continuation() {
    let dir = tempfile::tempdir().unwrap();
    for maximum in [2, 32] {
        let tools = code_tools_with_options(
            TestGate::new(dir.path()),
            CodeToolOptions {
                max_page_items: maximum,
                ..Default::default()
            },
        )
        .unwrap();
        let entry = tool(&tools, "chunk_search");
        let first = success(entry, json!({})).await;
        let default_limit = maximum.min(5);
        assert_eq!(
            first["chunks"]["items"].as_array().unwrap().len(),
            default_limit
        );
        assert_eq!(first["chunks"]["total"], 12);
        assert_eq!(first["chunks"]["next_offset"], default_limit);
        assert_eq!(
            entry.parameters_schema()["properties"]["limit"]["default"],
            default_limit
        );
        let next = success(entry, json!({"offset":10,"limit":maximum})).await;
        assert_eq!(next["chunks"]["items"].as_array().unwrap().len(), 2);
        assert_eq!(next["chunks"]["next_offset"], json!(null));
        let explicit = success(entry, json!({"limit":maximum})).await;
        assert_eq!(
            explicit["chunks"]["items"].as_array().unwrap().len(),
            maximum.min(12)
        );
        for invalid in [0, maximum + 1] {
            assert!(
                failure(entry, json!({"limit":invalid}))
                    .await
                    .contains("page size")
            );
        }
    }
}

#[tokio::test]
async fn denied_missing_binary_large_and_non_file_inputs_are_model_visible_errors() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("outside.rs"), "fn outside() {}").unwrap();
    std::fs::write(dir.path().join("binary.rs"), [0xff, 0xfe]).unwrap();
    std::fs::write(dir.path().join("large.rs"), " ".repeat(64)).unwrap();
    std::fs::create_dir(dir.path().join("folder.rs")).unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools_with_options(
        gate.clone(),
        CodeToolOptions {
            limits: tinytools_code::CodeLimits {
                max_source_bytes: 32,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    for path in [
        "missing.rs".to_owned(),
        "binary.rs".into(),
        "large.rs".into(),
        "folder.rs".into(),
        outside.path().join("outside.rs").display().to_string(),
        "../outside.rs".into(),
    ] {
        assert_ne!(
            failure(tool(&tools, "ast_outline"), json!({"path":path})).await,
            ""
        );
    }
    assert!(
        failure(tool(&tools, "ast_outline"), json!({"path":"unknown"}))
            .await
            .contains("explicit")
    );
    assert!(
        failure(
            tool(&tools, "ast_outline"),
            json!({"path":"source.rs","limit":0})
        )
        .await
        .contains("page size")
    );
    assert!(
        failure(
            tool(&tools, "ast_symbol"),
            json!({"path":"source.rs","selector":"x","line_count":81})
        )
        .await
        .contains("page size")
    );
    gate.state.denied.store(true, Ordering::SeqCst);
    assert!(
        failure(tool(&tools, "ast_outline"), json!({"path":"binary.rs"}))
            .await
            .contains("policy-blocked")
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_trusted_run_workspace_scopes_reads_without_mutating_the_parent_gate() {
    let parent = tempfile::tempdir().unwrap();
    let child = tempfile::tempdir().unwrap();
    std::fs::write(parent.path().join("source.rs"), "fn parent() {}").unwrap();
    std::fs::write(child.path().join("source.rs"), "fn child() {}").unwrap();
    let gate = TestGate::new(parent.path());
    let tools = code_tools(gate.clone()).unwrap();
    let context = Workspace(WorkspaceDescriptor::new(child.path().to_path_buf()));
    let result = tool(&tools, "ast_outline")
        .execute_with_context(
            json!({"path":"source.rs"}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert!(result.output().contains("child"));
    let ordinary = success(tool(&tools, "ast_outline"), json!({"path":"source.rs"})).await;
    assert_eq!(ordinary["symbols"]["items"][0]["name"], "parent");
    assert_eq!(gate.action_dir(), parent.path());
}

#[tokio::test]
async fn bundled_catalog_search_and_inspection_are_paged_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    gate.state.denied.store(true, Ordering::SeqCst);
    let tools = code_tools(gate.clone()).unwrap();
    let first = success(tool(&tools, "chunk_search"), json!({"limit":1})).await;
    assert_eq!(first["chunks"]["total"], 12);
    assert_eq!(first["chunks"]["next_offset"], 1);
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1","limit":1}),
    )
    .await;
    assert_eq!(inspected["parameters"]["items"][0]["name"], "name");
    assert_eq!(inspected["parameters"]["next_offset"], 1);
    assert_eq!(inspected["chunk_revision"].as_str().unwrap().len(), 64);
    assert!(inspected.get("source").is_none());
    assert!(
        failure(tool(&tools, "chunk_inspect"), json!({}))
            .await
            .contains("exactly one")
    );
    assert!(
        failure(
            tool(&tools, "chunk_inspect"),
            json!({"id":"no-such-pattern"})
        )
        .await
        .contains("no-such-pattern")
    );
    assert!(
        failure(
            tool(&tools, "chunk_search"),
            json!({"query":"x".repeat(1025)})
        )
        .await
        .contains("1024")
    );
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 0);
}
