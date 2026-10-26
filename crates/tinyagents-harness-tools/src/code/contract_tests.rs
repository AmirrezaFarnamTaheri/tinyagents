//! Permission, exposure, schema and strict argument-boundary regressions.

#![allow(clippy::unwrap_used)]

use super::test_support::*;
use super::*;
use serde_json::{Value, json};
use tinytools::{PermissionLevel, ToolExposure};

#[test]
fn deferred_tools_preserve_static_preview_access_and_classify_every_write_argument() {
    let dir = tempfile::tempdir().unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    assert_eq!(
        tools.iter().map(|tool| tool.name()).collect::<Vec<_>>(),
        [
            "ast_outline",
            "ast_symbol",
            "ast_patch",
            "chunk_search",
            "chunk_inspect",
            "chunk_apply"
        ]
    );
    for entry in &tools {
        assert_eq!(entry.permission_level(), PermissionLevel::ReadOnly);
        assert_eq!(entry.exposure(), ToolExposure::Deferred);
        assert_eq!(entry.family(), Some("code"));
        assert_eq!(entry.tags(), ["code", "structural", "pack:code"]);
        assert!(entry.policy().classified);
        let writes = ["ast_patch", "chunk_apply"].contains(&entry.name());
        assert_eq!(entry.policy().side_effects.writes_files, writes);
        assert_eq!(entry.external_effect(), writes);
        assert_eq!(entry.parameters_schema()["additionalProperties"], false);
        for value in [
            Value::Null,
            json!(false),
            json!("true"),
            json!(1),
            json!({}),
        ] {
            let args = json!({"dry_run":value});
            assert_eq!(
                entry.permission_level_with_args(&args),
                if writes {
                    PermissionLevel::Write
                } else {
                    PermissionLevel::ReadOnly
                }
            );
            assert_eq!(entry.external_effect_with_args(&args), writes);
            assert_eq!(entry.is_concurrency_safe(&args), !writes);
        }
        assert_eq!(
            entry.permission_level_with_args(&json!({"dry_run":true})),
            PermissionLevel::ReadOnly
        );
        assert!(!entry.external_effect_with_args(&json!({"dry_run":true})));
        assert!(entry.is_concurrency_safe(&json!({"dry_run":true})));
    }
}

#[test]
fn rejects_invalid_host_bounds_before_constructing_tools() {
    let dir = tempfile::tempdir().unwrap();
    let invalid = [
        CodeToolOptions {
            max_argument_bytes: 0,
            ..Default::default()
        },
        CodeToolOptions {
            max_manifest_bytes: 1_048_577,
            ..Default::default()
        },
        CodeToolOptions {
            max_template_bytes: 0,
            ..Default::default()
        },
        CodeToolOptions {
            max_output_bytes: 16_777_217,
            ..Default::default()
        },
        CodeToolOptions {
            max_page_items: 0,
            ..Default::default()
        },
        CodeToolOptions {
            max_page_lines: 4097,
            ..Default::default()
        },
        CodeToolOptions {
            limits: tinytools_code::CodeLimits {
                max_source_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    ];
    for options in invalid {
        assert!(code_tools_with_options(TestGate::new(dir.path()), options).is_err());
    }
}

#[tokio::test]
async fn malformed_or_oversized_arguments_fail_without_consuming_a_write_budget() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::new(dir.path());
    let tools = code_tools_with_options(
        gate.clone(),
        CodeToolOptions {
            max_argument_bytes: 512,
            max_output_bytes: 31,
            ..Default::default()
        },
    )
    .unwrap();
    for args in [
        json!(null),
        json!({"path":"x","dry_run":"true"}),
        json!({"unknown":"分析".repeat(300)}),
    ] {
        let result = tool(&tools, "ast_patch").execute(args).await.unwrap();
        assert!(result.is_error);
        assert!(result.output().len() <= 31);
    }
    assert_eq!(gate.state.used.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn output_pages_and_error_truncation_preserve_utf8_and_report_continuations() {
    let error = response::bounded_error("分析abc", 4);
    assert_eq!(error.output(), "分");
    let records = response::records(&[1, 2, 3], usize::MAX, 2);
    assert_eq!(records["items"], json!([]));
    assert_eq!(records["next_offset"], Value::Null);
    let page = response::source_page("one\ntwo\nthree", 2, 1).unwrap();
    assert_eq!(page["text"], "two\n");
    assert_eq!(page["next_start_line"], 3);
    assert_eq!(page["truncated"], true);
    assert!(response::source_page("", 0, 1).is_err());
}

#[tokio::test]
async fn argument_budget_counts_serialized_utf8_and_escape_bytes_at_the_exact_boundary() {
    let dir = tempfile::tempdir().unwrap();
    // The compact object contributes twelve bytes around each query value.
    for (query, bytes) in [("a", 13), ("\n", 14), ("分", 15), ("\"\\", 16), ("\0", 18)] {
        let args = json!({"query":query});
        for (maximum, accepted) in [(bytes, true), (bytes - 1, false)] {
            let gate = TestGate::new(dir.path());
            let tools = code_tools_with_options(
                gate.clone(),
                CodeToolOptions {
                    max_argument_bytes: maximum,
                    ..Default::default()
                },
            )
            .unwrap();
            let result = tool(&tools, "chunk_search")
                .execute(args.clone())
                .await
                .unwrap();
            assert_eq!(
                !result.is_error, accepted,
                "query {query:?}, limit {maximum}"
            );
            if !accepted {
                assert!(result.output().contains("arguments exceed the byte budget"));
            }
            assert_eq!(gate.state.used.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
