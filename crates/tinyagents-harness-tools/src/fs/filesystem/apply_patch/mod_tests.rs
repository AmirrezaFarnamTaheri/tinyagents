//! Behavior tests for the `apply_patch` tool, driven through a fake [`FsGate`].

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::TestGate;

fn test_security(workspace: std::path::PathBuf) -> Arc<TestGate> {
    TestGate::supervised(workspace)
}

#[test]
fn apply_patch_name() {
    let tool = ApplyPatchTool::new(test_security(std::env::temp_dir()));
    assert_eq!(tool.name(), "apply_patch");
}

#[tokio::test]
async fn apply_patch_applies_multiple_edits() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_multi");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "alpha\nbravo")
        .await
        .unwrap();
    tokio::fs::write(dir.join("b.txt"), "one two")
        .await
        .unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "a.txt", "old_string": "alpha", "new_string": "ALPHA" },
                { "path": "b.txt", "old_string": "two", "new_string": "TWO" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    let a = tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap();
    let b = tokio::fs::read_to_string(dir.join("b.txt")).await.unwrap();
    assert_eq!(a, "ALPHA\nbravo");
    assert_eq!(b, "one TWO");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_atomic_on_validation_failure() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_atomic");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "alpha").await.unwrap();
    tokio::fs::write(dir.join("b.txt"), "bravo").await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    // Second edit will fail (no match) — first must NOT be applied.
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "a.txt", "old_string": "alpha", "new_string": "ALPHA" },
                { "path": "b.txt", "old_string": "missing", "new_string": "x" }
            ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    let a = tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap();
    assert_eq!(a, "alpha", "atomic: first edit must not be persisted");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_chained_edits_same_file() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_chain");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "one two three")
        .await
        .unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "a.txt", "old_string": "one", "new_string": "ONE" },
                { "path": "a.txt", "old_string": "two", "new_string": "TWO" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    let updated = tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap();
    assert_eq!(updated, "ONE TWO three");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_rejects_empty_edits() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_empty");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool.execute(json!({"edits": []})).await.unwrap();
    assert!(result.is_error);

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_rejects_traversal() {
    let tool = ApplyPatchTool::new(test_security(std::env::temp_dir()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "../etc/passwd", "old_string": "x", "new_string": "y" }
            ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("not allowed"));
}

// -- create mode --------------------------------------------------------------
//
// Empty `old_string` + a path that does not exist = create. Before this the
// tool could only edit, so an agent asked to produce a document had no route:
// the life-scenario `meal-plan` run left two 1-byte files containing `x`,
// placeholders it made with `shell` purely so `apply_patch` had something to
// patch.

#[tokio::test]
async fn apply_patch_creates_a_new_file_from_an_empty_old_string() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_create");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "out/plan.md", "old_string": "", "new_string": "# Plan\nday one\n" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    let written = tokio::fs::read_to_string(dir.join("out/plan.md"))
        .await
        .expect("the file (and its parent) must have been created");
    assert_eq!(written, "# Plan\nday one\n");
    assert!(result.output().contains("created"), "{}", result.output());
}

#[tokio::test]
async fn apply_patch_takes_a_top_level_path_as_the_default_for_every_edit() {
    // The shape a model writes for "one file, several edits": the path once,
    // at the top level. Each such call used to be rejected for a missing
    // `edits[0].path`, and four in a row halted a run with 26 minutes left.
    let dir = std::env::temp_dir().join("openhuman_test_patch_top_level_path");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.c"), "int x = 1;\nint y = 2;\n")
        .await
        .unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "path": "a.c",
            "edits": [
                { "old_string": "int x = 1;", "new_string": "int x = 10;" },
                { "old_string": "int y = 2;", "new_string": "int y = 20;" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        tokio::fs::read_to_string(dir.join("a.c")).await.unwrap(),
        "int x = 10;\nint y = 20;\n"
    );
}

#[tokio::test]
async fn apply_patch_rejects_malformed_edit_path_instead_of_using_default() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_malformed_path");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("default.txt"), "original")
        .await
        .unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "path": "default.txt",
            "edits": [{ "path": null, "old_string": "original", "new_string": "changed" }]
        }))
        .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("`path` must be a string")
    );
    assert_eq!(
        tokio::fs::read_to_string(dir.join("default.txt"))
            .await
            .unwrap(),
        "original"
    );
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_rejects_a_malformed_top_level_path() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_malformed_top_level_path");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "original")
        .await
        .unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "path": 123,
            "edits": [{ "path": "a.txt", "old_string": "original", "new_string": "changed" }]
        }))
        .await;

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("top-level `path` must be a string")
    );
    assert_eq!(
        tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap(),
        "original"
    );
    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn apply_patch_lets_an_edits_own_path_win_over_the_top_level_one() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_top_level_path_override");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "alpha").await.unwrap();
    tokio::fs::write(dir.join("b.txt"), "bravo").await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "path": "a.txt",
            "edits": [
                { "old_string": "alpha", "new_string": "ALPHA" },
                { "path": "b.txt", "old_string": "bravo", "new_string": "BRAVO" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap(),
        "ALPHA"
    );
    assert_eq!(
        tokio::fs::read_to_string(dir.join("b.txt")).await.unwrap(),
        "BRAVO"
    );
}

#[tokio::test]
async fn apply_patch_names_both_ways_to_give_a_path_when_neither_is_given() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_no_path_anywhere");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let err = tool
        .execute(json!({
            "edits": [ { "old_string": "a", "new_string": "b" } ]
        }))
        .await
        .expect_err("no path anywhere is an argument error");
    let text = err.to_string();
    assert!(text.contains("edit[0]: missing `path`"), "{text}");
    assert!(text.contains("one top-level `path`"), "{text}");
}

#[tokio::test]
async fn apply_patch_refuses_an_empty_old_string_on_an_existing_file() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_create_existing");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "alpha").await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [{ "path": "a.txt", "old_string": "", "new_string": "overwritten" }]
        }))
        .await
        .unwrap();
    assert!(result.is_error, "{}", result.output());
    // Unchanged — "replace nothing" must never become "replace everything".
    let a = tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap();
    assert_eq!(a, "alpha");
}

#[tokio::test]
async fn apply_patch_mixes_a_create_and_an_edit_in_one_batch() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_create_mixed");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("a.txt"), "alpha").await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "a.txt", "old_string": "alpha", "new_string": "ALPHA" },
                { "path": "b.txt", "old_string": "", "new_string": "bravo" }
            ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        tokio::fs::read_to_string(dir.join("a.txt")).await.unwrap(),
        "ALPHA"
    );
    assert_eq!(
        tokio::fs::read_to_string(dir.join("b.txt")).await.unwrap(),
        "bravo"
    );
}

#[tokio::test]
async fn apply_patch_refuses_two_creates_for_the_same_path() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_create_dup");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [
                { "path": "c.txt", "old_string": "", "new_string": "first" },
                { "path": "c.txt", "old_string": "", "new_string": "second" }
            ]
        }))
        .await
        .unwrap();
    assert!(result.is_error, "{}", result.output());
    assert!(
        !dir.join("c.txt").exists(),
        "a rejected batch must write nothing"
    );
}

#[tokio::test]
async fn apply_patch_create_still_obeys_the_path_policy() {
    let dir = std::env::temp_dir().join("openhuman_test_patch_create_escape");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = ApplyPatchTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({
            "edits": [{ "path": "../escaped.md", "old_string": "", "new_string": "nope" }]
        }))
        .await
        .unwrap();
    assert!(result.is_error, "{}", result.output());
}

// ── Coverage of limits, budgets, guards, symlinks and write failures ────

use crate::filesystem::test_support::{AutonomyLevel, RacyGate, WorkspaceContext};

fn one_edit(path: &str, old: &str, new: &str) -> serde_json::Value {
    json!({"edits": [{"path": path, "old_string": old, "new_string": new}]})
}

#[tokio::test]
async fn apply_patch_rejects_too_many_edits() {
    let dir = tempfile::tempdir().unwrap();
    let edits: Vec<_> = (0..=MAX_EDITS)
        .map(|_| json!({"path": "a.txt", "old_string": "a", "new_string": "b"}))
        .collect();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(json!({"edits": edits})).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Too many edits"));
}

#[tokio::test]
async fn apply_patch_enforces_autonomy_and_budgets() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "x").unwrap();
    let args = one_edit("a.txt", "x", "y");

    let read_only = ApplyPatchTool::new(TestGate::with(
        dir.path().to_path_buf(),
        AutonomyLevel::ReadOnly,
        100,
    ));
    let result = read_only.execute(args.clone()).await.unwrap();
    assert!(result.output().contains("autonomy is read-only"));

    let limited = ApplyPatchTool::new(TestGate::with(
        dir.path().to_path_buf(),
        AutonomyLevel::Supervised,
        0,
    ));
    let result = limited.execute(args.clone()).await.unwrap();
    assert!(
        result
            .output()
            .contains("too many actions in the last hour")
    );

    let racy = ApplyPatchTool::new(Arc::new(RacyGate(test_security(dir.path().to_path_buf()))));
    let result = racy.execute(args).await.unwrap();
    assert!(result.output().contains("action budget exhausted"));
}

#[tokio::test]
async fn apply_patch_resolves_an_absolute_create_target() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("abs_new.txt");
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool
        .execute(one_edit(target.to_str().unwrap(), "", "created"))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(std::fs::read_to_string(target).unwrap(), "created");
}

#[cfg(unix)]
#[tokio::test]
async fn apply_patch_refuses_to_edit_through_a_symlink() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real.txt"), "x").unwrap();
    std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt")).unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(one_edit("link.txt", "x", "y")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("refusing to edit through symlink"));
}

#[tokio::test]
async fn apply_patch_reports_unresolvable_and_unreadable_targets() {
    let dir = tempfile::tempdir().unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool
        .execute(one_edit("absent.txt", "x", "y"))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().starts_with("edit[0]: "));
    assert!(result.output().contains("Failed to resolve"));

    std::fs::write(dir.path().join("bin.dat"), [0xff_u8, 0xfe]).unwrap();
    let result = tool.execute(one_edit("bin.dat", "x", "y")).await.unwrap();
    assert!(result.output().contains("failed to read bin.dat"));
}

#[tokio::test]
async fn apply_patch_reports_a_create_whose_parent_cannot_be_made() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file.txt"), "x").unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool
        .execute(one_edit("file.txt/child.txt", "", "body"))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(
        result
            .output()
            .contains("failed to create parent of file.txt/child.txt")
    );
}

#[tokio::test]
async fn apply_patch_refuses_an_oversized_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("big.txt")).unwrap();
    file.set_len(MAX_FILE_BYTES + 1).unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(one_edit("big.txt", "x", "y")).await.unwrap();
    assert!(result.output().contains("file too large"));
}

#[tokio::test]
async fn apply_patch_needs_replace_all_for_repeated_matches() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("r.txt"), "a a a").unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let result = tool.execute(one_edit("r.txt", "a", "b")).await.unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("matches 3 times"));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("r.txt")).unwrap(),
        "a a a"
    );

    let result = tool
        .execute(json!({"edits": [
            {"path": "r.txt", "old_string": "a", "new_string": "b", "replace_all": true}
        ]}))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("r.txt")).unwrap(),
        "b b b"
    );
}

#[tokio::test]
async fn apply_patch_uses_the_context_workspace_when_one_is_threaded() {
    let home = tempfile::tempdir().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    std::fs::write(isolated.path().join("w.txt"), "x").unwrap();
    let tool = ApplyPatchTool::new(test_security(home.path().to_path_buf()));
    let context = WorkspaceContext::at(isolated.path());
    let result = tool
        .execute_with_context(
            one_edit("w.txt", "x", "y"),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(
        std::fs::read_to_string(isolated.path().join("w.txt")).unwrap(),
        "y"
    );
}

#[tokio::test]
async fn apply_patch_honours_the_file_state_guard_and_records_writes() {
    crate::file_state::init_global(true);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().canonicalize().unwrap().join("g.txt");
    std::fs::write(&target, "x").unwrap();
    let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
    let agent = format!("ap-agent-{}", dir.path().display());
    let other = format!("ap-other-{}", dir.path().display());
    let run = |agent: String| {
        crate::file_state::with_file_state_agent_id(
            agent,
            tool.execute(one_edit("g.txt", "x", "y")),
        )
    };

    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        true,
        std::time::Instant::now(),
    );
    assert!(
        run(agent.clone())
            .await
            .unwrap()
            .output()
            .contains("Partial read")
    );

    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    crate::file_state::record_write(&other, target.clone());
    assert!(
        run(agent.clone())
            .await
            .unwrap()
            .output()
            .contains("Stale read")
    );

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

/// A create whose file name exceeds the OS limit passes every check and then
/// fails at write time. Whichever order the batch writes in, no file from the
/// failed batch may be left changed, and the error must say it was restored.
#[tokio::test]
async fn apply_patch_restores_earlier_writes_when_a_later_write_fails() {
    let long_name = "n".repeat(300);
    let mut saw_restore = false;
    for _ in 0..40 {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("keep.txt"), "orig").unwrap();
        let tool = ApplyPatchTool::new(test_security(dir.path().to_path_buf()));
        let result = tool
            .execute(json!({"edits": [
                {"path": "keep.txt", "old_string": "orig", "new_string": "changed"},
                {"path": "fresh.txt", "old_string": "", "new_string": "new"},
                {"path": long_name, "old_string": "", "new_string": "boom"},
            ]}))
            .await
            .unwrap();
        assert!(result.is_error, "{}", result.output());
        assert!(result.output().contains("Failed to write"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("keep.txt")).unwrap(),
            "orig"
        );
        assert!(!dir.path().join("fresh.txt").exists());
        saw_restore |= result.output().contains("restored from snapshot");
    }
    assert!(
        saw_restore,
        "no iteration wrote a file before the failing one"
    );
}
