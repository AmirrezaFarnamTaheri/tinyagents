//! Behavior tests for the `file_write` tool, driven through a fake [`FsGate`].

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::*;
use crate::filesystem::test_support::{AutonomyLevel, TestGate};

fn test_security(workspace: std::path::PathBuf) -> Arc<TestGate> {
    TestGate::supervised(workspace)
}

fn test_security_with(
    workspace: std::path::PathBuf,
    autonomy: AutonomyLevel,
    max_actions_per_hour: usize,
) -> Arc<TestGate> {
    TestGate::with(workspace, autonomy, max_actions_per_hour)
}

#[test]
fn file_write_name() {
    let tool = FileWriteTool::new(test_security(std::env::temp_dir()));
    assert_eq!(tool.name(), "file_write");
}

#[test]
fn file_write_schema_has_path_and_content() {
    let tool = FileWriteTool::new(test_security(std::env::temp_dir()));
    let schema = tool.parameters_schema();
    assert!(schema["properties"]["path"].is_object());
    assert!(schema["properties"]["content"].is_object());
    let required = schema["required"].as_array().unwrap();
    assert!(required.contains(&json!("path")));
    assert!(required.contains(&json!("content")));
}

#[test]
fn approval_probe_uses_effective_workspace_root() {
    let root = tempfile::tempdir().expect("root");
    let profile_workspace = root.path().join("profiles/alice");
    std::fs::create_dir_all(&profile_workspace).expect("profile workspace");
    std::fs::write(profile_workspace.join("notes.md"), "existing").expect("seed file");

    let tool = FileWriteTool::with_approval_workspace_root(
        test_security(root.path().to_path_buf()),
        profile_workspace,
    );

    assert!(tool.external_effect_with_args(&json!({
        "path": "notes.md",
        "content": "updated"
    })));
}

#[tokio::test]
async fn file_write_creates_file() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "out.txt", "content": "written!"}))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert!(result.output().contains("8 bytes"));

    let content = tokio::fs::read_to_string(dir.join("out.txt"))
        .await
        .unwrap();
    assert_eq!(content, "written!");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_creates_parent_dirs() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_nested");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "a/b/c/deep.txt", "content": "deep"}))
        .await
        .unwrap();
    assert!(!result.is_error);

    let content = tokio::fs::read_to_string(dir.join("a/b/c/deep.txt"))
        .await
        .unwrap();
    assert_eq!(content, "deep");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_overwrites_existing() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_overwrite");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("exist.txt"), "old")
        .await
        .unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "exist.txt", "content": "new"}))
        .await
        .unwrap();
    assert!(!result.is_error);

    let content = tokio::fs::read_to_string(dir.join("exist.txt"))
        .await
        .unwrap();
    assert_eq!(content, "new");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_blocks_path_traversal() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_traversal");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "../../etc/evil", "content": "bad"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(&result.output().contains("not allowed"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_blocks_absolute_path() {
    let tool = FileWriteTool::new(test_security(std::env::temp_dir()));
    let result = tool
        .execute(json!({"path": "/etc/evil", "content": "bad"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(&result.output().contains("not allowed"));
}

#[tokio::test]
async fn file_write_missing_path_param() {
    let tool = FileWriteTool::new(test_security(std::env::temp_dir()));
    let result = tool.execute(json!({"content": "data"})).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn file_write_missing_content_param() {
    let tool = FileWriteTool::new(test_security(std::env::temp_dir()));
    let result = tool.execute(json!({"path": "file.txt"})).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn file_write_empty_content() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_empty");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "empty.txt", "content": ""}))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert!(result.output().contains("0 bytes"));

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[cfg(unix)]
#[tokio::test]
async fn file_write_blocks_symlink_escape() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join("openhuman_test_file_write_symlink_escape");
    let workspace = root.join("workspace");
    let outside = root.join("outside");

    let _ = tokio::fs::remove_dir_all(&root).await;
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::create_dir_all(&outside).await.unwrap();

    symlink(&outside, workspace.join("escape_dir")).unwrap();

    let tool = FileWriteTool::new(test_security(workspace.clone()));
    let result = tool
        .execute(json!({"path": "escape_dir/hijack.txt", "content": "bad"}))
        .await
        .unwrap();

    assert!(result.is_error);
    // SecurityPolicy now blocks symlink escapes at the is_path_allowed
    // layer (#1927) — error becomes "Path not allowed by security
    // policy" rather than the deeper "escapes workspace" message.
    let out = result.output();
    assert!(
        out.contains("escapes workspace") || out.contains("not allowed"),
        "expected escape/not-allowed error, got: {out}"
    );
    assert!(!outside.join("hijack.txt").exists());

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn file_write_blocks_readonly_mode() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_readonly");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security_with(dir.clone(), AutonomyLevel::ReadOnly, 20));
    let result = tool
        .execute(json!({"path": "out.txt", "content": "should-block"}))
        .await
        .unwrap();

    assert!(result.is_error);
    assert!(result.output().contains("read-only"));
    // The readonly block must carry the hard-reject marker so the agent
    // harness recognizes it and halts on a verbatim repeat instead of
    // grinding. Ties this tool's literal to the marker const — the
    // const→detector half lives in `RepeatedToolFailureMiddleware` and is
    // covered by its tests (`crates/openhuman-core/src/agent/tinyagents/middleware.rs`).
    assert!(
        result
            .output()
            .contains(crate::filesystem::test_support::POLICY_BLOCKED_MARKER),
        "file_write readonly block must carry the hard-reject marker: {}",
        result.output()
    );
    assert!(!dir.join("out.txt").exists());

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_blocks_when_rate_limited() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_rate_limited");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security_with(
        dir.clone(),
        AutonomyLevel::Supervised,
        0,
    ));
    let result = tool
        .execute(json!({"path": "out.txt", "content": "should-block"}))
        .await
        .unwrap();

    assert!(result.is_error);
    assert!(result.output().contains("Rate limit exceeded"));
    assert!(!dir.join("out.txt").exists());

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

// ── §5.1 TOCTOU / symlink file write protection tests ────

#[cfg(unix)]
#[tokio::test]
async fn file_write_blocks_symlink_target_file() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join("openhuman_test_file_write_symlink_target");
    let workspace = root.join("workspace");
    let outside = root.join("outside");

    let _ = tokio::fs::remove_dir_all(&root).await;
    tokio::fs::create_dir_all(&workspace).await.unwrap();
    tokio::fs::create_dir_all(&outside).await.unwrap();

    // Create a file outside and symlink to it inside workspace
    tokio::fs::write(outside.join("target.txt"), "original")
        .await
        .unwrap();
    symlink(outside.join("target.txt"), workspace.join("linked.txt")).unwrap();

    let tool = FileWriteTool::new(test_security(workspace.clone()));
    let result = tool
        .execute(json!({"path": "linked.txt", "content": "overwritten"}))
        .await
        .unwrap();

    assert!(result.is_error, "writing through symlink must be blocked");
    // The symlink-safe is_path_allowed check (#1927) blocks at the
    // policy layer before the tool's own symlink-target detection
    // runs; accept either error message.
    let out = result.output();
    assert!(
        out.contains("symlink") || out.contains("not allowed"),
        "error should mention symlink or policy block, got: {out}"
    );

    // Verify original file was not modified
    let content = tokio::fs::read_to_string(outside.join("target.txt"))
        .await
        .unwrap();
    assert_eq!(content, "original", "original file must not be modified");

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn file_write_blocks_null_byte_in_path() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_null");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let result = tool
        .execute(json!({"path": "file\u{0000}.txt", "content": "bad"}))
        .await
        .unwrap();
    assert!(result.is_error, "paths with null bytes must be blocked");

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

/// `file_write` enforces the same 5MB content-size cap as `edit_file` and
/// `grep`'s file caps (`file_read` allows up to 10MB for reads).
#[tokio::test]
async fn file_write_reports_a_refused_write_rather_than_a_silent_success() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_refused");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone())).with_sink(Arc::new(
        crate::filesystem::file_sink::RefusingSink(std::io::ErrorKind::PermissionDenied),
    ));
    let result = tool
        .execute(json!({"path": "f.txt", "content": "abc"}))
        .await
        .unwrap();

    assert!(
        result.is_error,
        "a refused write must surface as an error, not a fabricated success"
    );
    assert!(
        !dir.join("f.txt").exists(),
        "and nothing may be left behind claiming the write happened"
    );

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

#[tokio::test]
async fn file_write_refuses_an_oversized_content_write() {
    let dir = std::env::temp_dir().join("openhuman_test_file_write_oversized");
    let _ = tokio::fs::remove_dir_all(&dir).await;
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let tool = FileWriteTool::new(test_security(dir.clone()));
    let content = "x".repeat(64 * 1024 * 1024);
    let result = tool
        .execute(json!({"path": "huge.txt", "content": content}))
        .await
        .unwrap();
    assert!(
        result.is_error,
        "a 64MB write must be refused by a stated ceiling, not written: {}",
        result.output()
    );

    let _ = tokio::fs::remove_dir_all(&dir).await;
}

// ── Coverage of the guard, symlink and budget branches ──────────────────

use crate::filesystem::test_support::RacyGate;

#[tokio::test]
async fn file_write_refuses_a_racing_budget() {
    let dir = tempfile::tempdir().unwrap();
    let tool = FileWriteTool::new(Arc::new(RacyGate(test_security(dir.path().to_path_buf()))));
    let result = tool
        .execute(json!({"path": "a.txt", "content": "x"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("action budget exhausted"));
}

#[cfg(unix)]
#[tokio::test]
async fn file_write_refuses_to_write_through_an_in_workspace_symlink() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("real.txt"), "real").unwrap();
    std::os::unix::fs::symlink(dir.path().join("real.txt"), dir.path().join("link.txt")).unwrap();
    let tool = FileWriteTool::new(test_security(dir.path().to_path_buf()));
    let result = tool
        .execute(json!({"path": "link.txt", "content": "x"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(
        result
            .output()
            .contains("Refusing to write through symlink")
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("real.txt")).unwrap(),
        "real"
    );
}

#[tokio::test]
async fn file_write_honours_the_file_state_guard_and_records_its_write() {
    crate::file_state::init_global(true);
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().canonicalize().unwrap().join("g.txt");
    std::fs::write(&target, "old").unwrap();
    let tool = FileWriteTool::new(test_security(dir.path().to_path_buf()));
    let agent = format!("fw-agent-{}", dir.path().display());
    let other = format!("fw-other-{}", dir.path().display());

    // Partial read blocks the overwrite.
    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        true,
        std::time::Instant::now(),
    );
    let result = crate::file_state::with_file_state_agent_id(
        agent.clone(),
        tool.execute(json!({"path": "g.txt", "content": "new"})),
    )
    .await
    .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Partial read"));

    // A sibling's later write makes the read stale.
    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    crate::file_state::record_write(&other, target.clone());
    let result = crate::file_state::with_file_state_agent_id(
        agent.clone(),
        tool.execute(json!({"path": "g.txt", "content": "new"})),
    )
    .await
    .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("Stale read"));

    // With a fresh full read the write goes through and is recorded.
    crate::file_state::record_read(
        &agent,
        target.clone(),
        std::time::SystemTime::now(),
        false,
        std::time::Instant::now(),
    );
    let result = crate::file_state::with_file_state_agent_id(
        agent.clone(),
        tool.execute(json!({"path": "g.txt", "content": "new"})),
    )
    .await
    .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "new");
    assert!(crate::file_state::check_stale_read(&agent, &target).is_none());
}

#[tokio::test]
async fn file_write_uses_the_context_workspace_when_one_is_threaded() {
    use crate::filesystem::test_support::WorkspaceContext;
    let home = tempfile::tempdir().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    let tool = FileWriteTool::new(test_security(home.path().to_path_buf()));
    let context = WorkspaceContext::at(isolated.path());
    let result = tool
        .execute_with_context(
            json!({"path": "in_ws.txt", "content": "hi"}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", result.output());
    assert!(isolated.path().join("in_ws.txt").exists());
    assert!(!home.path().join("in_ws.txt").exists());
}
