#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::items_after_statements,
    clippy::single_match_else,
    clippy::single_match
)]

use super::*;

fn make_tool(dir: &std::path::Path) -> UpdateMemoryMdTool {
    UpdateMemoryMdTool::new(dir.to_path_buf())
}

#[tokio::test]
async fn append_creates_file_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "MEMORY.md",
            "action": "append",
            "content": "first note"
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{:?}", result.output());
    let text = std::fs::read_to_string(dir.path().join("MEMORY.md")).unwrap();
    assert!(text.contains("first note"));
}

#[tokio::test]
async fn append_adds_to_existing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MEMORY.md");
    std::fs::write(&path, "existing\n").unwrap();
    let tool = make_tool(dir.path());
    tool.execute(json!({
        "file": "MEMORY.md",
        "action": "append",
        "content": "second note"
    }))
    .await
    .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("existing"));
    assert!(text.contains("second note"));
}

#[tokio::test]
async fn replace_section_overwrites_body() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MEMORY.md");
    std::fs::write(&path, "## Lessons\nold body\n## Other\nkept\n").unwrap();
    let tool = make_tool(dir.path());
    tool.execute(json!({
        "file": "MEMORY.md",
        "action": "replace_section",
        "section_title": "Lessons",
        "content": "new body"
    }))
    .await
    .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("new body"), "new body missing: {text}");
    assert!(
        !text.contains("old body"),
        "old body should be gone: {text}"
    );
    assert!(text.contains("## Other"), "other section missing: {text}");
    assert!(text.contains("kept"), "other section body missing: {text}");
}

#[tokio::test]
async fn replace_section_appends_when_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("SKILL.md");
    std::fs::write(&path, "# Header\n").unwrap();
    let tool = make_tool(dir.path());
    tool.execute(json!({
        "file": "SKILL.md",
        "action": "replace_section",
        "section_title": "New Section",
        "content": "brand new"
    }))
    .await
    .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("## New Section"), "heading missing: {text}");
    assert!(text.contains("brand new"), "content missing: {text}");
}

#[tokio::test]
async fn replace_section_with_empty_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MEMORY.md");
    std::fs::write(&path, "## Notes\nold stuff\n## End\ndone\n").unwrap();
    let tool = make_tool(dir.path());
    tool.execute(json!({
        "file": "MEMORY.md",
        "action": "replace_section",
        "section_title": "Notes",
        "content": ""
    }))
    .await
    .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains("old stuff"),
        "old body should be gone: {text}"
    );
    assert!(text.contains("## End"), "other section missing: {text}");
}

#[tokio::test]
async fn append_to_empty_memory_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MEMORY.md");
    std::fs::write(&path, "").unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "MEMORY.md",
            "action": "append",
            "content": "first line"
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "unexpected error: {}", result.output());
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("first line"));
}

#[tokio::test]
async fn replace_section_creates_memory_file_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "MEMORY.md",
            "action": "replace_section",
            "section_title": "First",
            "content": "hello"
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "unexpected error: {}", result.output());
    let text = std::fs::read_to_string(dir.path().join("MEMORY.md")).unwrap();
    assert!(text.contains("## First"));
    assert!(text.contains("hello"));
}

#[tokio::test]
async fn rejects_unknown_action() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "MEMORY.md",
            "action": "delete_all",
            "content": "x"
        }))
        .await
        .unwrap();
    assert!(result.is_error);
}

#[tokio::test]
async fn replace_section_missing_section_title_errors() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "MEMORY.md",
            "action": "replace_section",
            "content": "x"
        }))
        .await;
    // May return Err or Ok with is_error
    match result {
        Ok(r) => assert!(r.is_error),
        Err(_) => {} // also acceptable
    }
}

#[test]
fn tool_name_and_description() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    assert_eq!(tool.name(), "update_memory_md");
    assert_ne!(tool.description().len(), 0);
}

#[test]
fn parameters_schema_has_required_fields() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let schema = tool.parameters_schema();
    let required = schema["required"].as_array().unwrap();
    assert!(required.contains(&json!("file")));
    assert!(required.contains(&json!("action")));
}

#[tokio::test]
async fn rejects_disallowed_file() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    let result = tool
        .execute(json!({
            "file": "../../etc/passwd",
            "action": "append",
            "content": "evil"
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(result.output().contains("not allowed"));
}

#[tokio::test]
async fn missing_parameters_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(dir.path());
    for (args, needle) in [
        (json!({"action": "append", "content": "x"}), "'file'"),
        (json!({"file": "MEMORY.md", "content": "x"}), "'action'"),
        (
            json!({"file": "MEMORY.md", "action": "append"}),
            "'content'",
        ),
    ] {
        let err = tool.execute(args).await.unwrap_err();
        assert!(err.to_string().contains(needle), "{err}");
    }
}

#[tokio::test]
async fn a_missing_workspace_fails_to_canonicalize() {
    let dir = tempfile::tempdir().unwrap();
    let tool = make_tool(&dir.path().join("does-not-exist"));
    let err = tool
        .execute(json!({"file": "MEMORY.md", "action": "append", "content": "x"}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Failed to canonicalize workspace"));
}

#[tokio::test]
async fn an_unreadable_target_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    // A directory named MEMORY.md cannot be read as text (and is not NotFound).
    std::fs::create_dir(dir.path().join("MEMORY.md")).unwrap();
    let err = make_tool(dir.path())
        .execute(json!({"file": "MEMORY.md", "action": "append", "content": "x"}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Failed to read"), "{err}");
}

#[tokio::test]
async fn atomic_write_reports_staging_and_rename_failures() {
    let dir = tempfile::tempdir().unwrap();
    let missing_dir_target = dir.path().join("nope").join("MEMORY.md");
    let err = atomic_write(&missing_dir_target, "MEMORY.md", "x")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Failed to stage temp file"));

    // Renaming a file over a directory fails after the temp file was staged.
    let dir_target = dir.path().join("MEMORY.md");
    std::fs::create_dir(&dir_target).unwrap();
    let err = atomic_write(&dir_target, "MEMORY.md", "x")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Failed to atomically write"));
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_lock_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("target"),
        dir.path().join(".memory-write.lock"),
    )
    .unwrap();
    let err = make_tool(dir.path())
        .execute(json!({"file": "MEMORY.md", "action": "append", "content": "x"}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("is a symlink"), "{err}");
}

#[tokio::test]
async fn a_workspace_descriptor_overrides_the_configured_directory() {
    use crate::filesystem::test_support::WorkspaceContext;
    let configured = tempfile::tempdir().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    let context = WorkspaceContext::at(isolated.path());
    let result = make_tool(configured.path())
        .execute_with_context(
            json!({"file": "MEMORY.md", "action": "append", "content": "hi"}),
            ToolCallOptions::default(),
            Some(&context),
        )
        .await
        .unwrap();
    assert!(!result.is_error);
    assert!(isolated.path().join("MEMORY.md").exists());
    assert!(!configured.path().join("MEMORY.md").exists());
}

#[tokio::test]
async fn append_separates_from_content_lacking_a_trailing_newline() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("MEMORY.md"), "no newline").unwrap();
    make_tool(dir.path())
        .execute(json!({"file": "MEMORY.md", "action": "append", "content": "next"}))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.path().join("MEMORY.md")).unwrap(),
        "no newline\nnext\n"
    );
}

#[tokio::test]
async fn replace_section_before_another_heading_keeps_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("MEMORY.md"), "## A\nold\n## B\nkeep\n").unwrap();
    make_tool(dir.path())
        .execute(json!({"file": "MEMORY.md", "action": "replace_section", "section_title": "A", "content": "new"}))
        .await
        .unwrap();
    let text = std::fs::read_to_string(dir.path().join("MEMORY.md")).unwrap();
    assert!(text.contains("## A\nnew\n## B\nkeep"), "{text}");
}

#[tokio::test]
async fn replace_section_appends_after_a_file_without_trailing_newline() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("MEMORY.md"), "intro").unwrap();
    make_tool(dir.path())
        .execute(json!({"file": "MEMORY.md", "action": "replace_section", "section_title": "New", "content": "body"}))
        .await
        .unwrap();
    let text = std::fs::read_to_string(dir.path().join("MEMORY.md")).unwrap();
    assert_eq!(text, "intro\n## New\nbody\n");
}
