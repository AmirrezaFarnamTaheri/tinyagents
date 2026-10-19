//! Unit tests for `PATH` probing, `PATHEXT` candidate names, and the
//! `detect_tools` tool's payload.

use super::*;

#[cfg(unix)]
#[test]
fn executable_lookup_requires_current_process_access() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("tinytools-exec-check-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let file = dir.join("candidate");
    std::fs::write(&file, "binary")?;
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))?;
    assert!(!is_executable_file(&file));
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700))?;
    assert!(is_executable_file(&file));
    assert!(!is_executable_file(&dir));
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn name_and_permission() {
    let tool = DetectToolsTool::new();
    assert_eq!(tool.name(), "detect_tools");
    assert_eq!(tool.permission_level(), PermissionLevel::ReadOnly);
}

#[test]
fn default_and_metadata_contracts_are_available() {
    let tool = <DetectToolsTool as Default>::default();
    assert!(tool.description().contains("PATH"));
    assert_eq!(
        tool.parameters_schema()["properties"]["tools"]["type"],
        "array"
    );
}

#[tokio::test]
async fn non_string_tool_names_fall_back_to_the_default_catalog() -> anyhow::Result<()> {
    let result = DetectToolsTool::new()
        .execute(json!({"tools": [null, 3]}))
        .await?;
    let payload: serde_json::Value = serde_json::from_str(&result.output())?;
    assert_eq!(payload["probed"], super::DEFAULT_CANDIDATES.len());
    Ok(())
}

#[tokio::test]
async fn missing_tool_reported_missing() -> anyhow::Result<()> {
    let tool = DetectToolsTool::new();
    let result = tool
        .execute(json!({ "tools": ["definitely_not_a_real_binary_xyz_123"] }))
        .await?;
    assert!(!result.is_error);
    let payload: serde_json::Value = serde_json::from_str(&result.output())?;
    assert_eq!(payload["probed"], 1);
    assert_eq!(payload["available"], json!([]));
    assert_eq!(
        payload["missing"],
        json!(["definitely_not_a_real_binary_xyz_123"])
    );
    Ok(())
}

#[tokio::test]
async fn available_plus_missing_equals_probed() -> anyhow::Result<()> {
    let tool = DetectToolsTool::new();
    let result = tool
        .execute(json!({ "tools": ["sh", "definitely_not_a_real_binary_xyz_123"] }))
        .await?;
    let payload: serde_json::Value = serde_json::from_str(&result.output())?;
    let avail = payload["available"].as_array().map_or(0, Vec::len);
    let miss = payload["missing"].as_array().map_or(0, Vec::len);
    assert_eq!(avail + miss, 2);
    Ok(())
}

#[test]
fn bare_name_is_probed_unchanged_without_pathext() {
    assert_eq!(candidate_file_names("git", None), vec!["git".to_string()]);
}

#[test]
fn extensionless_name_gets_each_pathext_extension() {
    assert_eq!(
        candidate_file_names("git", Some(".EXE;.CMD")),
        vec!["git.EXE".to_string(), "git.CMD".to_string()]
    );
}

#[test]
fn name_already_carrying_a_pathext_extension_is_probed_unchanged() {
    // Windows must probe `git.exe`, not `git.exe.EXE`.
    assert_eq!(
        candidate_file_names("git.exe", Some(".EXE;.CMD;.BAT")),
        vec!["git.exe".to_string()]
    );
    assert_eq!(
        candidate_file_names("build.Cmd", Some(".EXE;.CMD")),
        vec!["build.Cmd".to_string()]
    );
}

#[test]
fn name_with_a_non_pathext_extension_still_gets_pathext_appended() {
    assert_eq!(
        candidate_file_names("python3.11", Some(".EXE")),
        vec!["python3.11.EXE".to_string()]
    );
}

#[test]
fn empty_pathext_entries_are_ignored() {
    assert_eq!(
        candidate_file_names("git", Some(".EXE;;")),
        vec!["git.EXE".to_string()]
    );
    assert_eq!(
        candidate_file_names("git", Some("")),
        vec!["git".to_string()]
    );
}
