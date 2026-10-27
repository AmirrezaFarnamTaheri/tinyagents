//! Revision races, final policy decisions, atomicity and staging cleanup.

#![allow(clippy::unwrap_used)]

use super::test_support::*;
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;

const SOURCE: &str = "fn value() -> i32 { 1 }\n";

#[tokio::test]
async fn an_external_write_after_admission_is_preserved_and_staging_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    *gate.state.action.lock() = Some(Action::Write(
        path.clone(),
        "fn value() -> i32 { 99 }".into(),
    ));
    let tools = code_tools(gate.clone()).unwrap();
    assert!(
        failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
            .await
            .contains("revision changed")
    );
    assert!(std::fs::read_to_string(path).unwrap().contains("99"));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn original_path_reauthorization_and_late_read_only_policy_prevent_publication() {
    for revoke_at in ["path", "action", "publication"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.rs");
        std::fs::write(&path, SOURCE).unwrap();
        let gate = TestGate::new(dir.path());
        if revoke_at == "action" {
            *gate.state.action.lock() = Some(Action::Revoke);
        } else if revoke_at == "publication" {
            gate.state.revoke_at_recheck.store(true, Ordering::SeqCst);
        } else {
            gate.state.deny_recheck.store(true, Ordering::SeqCst);
        }
        let tools = code_tools(gate).unwrap();
        assert!(
            failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
                .await
                .contains("policy-blocked")
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), SOURCE);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn the_final_available_action_can_publish_after_reserving_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = Arc::new(TestGate {
        root: dir.path().to_path_buf(),
        state: Arc::new(State::default()),
        max_actions: 1,
    });
    let tools = code_tools(gate.clone()).unwrap();
    let receipt = success(tool(&tools, "ast_patch"), patch_args(SOURCE)).await;
    assert_eq!(receipt["written"], true);
    assert!(std::fs::read_to_string(path).unwrap().contains('2'));
    assert_eq!(gate.state.used.load(Ordering::SeqCst), 1);
    assert!(gate.is_rate_limited());
}

#[tokio::test]
async fn depleted_and_racing_action_budgets_refuse_before_staging() {
    for predepleted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("source.rs"), SOURCE).unwrap();
        let gate = TestGate::new(dir.path());
        if predepleted {
            gate.state.used.store(100, Ordering::SeqCst);
        } else {
            gate.state.refuse_record.store(true, Ordering::SeqCst);
        }
        let tools = code_tools(gate).unwrap();
        assert!(
            failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
                .await
                .contains("budget")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("source.rs")).unwrap(),
            SOURCE
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn deleted_destinations_and_new_file_collisions_do_not_get_recreated_or_clobbered() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let gate = TestGate::new(dir.path());
    *gate.state.action.lock() = Some(Action::Remove(path.clone()));
    let tools = code_tools(gate.clone()).unwrap();
    assert!(
        failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
            .await
            .contains("permissions")
    );
    assert!(!path.exists());
    let inspected = success(
        tool(&tools, "chunk_inspect"),
        json!({"id":"rust.fallible-function.v1"}),
    )
    .await;
    *gate.state.action.lock() = Some(Action::Write(path.clone(), "external writer".into()));
    assert!(
        failure(
            tool(&tools, "chunk_apply"),
            json!({"id":"rust.fallible-function.v1","path":"source.rs", "create_new":true,
        "chunk_revision":inspected["chunk_revision"]})
        )
        .await
        .contains("existing")
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "external writer");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn a_changed_custom_manifest_or_template_invalidates_publication() {
    for manifest_changed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        custom_manifest(dir.path(), "file", "fn created() {}\n");
        let gate = TestGate::new(dir.path());
        let tools = code_tools(gate.clone()).unwrap();
        let inspected = success(
            tool(&tools, "chunk_inspect"),
            json!({"manifest_path":"manifest.json"}),
        )
        .await;
        let path = dir.path().join(if manifest_changed {
            "manifest.json"
        } else {
            "template.rs.txt"
        });
        let changed = if manifest_changed {
            std::fs::read_to_string(&path)
                .unwrap()
                .replace("Custom pattern", "Changed pattern")
        } else {
            "fn changed() {}\n".into()
        };
        *gate.state.action.lock() = Some(Action::Write(path, changed));
        assert!(
            failure(
                tool(&tools, "chunk_apply"),
                json!({"manifest_path":"manifest.json", "path":"new.rs", "create_new":true,
            "chunk_revision":inspected["chunk_revision"]})
            )
            .await
            .contains("revision changed")
        );
        assert!(!dir.path().join("new.rs").exists());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }
}

#[tokio::test]
async fn cancellation_after_staging_keeps_the_original_and_removes_the_temporary_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(TestGate {
        root: dir.path().to_path_buf(),
        max_actions: 100,
        state: Arc::new(State {
            pause_recheck: Some((entered.clone(), released)),
            ..Default::default()
        }),
    });
    let tools = code_tools(gate).unwrap();
    let mut task =
        tokio::spawn(async move { tool(&tools, "ast_patch").execute(patch_args(SOURCE)).await });
    let completed = tokio::select! {
        () = entered.notified() => None,
        result = &mut task => Some(result),
    };
    assert!(
        completed.is_none(),
        "call did not reach its staging pause: {completed:?}"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(std::fs::read_to_string(path).unwrap(), SOURCE);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_preserves_permissions_and_symlink_destinations_are_refused() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.rs");
    std::fs::write(&path, SOURCE).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o751)).unwrap();
    let tools = code_tools(TestGate::new(dir.path())).unwrap();
    success(tool(&tools, "ast_patch"), patch_args(SOURCE)).await;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o751
    );
    std::fs::rename(&path, dir.path().join("real.rs")).unwrap();
    symlink(dir.path().join("real.rs"), &path).unwrap();
    assert!(
        failure(tool(&tools, "ast_patch"), patch_args(SOURCE))
            .await
            .contains("symlink")
    );
}
