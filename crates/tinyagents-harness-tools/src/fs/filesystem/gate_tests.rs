//! Tests for how a call's workspace descriptor scopes the gate.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use super::gate::{FsGate, gate_for_context};
use super::test_support::{TestGate, WorkspaceContext};

#[tokio::test]
async fn without_a_descriptor_the_same_gate_is_used() {
    let dir = tempfile::tempdir().unwrap();
    let gate: Arc<dyn FsGate> = TestGate::supervised(dir.path().to_path_buf());
    let resolved = gate_for_context(&gate, None, "file_read");
    assert!(Arc::ptr_eq(&gate, &resolved));
}

#[tokio::test]
async fn a_descriptor_roots_the_gate_at_the_isolated_workspace() {
    let home = tempfile::tempdir().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    std::fs::write(isolated.path().join("seen.txt"), "x").unwrap();
    let gate: Arc<dyn FsGate> = TestGate::supervised(home.path().to_path_buf());

    let context = WorkspaceContext::at(isolated.path());
    let scoped = gate_for_context(&gate, Some(&context), "file_read");

    assert_eq!(scoped.action_dir(), isolated.path());
    let resolved = scoped.validate_path("seen.txt").await.unwrap();
    assert_eq!(
        resolved,
        isolated.path().join("seen.txt").canonicalize().unwrap()
    );
    // The original gate keeps its own root and refuses the isolated file.
    assert!(gate.validate_path("seen.txt").await.is_err());
}

#[tokio::test]
async fn the_fake_gates_delegate_every_question_to_the_gate_they_wrap() {
    use super::test_support::{AutonomyLevel, RacyGate, WrapGate};

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("f.txt"), "x").unwrap();
    let inner = TestGate::with(dir.path().to_path_buf(), AutonomyLevel::Supervised, 5);

    let racy = RacyGate(inner.clone());
    let wrap = WrapGate::new(inner.clone()).arc();
    let gates: [&dyn FsGate; 2] = [&racy, wrap.as_ref()];
    for gate in gates {
        assert!(gate.can_act());
        assert!(!gate.is_read_only());
        assert!(gate.write_needs_approval());
        assert_eq!(gate.action_dir(), dir.path());
        assert!(gate.is_path_string_allowed("f.txt"));
        assert!(gate.validate_path("f.txt").await.is_ok());
        assert!(gate.validate_parent_path("new.txt").await.is_ok());
        let scoped = gate.scoped_to_workspace(dir.path());
        assert_eq!(scoped.action_dir(), dir.path());
    }
}

#[tokio::test]
async fn the_fake_gate_refuses_what_a_real_policy_would() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::supervised(dir.path().to_path_buf());

    // No parent / no file name to resolve.
    assert!(gate.validate_parent_path("/").await.is_err());
    // Nothing on disk to resolve, even at the filesystem root.
    let far = TestGate::supervised(std::path::PathBuf::from("/definitely/not/a/real/root"));
    assert!(!far.is_path_string_allowed("x.txt"));
    assert!(far.validate_parent_path("x.txt").await.is_err());
    // A NUL byte is never a path.
    assert!(!gate.is_path_string_allowed("a\0b"));
}

#[test]
fn the_default_batch_answer_maps_the_single_path_check_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let gate = TestGate::supervised(dir.path().to_path_buf());
    let paths = vec![
        "ok.txt".to_string(),
        "../escape.txt".to_string(),
        "a\0b".to_string(),
        "sub/ok.rs".to_string(),
    ];
    let batch = gate.paths_string_allowed(&paths);
    let single: Vec<bool> = paths
        .iter()
        .map(|p| gate.is_path_string_allowed(p))
        .collect();
    assert_eq!(batch, single);
    assert_eq!(batch, vec![true, false, false, true]);
    assert_eq!(gate.paths_string_allowed(&[]), Vec::<bool>::new());
}
