#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;

#[test]
fn exited_members_do_not_block_cleanup_but_running_members_do() {
    for state in ["R", "S", "D", "T", "I"] {
        assert!(is_active_member(
            &format!("12 (name with ) spaces) {state} 1 42 42"),
            42
        ));
    }
    for state in ["Z", "X", "x"] {
        assert!(!is_active_member(
            &format!("12 (sleep) {state} 1 42 42"),
            42
        ));
    }
    assert!(!is_active_member("12 (sleep) R 1 43 43", 42));
    assert!(!is_active_member("malformed", 42));
}

#[tokio::test]
async fn wait_returns_immediately_for_a_group_with_no_members() {
    // PID 0x7fff_fff0 is above any pid_max, so no process group matches.
    wait(0x7fff_fff0).await.unwrap();
    assert!(!active_members(0x7fff_fff0).unwrap());
}

#[tokio::test]
async fn wait_polls_until_the_group_has_stopped() {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c").arg("sleep 0.3");
    cmd.process_group(0);
    let mut child = cmd.spawn().unwrap();
    let pid = child.id().unwrap();
    assert!(active_members(pid).unwrap());
    // The group leader stays unreaped, so `wait` only returns once the
    // member has exited (zombie) after polling at least once.
    tokio::time::timeout(std::time::Duration::from_secs(10), wait(pid))
        .await
        .expect("group stops")
        .unwrap();
    child.wait().await.unwrap();
}
