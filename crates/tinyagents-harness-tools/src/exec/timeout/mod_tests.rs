#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::cast_sign_loss
)]
use super::*;
use std::time::Duration;
use tinytools::ToolTimeout;

fn sh(script: &str) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c").arg(script);
    cmd
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    rustix::process::Pid::from_raw(pid)
        .is_some_and(|pid| rustix::process::test_kill_process(pid).is_ok())
}

#[test]
fn configure_pushes_the_inherited_timeout_into_the_global() {
    let global = configure(TimeoutSettings::with_inherited_secs(77));
    assert_eq!(global.inherited_timeout(), Duration::from_secs(77));
    configure(TimeoutSettings::with_inherited_secs(88));
    assert_eq!(settings().inherited_timeout(), Duration::from_secs(88));
    assert_eq!(
        settings().resolve(ToolTimeout::Inherit).deadline,
        Some(Duration::from_secs(88))
    );
    assert_eq!(settings().resolve(ToolTimeout::Unbounded).deadline, None);
}

#[test]
fn default_when_env_missing() {
    assert_eq!(parse_tool_timeout_secs(None), DEFAULT_TIMEOUT_SECS);
}

#[test]
fn default_when_value_not_numeric() {
    assert_eq!(
        parse_tool_timeout_secs(Some("not-a-number")),
        DEFAULT_TIMEOUT_SECS
    );
    assert_eq!(parse_tool_timeout_secs(Some("")), DEFAULT_TIMEOUT_SECS);
    assert_eq!(parse_tool_timeout_secs(Some("12x")), DEFAULT_TIMEOUT_SECS);
}

#[test]
fn default_when_value_zero() {
    // 0 seconds would disable the timeout — reject and fall back.
    assert_eq!(parse_tool_timeout_secs(Some("0")), DEFAULT_TIMEOUT_SECS);
}

#[test]
fn default_when_value_above_max() {
    assert_eq!(parse_tool_timeout_secs(Some("3601")), DEFAULT_TIMEOUT_SECS);
    assert_eq!(
        parse_tool_timeout_secs(Some("99999999999")),
        DEFAULT_TIMEOUT_SECS
    );
}

#[test]
fn default_when_value_negative_or_signed() {
    // Negative values fail u64 parse and fall back to default.
    assert_eq!(parse_tool_timeout_secs(Some("-5")), DEFAULT_TIMEOUT_SECS);
}

#[test]
fn accepts_valid_values_at_boundaries() {
    assert_eq!(parse_tool_timeout_secs(Some("1")), MIN_TIMEOUT_SECS);
    assert_eq!(parse_tool_timeout_secs(Some("3600")), MAX_TIMEOUT_SECS);
}

#[test]
fn accepts_valid_midrange_value() {
    assert_eq!(parse_tool_timeout_secs(Some("300")), 300);
}

#[test]
fn env_override_takes_precedence_over_config() {
    // When the env var holds a valid value it wins over the config value.
    assert_eq!(resolve_effective(300, Some("600")), 600);
}

#[test]
fn config_value_used_when_env_absent_or_invalid() {
    // No env → config drives the effective value (bounded).
    assert_eq!(resolve_effective(300, None), 300);
    // Present-but-invalid env (non-numeric / 0 / out of range) is ignored,
    // so the config value still applies.
    assert_eq!(resolve_effective(300, Some("nonsense")), 300);
    assert_eq!(resolve_effective(300, Some("0")), 300);
    assert_eq!(resolve_effective(300, Some("4000")), 300);
}

#[test]
fn config_value_is_bounded() {
    // An out-of-range config value falls back to the default rather than
    // being applied verbatim.
    assert_eq!(resolve_effective(0, None), DEFAULT_TIMEOUT_SECS);
    assert_eq!(resolve_effective(99_999, None), DEFAULT_TIMEOUT_SECS);
}

#[test]
fn explicit_call_timeout_unbounded_when_absent_or_disabled() {
    // No request, or an explicit 0, means "run unbounded" (None).
    assert_eq!(explicit_call_timeout_secs(None, MAX_TIMEOUT_SECS), None);
    assert_eq!(explicit_call_timeout_secs(Some(0), MAX_TIMEOUT_SECS), None);
}

#[test]
fn explicit_call_timeout_enforces_and_clamps_request() {
    // An in-range request is enforced verbatim.
    assert_eq!(
        explicit_call_timeout_secs(Some(900), MAX_TIMEOUT_SECS),
        Some(900)
    );
    // Below the floor clamps up to MIN.
    assert_eq!(
        explicit_call_timeout_secs(Some(0), MAX_TIMEOUT_SECS),
        None,
        "0 disables rather than clamping to MIN"
    );
    // Above the cap clamps down to the cap.
    assert_eq!(
        explicit_call_timeout_secs(Some(99_999), MAX_TIMEOUT_SECS),
        Some(MAX_TIMEOUT_SECS)
    );
    // A tool with a tighter own ceiling (node/npm at 1800) clamps to it.
    assert_eq!(explicit_call_timeout_secs(Some(99_999), 1800), Some(1800));
    assert_eq!(explicit_call_timeout_secs(Some(600), 1800), Some(600));
}

#[test]
fn explicit_call_timeout_duration_matches_secs() {
    assert_eq!(
        explicit_call_timeout_duration(Some(900), MAX_TIMEOUT_SECS),
        Some(Duration::from_secs(900))
    );
    assert_eq!(explicit_call_timeout_duration(None, MAX_TIMEOUT_SECS), None);
}

#[test]
fn override_from_rejects_invalid() {
    assert_eq!(override_from(None), None);
    assert_eq!(override_from(Some("")), None);
    assert_eq!(override_from(Some("0")), None);
    assert_eq!(override_from(Some("abc")), None);
    assert_eq!(override_from(Some("3601")), None);
    assert_eq!(override_from(Some("120")), Some(120));
}

/// Table of `(policy, inherited config secs, expected (deadline_ms, budget_secs))`.
/// Pins the deadline resolution.
#[test]
fn resolve_tool_deadline_table() {
    use tinytools::ToolTimeout;
    let ms = |m: u64| Some(Duration::from_millis(m));
    let cases: Vec<(ToolTimeout, u64, (Option<Duration>, u64))> = vec![
        // Inherit: the global config value, no grace.
        (ToolTimeout::Inherit, 120, (ms(120_000), 120)),
        (ToolTimeout::Inherit, 1, (ms(1_000), 1)),
        (ToolTimeout::Inherit, 3600, (ms(3_600_000), 3600)),
        // Unbounded: no deadline, zero budget.
        (ToolTimeout::Unbounded, 120, (None, 0)),
        // Millis: rounded UP to whole seconds, clamped, +5s grace on deadline.
        (ToolTimeout::Millis(0), 120, (ms(6_000), 1)),
        (ToolTimeout::Millis(1), 120, (ms(6_000), 1)),
        (ToolTimeout::Millis(1_000), 120, (ms(6_000), 1)),
        (ToolTimeout::Millis(1_001), 120, (ms(7_000), 2)),
        (ToolTimeout::Millis(1_500), 120, (ms(7_000), 2)),
        (ToolTimeout::Millis(30_000), 120, (ms(35_000), 30)),
        (ToolTimeout::Millis(3_600_000), 120, (ms(3_605_000), 3600)),
        (ToolTimeout::Millis(3_600_001), 120, (ms(3_605_000), 3600)),
        (ToolTimeout::Millis(u64::MAX), 120, (ms(3_605_000), 3600)),
    ];
    for (policy, inherited_secs, expected) in cases {
        // Resolve against the pure helper so the process-global stays untouched.
        let r = TimeoutSettings::with_inherited_secs(inherited_secs).resolve(policy);
        let got = (r.deadline, r.budget_ms / 1000);
        assert_eq!(
            got, expected,
            "policy {policy:?} inherited {inherited_secs}s"
        );
    }
}

/// Env / config precedence table for the effective inherited timeout.
#[test]
fn resolve_effective_table() {
    let cases: &[(u64, Option<&str>, u64)] = &[
        (300, None, 300),
        (300, Some("600"), 600),
        (300, Some("1"), 1),
        (300, Some("3600"), 3600),
        (300, Some("3601"), 300),
        (300, Some("0"), 300),
        (300, Some("-1"), 300),
        (300, Some(""), 300),
        (300, Some("abc"), 300),
        (0, None, DEFAULT_TIMEOUT_SECS),
        (0, Some("45"), 45),
        (3601, None, DEFAULT_TIMEOUT_SECS),
        (u64::MAX, None, DEFAULT_TIMEOUT_SECS),
        (1, None, 1),
        (3600, None, 3600),
    ];
    for &(config, env, expected) in cases {
        assert_eq!(
            resolve_effective(config, env),
            expected,
            "config {config} env {env:?}"
        );
    }
}

/// The deadline must take the command's whole process group with it. A shell
/// pipeline is grandchildren of the tool's child, and a `grep -rl … /` the tool
/// had reported as killed ran on for half an hour at a full core.
#[cfg(unix)]
#[tokio::test]
async fn a_timed_out_command_takes_its_whole_process_group_with_it() {
    let dir = tempfile::tempdir()
        .unwrap()
        .keep()
        .join(format!("oh-pgkill-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let pidfile = dir.join("grandchild.pid");
    // `sleep 30 &` is a grandchild in the shell's own process group; `wait`
    // keeps the shell alive past the deadline.
    let mut cmd = sh(&format!("sleep 30 & echo $! > {}; wait", pidfile.display()));
    let result = output_or_kill(&mut cmd, Duration::from_millis(700)).await;
    assert!(result.is_err(), "the deadline must fire on a 30s sleep");

    let grandchild: i32 = loop {
        if let Ok(text) = std::fs::read_to_string(&pidfile) {
            if let Ok(pid) = text.trim().parse() {
                break pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut gone = false;
    for _ in 0..100 {
        // kill(pid, 0) succeeds while the process (or its zombie) exists.
        if !process_exists(grandchild) {
            gone = true;
            break;
        }
        // A container's PID 1 may leave a killed orphan unreaped. It cannot
        // execute side effects; use the same kernel-state rule as cleanup.
        #[cfg(target_os = "linux")]
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{grandchild}/stat")) {
            let group = rustix::process::Pid::from_raw(grandchild)
                .and_then(|pid| rustix::process::getpgid(Some(pid)).ok());
            if let Some(group) = group.map(|g| g.as_raw_nonzero().get() as u32) {
                if !group_exit::is_active_member(&stat, group) {
                    gone = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(gone, "grandchild {grandchild} survived the deadline");
}

/// Without a deadline the child still dies with the handle: a cancelled tool
/// future must not leave the command running either.
#[cfg(unix)]
#[tokio::test]
async fn a_shell_family_child_dies_with_a_dropped_future() {
    let dir = tempfile::tempdir()
        .unwrap()
        .keep()
        .join(format!("oh-dropkill-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let pidfile = dir.join("child.pid");
    let mut cmd = sh(&format!("echo $$ > {}; sleep 30", pidfile.display()));
    // `timeout` takes the future by value and drops it when the deadline
    // passes -- exactly the abandoned-future path a cancelled tool call takes.
    let _ = tokio::time::timeout(Duration::from_millis(300), output_unbounded(&mut cmd)).await;
    let shell: i32 = loop {
        if let Ok(text) = std::fs::read_to_string(&pidfile) {
            if let Ok(pid) = text.trim().parse() {
                break pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut gone = false;
    for _ in 0..100 {
        if !process_exists(shell) {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(gone, "shell {shell} survived its dropped future");
}

/// A reaped scope: after the turn future is dropped, `ProcessCleanup::wait`
/// returns only once the sleeping child group is gone.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn process_cleanup_waits_for_a_cancelled_sleeping_group() {
    let cleanup = ProcessCleanup::default();
    let turn = {
        let cleanup = cleanup.clone();
        tokio::spawn(async move {
            cleanup
                .scope(async {
                    let mut cmd = sh("sleep 30 & wait");
                    let _ = output_unbounded(&mut cmd).await;
                })
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    turn.abort();
    let _ = turn.await;
    tokio::time::timeout(Duration::from_secs(10), cleanup.wait())
        .await
        .expect("cleanup completes once the group is reaped");
}

#[tokio::test]
async fn command_environment_replaces_the_child_environment() {
    let env = CommandEnvironment::new([
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("ONLY_THIS".to_string(), "1".to_string()),
    ]);
    let out = env
        .scope(async {
            let mut cmd = sh("echo ${ONLY_THIS:-missing}-${HOME:-nohome}");
            output_unbounded(&mut cmd).await.unwrap()
        })
        .await;
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "1-nohome");
}

#[tokio::test]
async fn command_environment_scopes_variable_lookup() {
    assert!(!CommandEnvironment::is_active());
    let env = CommandEnvironment::new([("SCOPED_ONLY".to_string(), "yes".to_string())]);
    env.scope(async {
        assert!(CommandEnvironment::is_active());
        assert_eq!(
            CommandEnvironment::var_os("SCOPED_ONLY").unwrap(),
            std::ffi::OsString::from("yes")
        );
        assert_eq!(CommandEnvironment::var("SCOPED_ONLY").unwrap(), "yes");
        assert!(CommandEnvironment::var_os("PATH").is_none());
        assert!(CommandEnvironment::var("PATH").is_err());
    })
    .await;
    assert!(CommandEnvironment::var_os("PATH").is_some());
    assert!(CommandEnvironment::var("PATH").is_ok());
}

#[tokio::test]
async fn explicit_command_env_overrides_and_removals_survive_the_scoped_base() {
    let env = CommandEnvironment::new([
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ("BASE".to_string(), "base".to_string()),
        ("DROP".to_string(), "x".to_string()),
    ]);
    let out = env
        .scope(async {
            let mut cmd = sh("echo $BASE-$EXTRA-${DROP:-dropped}");
            cmd.env("BASE", "override")
                .env("EXTRA", "e")
                .env_remove("DROP");
            output_unbounded(&mut cmd).await.unwrap()
        })
        .await;
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "override-e-dropped"
    );
}

#[tokio::test]
async fn process_cleanup_reports_active_only_inside_its_scope() {
    assert!(!ProcessCleanup::is_active());
    let cleanup = ProcessCleanup::default();
    cleanup
        .scope(async {
            assert!(ProcessCleanup::is_active());
        })
        .await;
    cleanup.wait().await;
}

/// A hold taken inside a turn's scope keeps `wait` pending until dropped, so
/// detached host work can finish its own cleanup before cancellation is
/// acknowledged. A hold taken outside any scope registers nothing.
#[tokio::test]
async fn a_cleanup_hold_keeps_wait_pending_until_dropped() {
    let cleanup = ProcessCleanup::default();
    let hold = cleanup.scope(async { ProcessCleanup::hold() }).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cleanup.wait())
            .await
            .is_err(),
        "wait must not return while the hold is alive"
    );
    drop(hold);
    tokio::time::timeout(Duration::from_secs(1), cleanup.wait())
        .await
        .expect("wait returns once the hold is dropped");

    let unscoped = ProcessCleanup::default();
    let _outside = ProcessCleanup::hold();
    tokio::time::timeout(Duration::from_secs(1), unscoped.wait())
        .await
        .expect("a hold outside any scope registers nothing");
}
