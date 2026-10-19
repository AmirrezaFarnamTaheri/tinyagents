use super::*;
use crate::exec::{ExecBackendCapability, ExecRequest};
use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, time::Duration};

fn request(
    program: &str,
    args: &[&str],
    timeout: Option<Duration>,
    capability: bool,
) -> ExecRequest {
    ExecRequest {
        program: OsString::from(program),
        args: args.iter().map(OsString::from).collect(),
        action_dir: PathBuf::from("."),
        environment: BTreeMap::new(),
        timeout,
        shell_command: None,
        backend_selection: "native".into(),
        backend_capability: if capability {
            ExecBackendCapability::new(NativeExecCapability)
        } else {
            ExecBackendCapability::new(())
        },
    }
}

#[cfg(target_os = "linux")]
fn process_is_gone_or_zombie(pid: i32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    stat.rsplit_once(')')
        .and_then(|(_, tail)| tail.split_whitespace().next())
        .is_some_and(|state| state == "Z")
}

#[cfg(target_os = "linux")]
fn shell_quote(path: &std::path::Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

#[cfg(unix)]
#[tokio::test]
async fn native_backend_preserves_argv_and_captures_both_streams() {
    use crate::exec::{ExecBackend, ExecBackendOutcome, ShellFlavor};
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let outcome = backend
        .execute(
            request(
                "sh",
                &["-c", "printf 'hello'; printf 'warning' >&2"],
                None,
                true,
            ),
            None,
            None,
        )
        .await;
    let ExecBackendOutcome::Completed(output) = outcome else {
        panic!("expected process output")
    };
    assert_eq!(output.stdout, b"hello");
    assert_eq!(output.stderr, b"warning");
    assert_eq!(output.exit_code, Some(0));
    assert!(!output.timed_out);
}

#[tokio::test]
async fn native_backend_refuses_without_the_host_capability() {
    use crate::exec::{ExecBackend, ExecBackendOutcome, ExecRefusalKind, ShellFlavor};
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let outcome = backend
        .execute(
            request(
                "sh",
                &["-c", "touch /tmp/tinyagents-should-not-run"],
                None,
                false,
            ),
            None,
            None,
        )
        .await;
    assert!(matches!(
        outcome,
        ExecBackendOutcome::Refused {
            kind: ExecRefusalKind::Preflight,
            ..
        }
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn native_backend_does_not_reintroduce_ambient_turn_environment() {
    use crate::exec::{ExecBackend, ExecBackendOutcome, ShellFlavor, timeout::CommandEnvironment};
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let authorized =
        CommandEnvironment::new([("DENIED_FROM_TURN".to_owned(), "secret".to_owned())]);
    let outcome=authorized.scope(backend.execute(request("sh", &["-c", "if [ -z \"${DENIED_FROM_TURN:-}\" ]; then printf clean; else printf leaked; fi"],None,true),None,None)).await;
    let ExecBackendOutcome::Completed(output) = outcome else {
        panic!("expected process output")
    };
    assert_eq!(output.stdout, b"clean");
}

#[cfg(unix)]
#[tokio::test]
async fn native_backend_timeout_kills_the_process_group_before_returning() {
    use crate::exec::{ExecBackend, ExecBackendOutcome, ShellFlavor};
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let outcome = backend
        .execute(
            request(
                "sh",
                &["-c", "printf partial; sleep 10"],
                Some(Duration::from_millis(40)),
                true,
            ),
            None,
            None,
        )
        .await;
    let ExecBackendOutcome::Completed(output) = outcome else {
        panic!("expected timeout result")
    };
    assert!(output.timed_out);
    assert_eq!(output.stdout, b"partial");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_timeout_returns_after_descendant_and_pipes_are_cleaned() {
    use crate::exec::{ExecBackend, ExecBackendOutcome, ShellFlavor};
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("child.pid");
    let command = format!(
        "sleep 30 & child=$!; printf '%s\\n' \"$child\" > {}; printf captured; wait",
        shell_quote(&pidfile),
    );
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let cleanup = crate::exec::timeout::ProcessCleanup::default();
    let outcome = cleanup
        .scope(backend.execute(
            request("sh", &["-c", &command], Some(Duration::from_secs(1)), true),
            None,
            None,
        ))
        .await;
    cleanup.wait().await;
    let ExecBackendOutcome::Completed(output) = outcome else {
        panic!("expected timeout completion")
    };
    assert!(output.timed_out);
    assert_eq!(output.stdout, b"captured");
    let pid: i32 = std::fs::read_to_string(pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        process_is_gone_or_zombie(pid),
        "descendant {pid} was still executing after timeout returned"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancellation_waits_for_native_process_tree_reaping() {
    use crate::exec::{ExecBackend, ShellFlavor, timeout::ProcessCleanup};
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("child.pid");
    let command = format!(
        "sleep 30 & child=$!; printf '%s\\n' \"$child\" > {}; wait",
        shell_quote(&pidfile),
    );
    let backend = NativeExecBackend::new(ShellFlavor::Posix);
    let cleanup = ProcessCleanup::default();
    let turn_cleanup = cleanup.clone();
    let turn = tokio::spawn(async move {
        turn_cleanup
            .scope(backend.execute(request("sh", &["-c", &command], None, true), None, None))
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|pid| pid.trim().parse::<i32>().ok())
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native child began before cancellation");
    turn.abort();
    let _ = turn.await;
    tokio::time::timeout(Duration::from_secs(3), cleanup.wait())
        .await
        .expect("native process reaped before cancellation acknowledgment");
    let pid: i32 = std::fs::read_to_string(pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        process_is_gone_or_zombie(pid),
        "descendant {pid} was still executing after cleanup acknowledgment"
    );
}
