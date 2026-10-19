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
                &["-c", "sleep 10"],
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
}
