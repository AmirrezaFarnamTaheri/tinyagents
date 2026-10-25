//! Native process backend for hosts that explicitly select local execution.

use tinytools::ToolRunContext;

use super::{
    ExecBackend, ExecBackendOutcome, ExecOutput, ExecProgress, ExecRequest, ShellFlavor,
    timeout::{
        CapturedCommandResult, output_or_kill_captured_isolated,
        output_unbounded_with_progress_isolated,
    },
};

/// Capability token required by [`NativeExecBackend`] before it may spawn.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeExecCapability;

/// Runs an authorized request as a local child process.
///
/// Hosts opt into this backend by constructing it with the selected
/// [`ShellFlavor`] and providing it to an [`ExecTool`](super::ExecTool) through
/// the execution ports. The backend only checks the opaque native capability
/// supplied by the host; it does not implement approval, command policy,
/// sandbox selection, or audit policy.
///
/// A host that acknowledges cancellation must retain a
/// [`super::timeout::ProcessCleanup`] for the turn, run the turn inside
/// [`ProcessCleanup::scope`](super::timeout::ProcessCleanup::scope), drop the
/// scoped turn future on cancellation, and await
/// [`ProcessCleanup::wait`](super::timeout::ProcessCleanup::wait) before
/// acknowledging cleanup. This lets the backend's child waiter finish and
/// close captured output pipes before the host reports that cancellation is
/// complete.
pub struct NativeExecBackend {
    flavor: ShellFlavor,
}

#[cfg(test)]
#[path = "native_backend_tests.rs"]
mod tests;

impl NativeExecBackend {
    /// Selects the shell syntax used by this local backend.
    ///
    /// The host remains responsible for authorizing a request and attaching a
    /// [`NativeExecCapability`] only when local execution is permitted.
    #[must_use]
    pub fn new(flavor: ShellFlavor) -> Self {
        Self { flavor }
    }
}

#[async_trait::async_trait]
impl ExecBackend for NativeExecBackend {
    fn shell_flavor(&self) -> ShellFlavor {
        self.flavor
    }

    async fn execute(
        &self,
        request: ExecRequest,
        _context: Option<&dyn ToolRunContext>,
        progress: Option<ExecProgress>,
    ) -> ExecBackendOutcome {
        use std::process::Stdio;
        if request.backend_selection != "native"
            || request
                .backend_capability
                .downcast_ref::<NativeExecCapability>()
                .is_none()
        {
            return ExecBackendOutcome::Refused {
                kind: super::ExecRefusalKind::Preflight,
                reason: "native execution was not selected or authorized".into(),
            };
        }
        if request.program.is_empty() {
            return ExecBackendOutcome::Refused {
                kind: super::ExecRefusalKind::Preflight,
                reason: "program is empty".into(),
            };
        }
        let mut command = tokio::process::Command::new(&request.program);
        command
            .args(&request.args)
            .current_dir(&request.action_dir)
            .env_clear()
            .envs(request.environment)
            .stdin(Stdio::null());
        let run = async {
            match request.timeout {
                Some(timeout) => {
                    match output_or_kill_captured_isolated(&mut command, timeout, progress).await {
                        CapturedCommandResult::Completed(Ok(output)) => Ok(ExecOutput {
                            stdout: output.stdout,
                            stderr: output.stderr,
                            exit_code: output.status.code(),
                            timed_out: false,
                        }),
                        CapturedCommandResult::Completed(Err(error)) => Err(error),
                        CapturedCommandResult::TimedOut(output) => Ok(ExecOutput {
                            stdout: output
                                .as_ref()
                                .map_or_else(Vec::new, |output| output.stdout.clone()),
                            stderr: output
                                .as_ref()
                                .map_or_else(Vec::new, |output| output.stderr.clone()),
                            exit_code: output.and_then(|output| output.status.code()),
                            timed_out: true,
                        }),
                    }
                }
                None => output_unbounded_with_progress_isolated(&mut command, progress)
                    .await
                    .map(|output| ExecOutput {
                        stdout: output.stdout,
                        stderr: output.stderr,
                        exit_code: output.status.code(),
                        timed_out: false,
                    }),
            }
        };
        match run.await {
            Ok(output) => ExecBackendOutcome::Completed(output),
            Err(error) => ExecBackendOutcome::LaunchFailed {
                error: error.to_string(),
            },
        }
    }
}
