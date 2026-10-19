//! Native process backend for hosts that explicitly select local execution.

use tinytools::ToolRunContext;

use super::{
    ExecBackend, ExecBackendOutcome, ExecOutput, ExecProgress, ExecRequest, ShellFlavor,
    timeout::{output_or_kill_with_progress_isolated, output_unbounded_with_progress_isolated},
};

/// Capability token required by [`NativeExecBackend`] before it may spawn.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeExecCapability;

/// Runs an authorized request as a local child process.
pub struct NativeExecBackend {
    flavor: ShellFlavor,
}

#[cfg(test)]
#[path = "native_backend_tests.rs"]
mod tests;

impl NativeExecBackend {
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
                    match output_or_kill_with_progress_isolated(&mut command, timeout, progress)
                        .await
                    {
                        Ok(Ok(output)) => Ok(ExecOutput {
                            stdout: output.stdout,
                            stderr: output.stderr,
                            exit_code: output.status.code(),
                            timed_out: false,
                        }),
                        Ok(Err(error)) => Err(error),
                        Err(_) => Ok(ExecOutput {
                            stdout: Vec::new(),
                            stderr: Vec::new(),
                            exit_code: None,
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
