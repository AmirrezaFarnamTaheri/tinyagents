use std::{ffi::OsString, sync::Arc, time::Instant};

use async_trait::async_trait;
use serde_json::Value;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext, ToolTimeout};

use super::{
    ExecAuditEvent, ExecAuditOutcome, ExecAuthorizationRequest, ExecBackend, ExecBackendOutcome,
    ExecDenied, ExecGate, ExecOperation, ExecRequest, ShellFlavor,
    progress::{HEARTBEAT_INTERVAL, with_exec_progress},
    shell_description::shell_description,
    shell_support::{exec_result, python_environment},
    timeout::{MAX_TIMEOUT_SECS, explicit_call_timeout_duration},
};

/// Generic shell execution tool. Host policy and process selection are injected.
pub struct ExecTool {
    gate: Arc<dyn ExecGate>,
    backend: Arc<dyn ExecBackend>,
}

impl ExecTool {
    /// Creates an exec tool with its host authorization gate and selected backend.
    #[must_use]
    pub fn new(gate: Arc<dyn ExecGate>, backend: Arc<dyn ExecBackend>) -> Self {
        Self { gate, backend }
    }

    fn operation(&self, args: &Value) -> Result<ExecOperation, String> {
        let Some(command) = args.get("command").and_then(Value::as_str) else {
            return Err("Missing required command string.".to_owned());
        };
        Ok(ExecOperation::Shell {
            command: command.to_owned(),
            category: args
                .get("category")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
    }
}

#[async_trait]
impl Tool for ExecTool {
    fn name(&self) -> &str {
        "exec"
    }

    fn description(&self) -> &str {
        shell_description(self.backend.shell_flavor())
    }

    fn parameters_schema(&self) -> Value {
        match self.backend.shell_flavor() {
            ShellFlavor::Posix => crate::static_schema!(include_str!("shell_schema_posix.json")),
            ShellFlavor::Cmd => crate::static_schema!(include_str!("shell_schema_cmd.json")),
        }
    }

    fn max_result_size_chars(&self) -> Option<usize> {
        Some(30_000)
    }

    fn timeout_policy(&self, args: &Value) -> ToolTimeout {
        match args.get("timeout_secs").and_then(Value::as_u64) {
            None | Some(0) => ToolTimeout::Unbounded,
            Some(secs) => ToolTimeout::Millis(secs.saturating_mul(1_000)),
        }
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }

    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.gate.external_effect_with_args(args)
    }

    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        self.gate.prepare_authorization(args).await
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(args, ToolCallOptions::default(), None)
            .await
    }

    async fn execute_with_context(
        &self,
        args: Value,
        _options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let operation = match self.operation(&args) {
            Ok(operation) => operation,
            Err(message) => return Ok(ToolResult::error(message)),
        };
        let timeout_secs = args.get("timeout_secs").and_then(Value::as_u64);
        let timeout = explicit_call_timeout_duration(timeout_secs, MAX_TIMEOUT_SECS);
        let authorization = ExecAuthorizationRequest {
            operation: operation.clone(),
            timeout,
        };
        let approval_required = self.gate.external_effect_with_args(&args);
        let started = Instant::now();
        let permit = match self.gate.authorize(&authorization, context).await {
            Ok(permit) => permit,
            Err(ExecDenied(reason)) => {
                self.gate.record(ExecAuditEvent {
                    operation,
                    approval_required,
                    authorized: false,
                    allowed: false,
                    backend_selection: None,
                    process_attempted: false,
                    outcome: ExecAuditOutcome::GateDenied,
                    duration: started.elapsed(),
                });
                return Ok(ToolResult::error(reason));
            }
        };

        let flavor = self.backend.shell_flavor();
        let command = match &authorization.operation {
            ExecOperation::Shell { command, .. } => command.clone(),
            ExecOperation::Install { .. } => unreachable!("ExecTool only handles shell operations"),
        };
        let (program, args): (OsString, Vec<OsString>) = match flavor {
            ShellFlavor::Posix => ("sh".into(), vec!["-c".into(), command.clone().into()]),
            ShellFlavor::Cmd => ("cmd.exe".into(), vec!["/C".into(), command.clone().into()]),
        };
        let backend_selection = permit.backend_selection.clone();
        let request = ExecRequest {
            program,
            args,
            action_dir: permit.action_dir,
            environment: python_environment(permit.environment),
            timeout,
            shell_command: Some(command),
            backend_selection: permit.backend_selection,
            backend_capability: permit.backend_capability,
        };
        let backend = Arc::clone(&self.backend);
        let outcome = with_exec_progress(context, HEARTBEAT_INTERVAL, |progress| async move {
            backend.execute(request, context, progress).await
        })
        .await;

        let (allowed, process_attempted, audit_outcome, result) = match outcome {
            ExecBackendOutcome::Refused { kind, reason } => (
                false,
                false,
                ExecAuditOutcome::BackendRefused { kind },
                ToolResult::error(reason),
            ),
            ExecBackendOutcome::LaunchFailed { error } => (
                true,
                true,
                ExecAuditOutcome::LaunchFailed,
                ToolResult::error(format!("Sandbox execution failed: {error}")),
            ),
            ExecBackendOutcome::Completed(output) => {
                let timed_out = output.timed_out;
                let result = exec_result(output, timeout);
                let success = !result.is_error;
                (
                    true,
                    true,
                    ExecAuditOutcome::Completed { success, timed_out },
                    result,
                )
            }
        };
        self.gate.record(ExecAuditEvent {
            operation,
            approval_required,
            authorized: true,
            allowed,
            backend_selection: Some(backend_selection),
            process_attempted,
            outcome: audit_outcome,
            duration: started.elapsed(),
        });
        Ok(result)
    }
}

#[cfg(test)]
#[path = "tool_tests.rs"]
mod tests;
