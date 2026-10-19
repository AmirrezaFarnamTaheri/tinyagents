use std::{
    ffi::OsString,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde_json::Value;
use tinytools::{PermissionLevel, Tool, ToolCallOptions, ToolResult, ToolRunContext};

use super::{
    ExecAuditEvent, ExecAuditOutcome, ExecAuthorizationRequest, ExecBackend, ExecBackendOutcome,
    ExecGate, ExecOperation, ExecRequest, detect_tools::find_on_path,
};

const INSTALL_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_OUTPUT_BYTES: usize = 1_048_576;

pub struct InstallTool {
    gate: Arc<dyn ExecGate>,
    backend: Arc<dyn ExecBackend>,
}

impl InstallTool {
    #[must_use]
    pub fn new(gate: Arc<dyn ExecGate>, backend: Arc<dyn ExecBackend>) -> Self {
        Self { gate, backend }
    }
}

fn manager_argv(manager: &str) -> Option<(&'static str, &'static [&'static str])> {
    Some(match manager {
        "apt" | "apt-get" => ("apt-get", &["install", "-y"]),
        "dnf" => ("dnf", &["install", "-y"]),
        "yum" => ("yum", &["install", "-y"]),
        "pacman" => ("pacman", &["-S", "--noconfirm"]),
        "apk" => ("apk", &["add"]),
        "brew" => ("brew", &["install"]),
        "winget" => ("winget", &["install", "-e", "--id"]),
        "pipx" => ("pipx", &["install"]),
        "npm" => ("npm", &["install", "-g"]),
        "cargo" => ("cargo", &["install"]),
        _ => return None,
    })
}

fn valid_package(package: &str) -> bool {
    !package.is_empty()
        && package.len() <= 200
        && package.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '+' | '-' | '@' | '/' | ':')
        })
}

fn truncate_stream(bytes: &[u8], label: &str) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= MAX_OUTPUT_BYTES {
        return text.into_owned();
    }
    let mut end = MAX_OUTPUT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n... [{label} truncated at 1 MiB]", &text[..end])
}

#[async_trait]
impl Tool for InstallTool {
    fn name(&self) -> &str {
        "install_tool"
    }
    fn description(&self) -> &str {
        "Install an OS or language package on the host (apt/dnf/pacman/apk/brew/winget/pipx/npm -g/cargo). HIGH IMPACT: host policy must require explicit user approval. Prefer detect_tools first; only install what's actually missing."
    }
    fn parameters_schema(&self) -> Value {
        crate::static_schema!(include_str!("install_schema.json"))
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Dangerous
    }
    fn external_effect(&self) -> bool {
        true
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
        let Some(package) = args.get("package").and_then(Value::as_str).map(str::trim) else {
            return Ok(ToolResult::error("Missing 'package' parameter"));
        };
        if !valid_package(package) {
            return Ok(ToolResult::error(format!(
                "Invalid package name '{package}': only letters, digits and ._+-@/: are allowed"
            )));
        }
        let manager = match args.get("manager").and_then(Value::as_str) {
            Some(manager) => manager.to_owned(),
            None => ["apt-get", "dnf", "yum", "pacman", "apk", "brew", "winget"]
                .into_iter()
                .find(|name| find_on_path(name).is_some())
                .unwrap_or("")
                .to_owned(),
        };
        if manager.is_empty() {
            return Ok(ToolResult::error(
                "No supported package manager detected on PATH. Pass 'manager' explicitly (e.g. brew, pipx, npm, cargo).",
            ));
        }
        let Some((program, prefix)) = manager_argv(&manager) else {
            return Ok(ToolResult::error(format!(
                "Unsupported package manager '{manager}'"
            )));
        };
        if find_on_path(program).is_none() {
            return Ok(ToolResult::error(format!(
                "Package manager '{program}' is not installed on PATH"
            )));
        }
        let operation = ExecOperation::Install {
            package: package.to_owned(),
            manager: manager.clone(),
        };
        let authorization = ExecAuthorizationRequest {
            operation: operation.clone(),
            timeout: Some(INSTALL_TIMEOUT),
        };
        let started = Instant::now();
        let approval_required = self.gate.external_effect_with_args(&args);
        let permit = match self.gate.authorize(&authorization, context).await {
            Ok(permit) => permit,
            Err(denied) => {
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
                return Ok(ToolResult::error(denied.0));
            }
        };
        let backend_selection = permit.backend_selection.clone();
        let mut argv = prefix.iter().map(OsString::from).collect::<Vec<_>>();
        argv.push(package.into());
        let request = ExecRequest {
            program: program.into(),
            args: argv,
            action_dir: permit.action_dir,
            environment: permit.environment,
            timeout: Some(INSTALL_TIMEOUT),
            shell_command: None,
            backend_selection: permit.backend_selection,
            backend_capability: permit.backend_capability,
        };
        let outcome = self.backend.execute(request, context, None).await;
        let (allowed, attempted, audit, result) = match outcome {
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
                ToolResult::error(format!("Failed to launch {program}: {error}")),
            ),
            ExecBackendOutcome::Completed(output) if output.timed_out => (
                true,
                true,
                ExecAuditOutcome::Completed {
                    success: false,
                    timed_out: true,
                },
                ToolResult::error("Install timed out after 300s and was killed"),
            ),
            ExecBackendOutcome::Completed(output) => {
                let stdout = truncate_stream(&output.stdout, "stdout");
                let stderr = truncate_stream(&output.stderr, "stderr");
                let result = if output.exit_code == Some(0) {
                    ToolResult::success(format!("Installed '{package}' via {program}.\n{stdout}"))
                } else {
                    ToolResult::error(format!(
                        "{program} failed to install '{package}' (exit {:?}).\n{}",
                        output.exit_code,
                        if stderr.is_empty() { stdout } else { stderr }
                    ))
                };
                let success = !result.is_error;
                (
                    true,
                    true,
                    ExecAuditOutcome::Completed {
                        success,
                        timed_out: false,
                    },
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
            process_attempted: attempted,
            outcome: audit,
            duration: started.elapsed(),
        });
        Ok(result)
    }
}

#[cfg(test)]
#[path = "install_tool_tests.rs"]
mod tests;
