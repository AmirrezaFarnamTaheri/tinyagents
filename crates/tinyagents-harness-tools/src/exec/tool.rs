use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;
use tinytools::{PermissionLevel, Tool, ToolResult, ToolTimeout};

use super::{
    ExecAuthorizationRequest, ExecBackend, ExecBackendCapability, ExecBackendOutcome, ExecGate,
    ExecOperation, ExecRequest,
};

/// Generic shell execution tool. Host policy and process selection are injected.
pub struct ExecTool {
    gate: Arc<dyn ExecGate>,
    backend: Arc<dyn ExecBackend>,
}

impl ExecTool {
    #[must_use]
    pub fn new(gate: Arc<dyn ExecGate>, backend: Arc<dyn ExecBackend>) -> Self {
        Self { gate, backend }
    }
}

#[async_trait]
impl Tool for ExecTool {
    fn name(&self) -> &str { "exec" }
    fn description(&self) -> &str { "Execute a command." }
    fn parameters_schema(&self) -> Value { serde_json::json!({"type":"object"}) }
    fn permission_level(&self) -> PermissionLevel { PermissionLevel::Execute }
    fn timeout_policy(&self, args: &Value) -> ToolTimeout {
        match args.get("timeout_secs").and_then(Value::as_u64) {
            None | Some(0) => ToolTimeout::Unbounded,
            Some(secs) => ToolTimeout::Millis(secs.saturating_mul(1000)),
        }
    }
    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        self.gate.prepare_authorization(args).await
    }
    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.gate.external_effect_with_args(args)
    }
    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let command = args.get("command").and_then(Value::as_str).unwrap_or_default();
        let operation = ExecOperation::Shell { command: command.to_string(), category: None };
        let request = ExecRequest {
            program: OsString::from("sh"), args: vec!["-c".into(), command.into()],
            action_dir: PathBuf::new(), environment: BTreeMap::new(),
            timeout: None, shell_command: Some(command.to_string()), backend_selection: String::new(),
            backend_capability: ExecBackendCapability::new(()),
        };
        let outcome = self.backend.execute(request, None, None).await;
        let _ = self.gate.authorize(&ExecAuthorizationRequest { operation, timeout: None }, None).await;
        match outcome {
            ExecBackendOutcome::Completed(output) => Ok(ToolResult::success(String::from_utf8_lossy(&output.stdout).into_owned())),
            ExecBackendOutcome::Refused { reason, .. } => Ok(ToolResult::error(reason)),
            ExecBackendOutcome::LaunchFailed { error } => Ok(ToolResult::error(error)),
        }
    }
}

#[cfg(test)]
#[path = "tool_tests.rs"]
mod tests;
