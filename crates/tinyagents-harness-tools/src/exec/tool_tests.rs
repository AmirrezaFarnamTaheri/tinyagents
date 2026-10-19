use super::*;
use crate::exec::{ExecAuditEvent, ExecBackendOutcome, ExecDenied, ExecOutput, ExecPermit, ExecProgress, ExecRequest, ExecAuthorizationRequest, ExecGate, ExecBackend, ShellFlavor};
use std::sync::atomic::{AtomicUsize, Ordering};

struct DenyingGate;

#[async_trait]
impl ExecGate for DenyingGate {
    async fn prepare_authorization(&self, _args: &Value) -> anyhow::Result<Option<Value>> { Ok(None) }
    fn external_effect_with_args(&self, _args: &Value) -> bool { true }
    async fn authorize(
        &self,
        _request: &ExecAuthorizationRequest,
        _context: Option<&dyn tinytools::ToolRunContext>,
    ) -> Result<ExecPermit, ExecDenied> { Err(ExecDenied("blocked by test policy".into())) }
    fn record(&self, _event: ExecAuditEvent) {}
}

struct CountingBackend(AtomicUsize);

#[async_trait]
impl ExecBackend for CountingBackend {
    fn shell_flavor(&self) -> ShellFlavor { ShellFlavor::Posix }
    async fn execute(
        &self,
        _request: ExecRequest,
        _context: Option<&dyn tinytools::ToolRunContext>,
        _progress: Option<ExecProgress>,
    ) -> ExecBackendOutcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        ExecBackendOutcome::Completed(ExecOutput {
            stdout: b"ran".to_vec(), stderr: Vec::new(), exit_code: Some(0), timed_out: false,
        })
    }
}

#[tokio::test]
async fn a_denied_gate_never_calls_the_backend() {
    let backend = Arc::new(CountingBackend(AtomicUsize::new(0)));
    let tool = ExecTool::new(Arc::new(DenyingGate), backend.clone());
    let result = tool.execute(serde_json::json!({"command":"echo should-not-run"})).await.unwrap();
    assert!(result.is_error, "a denied command must return an error");
    assert_eq!(backend.0.load(Ordering::SeqCst), 0, "denial must precede backend dispatch");
}
