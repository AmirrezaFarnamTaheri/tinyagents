use super::*;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::PathBuf,
    sync::{Mutex, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};
use crate::exec::{
    ExecAuditEvent, ExecAuditOutcome, ExecAuthorizationRequest, ExecBackend, ExecBackendOutcome,
    ExecDenied, ExecGate, ExecOperation, ExecOutput, ExecPermit, ExecProgress, ExecRequest,
    ExecRefusalKind, ShellFlavor,
};

#[derive(Default)]
struct GateState {
    denied: bool,
    events: Mutex<Vec<ExecAuditEvent>>,
    requests: Mutex<Vec<ExecAuthorizationRequest>>,
}

struct TestGate(Arc<GateState>);

#[async_trait]
impl ExecGate for TestGate {
    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        Ok(Some(json!({"prepared": args.get("command")})))
    }

    fn external_effect_with_args(&self, args: &Value) -> bool {
        args.get("command").and_then(Value::as_str).is_some_and(|command| !command.is_empty())
    }

    async fn authorize(
        &self,
        request: &ExecAuthorizationRequest,
        _context: Option<&dyn tinytools::ToolRunContext>,
    ) -> Result<ExecPermit, ExecDenied> {
        self.0.requests.lock().unwrap().push(request.clone());
        if self.0.denied {
            return Err(ExecDenied("blocked by test policy".to_owned()));
        }
        Ok(ExecPermit {
            action_dir: PathBuf::from("/authorized/action"),
            environment: BTreeMap::from([
                (OsString::from("KEEP"), OsString::from("provided")),
                (OsString::from("PYTHONUTF8"), OsString::from("0")),
            ]),
            backend_selection: "native-test".to_owned(),
            backend_capability: crate::exec::ExecBackendCapability::new("opaque-capability"),
        })
    }

    fn record(&self, event: ExecAuditEvent) {
        self.0.events.lock().unwrap().push(event);
    }
}

struct TestBackend {
    flavor: ShellFlavor,
    outcome: Mutex<ExecBackendOutcome>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ExecRequest>>,
}

impl TestBackend {
    fn new(flavor: ShellFlavor, outcome: ExecBackendOutcome) -> Self {
        Self { flavor, outcome: Mutex::new(outcome), calls: AtomicUsize::new(0), requests: Mutex::new(Vec::new()) }
    }
}

#[async_trait]
impl ExecBackend for TestBackend {
    fn shell_flavor(&self) -> ShellFlavor { self.flavor }

    async fn execute(
        &self,
        request: ExecRequest,
        _context: Option<&dyn tinytools::ToolRunContext>,
        _progress: Option<ExecProgress>,
    ) -> ExecBackendOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        self.outcome.lock().unwrap().clone()
    }
}

fn harness(
    denied: bool,
    flavor: ShellFlavor,
    outcome: ExecBackendOutcome,
) -> (ExecTool, Arc<GateState>, Arc<TestBackend>) {
    let state = Arc::new(GateState { denied, ..GateState::default() });
    let backend = Arc::new(TestBackend::new(flavor, outcome));
    let tool = ExecTool::new(Arc::new(TestGate(Arc::clone(&state))), backend.clone());
    (tool, state, backend)
}

fn completed(stdout: &[u8], stderr: &[u8], code: Option<i32>, timed_out: bool) -> ExecBackendOutcome {
    ExecBackendOutcome::Completed(ExecOutput {
        stdout: stdout.to_vec(), stderr: stderr.to_vec(), exit_code: code, timed_out,
    })
}

#[tokio::test]
async fn a_denied_gate_never_calls_the_backend() {
    let (tool, state, backend) = harness(
        true, ShellFlavor::Posix, completed(b"unexpected", b"", Some(0), false),
    );
    let result = tool.execute(json!({"command":"echo should-not-run"})).await.unwrap();
    assert!(result.is_error);
    assert_eq!(result.text(), "blocked by test policy");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert!(backend.requests.lock().unwrap().is_empty());
    let events = state.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, ExecAuditOutcome::GateDenied);
    assert!(!events[0].authorized);
    assert!(!events[0].allowed);
    assert!(!events[0].process_attempted);
}

#[tokio::test]
async fn authorized_shell_uses_the_gate_permit_and_records_success() {
    let (tool, state, backend) = harness(
        false, ShellFlavor::Posix, completed(b"done", b"", Some(0), false),
    );
    let result = tool.execute(json!({"command":"echo done", "category":"write", "timeout_secs":12})).await.unwrap();
    assert!(!result.is_error);
    assert_eq!(result.text(), "done");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let requests = backend.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].program, OsString::from("sh"));
    assert_eq!(requests[0].args, vec![OsString::from("-c"), OsString::from("echo done")]);
    assert_eq!(requests[0].action_dir, PathBuf::from("/authorized/action"));
    assert_eq!(requests[0].timeout, Some(Duration::from_secs(12)));
    assert_eq!(requests[0].shell_command.as_deref(), Some("echo done"));
    assert_eq!(requests[0].backend_selection, "native-test");
    assert_eq!(requests[0].environment.get(std::ffi::OsStr::new("KEEP")), Some(&OsString::from("provided")));
    assert_eq!(requests[0].environment.get(std::ffi::OsStr::new("PYTHONUTF8")), Some(&OsString::from("0")));
    assert_eq!(requests[0].environment.get(std::ffi::OsStr::new("PYTHONIOENCODING")), Some(&OsString::from("utf-8")));
    let events = state.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].approval_required);
    assert!(events[0].authorized);
    assert!(events[0].allowed);
    assert!(events[0].process_attempted);
    assert_eq!(events[0].backend_selection.as_deref(), Some("native-test"));
    assert_eq!(events[0].outcome, ExecAuditOutcome::Completed { success: true, timed_out: false });
}

#[tokio::test]
async fn cmd_flavor_uses_cmd_argv_and_matching_description() {
    let (tool, _, backend) = harness(false, ShellFlavor::Cmd, completed(b"ok", b"", Some(0), false));
    assert!(tool.description().contains("Windows cmd.exe"));
    assert_eq!(tool.parameters_schema()["properties"]["command"]["description"], "The cmd.exe command to execute (cmd syntax, not POSIX)");
    tool.execute(json!({"command":"echo ok"})).await.unwrap();
    let requests = backend.requests.lock().unwrap();
    assert_eq!(requests[0].program, OsString::from("cmd.exe"));
    assert_eq!(requests[0].args, vec![OsString::from("/C"), OsString::from("echo ok")]);
}

#[tokio::test]
async fn successful_stderr_is_returned_and_audited_as_success() {
    let (tool, state, _) = harness(false, ShellFlavor::Posix, completed(b"out", b"warning", Some(0), false));
    let result = tool.execute(json!({"command":"run"})).await.unwrap();
    assert!(!result.is_error);
    assert_eq!(result.text(), "out\n[stderr]\nwarning");
    assert_eq!(state.events.lock().unwrap()[0].outcome, ExecAuditOutcome::Completed { success: true, timed_out: false });
}

#[tokio::test]
async fn command_failure_and_timeout_are_audited_as_failed_completions() {
    for (output, expected) in [
        (completed(b"out", b"err", Some(7), false), ExecAuditOutcome::Completed { success: false, timed_out: false }),
        (completed(b"partial", b"", None, true), ExecAuditOutcome::Completed { success: false, timed_out: true }),
    ] {
        let (tool, state, _) = harness(false, ShellFlavor::Posix, output);
        let result = tool.execute(json!({"command":"run", "timeout_secs":5})).await.unwrap();
        assert!(result.is_error);
        assert_eq!(state.events.lock().unwrap()[0].outcome, expected);
    }
}

#[tokio::test]
async fn backend_refusal_is_not_reported_as_allowed_execution() {
    let (tool, state, backend) = harness(
        false, ShellFlavor::Posix,
        ExecBackendOutcome::Refused { kind: ExecRefusalKind::Preflight, reason: "sandbox unavailable".into() },
    );
    let result = tool.execute(json!({"command":"run"})).await.unwrap();
    assert!(result.is_error);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let event = &state.events.lock().unwrap()[0];
    assert!(event.authorized);
    assert!(!event.allowed);
    assert!(!event.process_attempted);
    assert_eq!(event.outcome, ExecAuditOutcome::BackendRefused { kind: ExecRefusalKind::Preflight });
}

#[tokio::test]
async fn launch_failure_is_audited_after_process_attempt() {
    let (tool, state, _) = harness(
        false, ShellFlavor::Posix,
        ExecBackendOutcome::LaunchFailed { error: "spawn denied".into() },
    );
    let result = tool.execute(json!({"command":"run"})).await.unwrap();
    assert!(result.is_error);
    assert!(result.text().contains("Sandbox execution failed: spawn denied"));
    let event = &state.events.lock().unwrap()[0];
    assert!(event.authorized);
    assert!(event.allowed);
    assert!(event.process_attempted);
    assert_eq!(event.outcome, ExecAuditOutcome::LaunchFailed);
}

#[tokio::test]
async fn authorization_preparation_and_external_effect_classification_delegate_to_gate() {
    let (tool, _, _) = harness(false, ShellFlavor::Posix, completed(b"", b"", Some(0), false));
    let args = json!({"command":"echo"});
    assert_eq!(tool.prepare_authorization(&args).await.unwrap(), Some(json!({"prepared":"echo"})));
    assert!(tool.external_effect_with_args(&args));
    assert!(!tool.external_effect_with_args(&json!({"command":""})));
}

#[tokio::test]
async fn missing_command_fails_without_authorization_or_audit() {
    let (tool, state, backend) = harness(false, ShellFlavor::Posix, completed(b"", b"", Some(0), false));
    assert!(tool.execute(json!({})).await.unwrap().is_error);
    assert!(state.requests.lock().unwrap().is_empty());
    assert!(state.events.lock().unwrap().is_empty());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn tool_metadata_and_timeout_policy_are_stable() {
    let (tool, _, _) = harness(false, ShellFlavor::Posix, completed(b"", b"", Some(0), false));
    assert_eq!(tool.name(), "exec");
    assert_eq!(tool.permission_level(), PermissionLevel::Execute);
    assert_eq!(tool.max_result_size_chars(), Some(30_000));
    assert_eq!(tool.timeout_policy(&json!({})), ToolTimeout::Unbounded);
    assert_eq!(tool.timeout_policy(&json!({"timeout_secs":0})), ToolTimeout::Unbounded);
    assert_eq!(tool.timeout_policy(&json!({"timeout_secs":12})), ToolTimeout::Millis(12_000));
    let schema = tool.parameters_schema();
    assert_eq!(schema["required"], json!(["command"]));
    assert_eq!(schema["properties"]["timeout_secs"]["maximum"], 3600);
    assert_eq!(schema["properties"]["category"]["enum"], json!(["read", "write", "network", "install", "destructive"]));
}
