use super::*;
use crate::exec::{
    ExecBackendCapability, ExecDenied, ExecOutput, ExecPermit, ExecProgress, ShellFlavor,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct State {
    denied: bool,
    events: Mutex<Vec<ExecAuditEvent>>,
    backend_calls: AtomicUsize,
    request: Mutex<Option<ExecRequest>>,
}
struct Gate(Arc<State>);

#[async_trait]
impl ExecGate for Gate {
    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>> {
        Ok(Some(json!({"prepared_package":args.get("package")})))
    }
    fn external_effect_with_args(&self, args: &Value) -> bool {
        args.get("gate_effect")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
    async fn authorize(
        &self,
        _: &ExecAuthorizationRequest,
        _: Option<&dyn ToolRunContext>,
    ) -> Result<ExecPermit, ExecDenied> {
        if self.0.denied {
            return Err(ExecDenied("installation denied".into()));
        }
        Ok(ExecPermit {
            action_dir: PathBuf::from("/tmp"),
            environment: BTreeMap::new(),
            backend_selection: "test".into(),
            backend_capability: ExecBackendCapability::new(()),
        })
    }
    fn record(&self, event: ExecAuditEvent) {
        self.0.events.lock().unwrap().push(event);
    }
}
struct Backend(Arc<State>);

#[async_trait]
impl ExecBackend for Backend {
    fn shell_flavor(&self) -> ShellFlavor {
        ShellFlavor::Posix
    }
    async fn execute(
        &self,
        request: ExecRequest,
        _: Option<&dyn ToolRunContext>,
        _: Option<ExecProgress>,
    ) -> ExecBackendOutcome {
        self.0.backend_calls.fetch_add(1, Ordering::SeqCst);
        *self.0.request.lock().unwrap() = Some(request);
        ExecBackendOutcome::Completed(ExecOutput {
            stdout: b"ok".to_vec(),
            stderr: Vec::new(),
            exit_code: Some(0),
            timed_out: false,
        })
    }
}

fn tool(denied: bool) -> (InstallTool, Arc<State>) {
    let state = Arc::new(State {
        denied,
        ..State::default()
    });
    (
        InstallTool::new(
            Arc::new(Gate(state.clone())),
            Arc::new(Backend(state.clone())),
        ),
        state,
    )
}

#[test]
fn package_validation_rejects_shell_injection_and_oversized_input() {
    assert!(!valid_package("pkg; touch /tmp/pwned"));
    assert!(!valid_package(&"a".repeat(201)));
    for package in ["ripgrep", "@scope/cli", "lib:feature", "a+b-c_1.0"] {
        assert!(valid_package(package));
    }
}

#[tokio::test]
async fn install_delegates_authorization_preparation_and_argument_effect_to_gate() {
    let (tool, _) = tool(false);
    let args = json!({"package":"ripgrep","gate_effect":false});
    assert_eq!(
        tool.prepare_authorization(&args).await.unwrap(),
        Some(json!({"prepared_package":"ripgrep"}))
    );
    assert!(!tool.external_effect_with_args(&args));
    assert!(tool.external_effect_with_args(&json!({"package":"ripgrep","gate_effect":true})));
}

#[test]
fn every_supported_manager_keeps_its_exact_argument_prefix() {
    let cases = [
        ("apt", "apt-get", &["install", "-y"][..]),
        ("apt-get", "apt-get", &["install", "-y"][..]),
        ("dnf", "dnf", &["install", "-y"][..]),
        ("yum", "yum", &["install", "-y"][..]),
        ("pacman", "pacman", &["-S", "--noconfirm"][..]),
        ("apk", "apk", &["add"][..]),
        ("brew", "brew", &["install"][..]),
        ("winget", "winget", &["install", "-e", "--id"][..]),
        ("pipx", "pipx", &["install"][..]),
        ("npm", "npm", &["install", "-g"][..]),
        ("cargo", "cargo", &["install"][..]),
    ];
    for (manager, program, prefix) in cases {
        let (actual_program, actual_prefix) = manager_argv(manager).unwrap();
        assert_eq!(actual_program, program);
        assert_eq!(actual_prefix, prefix);
    }
    assert_eq!(manager_argv("unknown"), None);
}

#[test]
fn package_output_truncation_is_per_stream_and_utf8_safe() {
    let text = format!("{}é", "a".repeat(MAX_OUTPUT_BYTES - 1));
    assert_eq!(text.len(), MAX_OUTPUT_BYTES + 1);
    let truncated = truncate_stream(text.as_bytes(), "stdout");
    assert!(truncated.ends_with("\n... [stdout truncated at 1 MiB]"));
    assert!(truncated.is_char_boundary(truncated.len()));
    assert!(!truncated[..MAX_OUTPUT_BYTES].ends_with('�'));
}

#[tokio::test]
async fn invalid_package_never_reaches_gate_or_backend() {
    let (tool, state) = tool(false);
    let result = tool
        .execute(json!({"package":"pkg; touch /tmp/pwned","manager":"cargo"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(state.backend_calls.load(Ordering::SeqCst), 0);
    assert!(state.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn denied_install_never_calls_backend_and_records_denial() {
    let (tool, state) = tool(true);
    let result = tool
        .execute(json!({"package":"ripgrep","manager":"cargo"}))
        .await
        .unwrap();
    assert!(result.is_error);
    assert_eq!(result.text(), "installation denied");
    assert_eq!(state.backend_calls.load(Ordering::SeqCst), 0);
    let events = state.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, ExecAuditOutcome::GateDenied);
    assert!(!events[0].process_attempted);
}

#[tokio::test]
async fn install_uses_exact_manager_argv_and_no_shell_command() {
    let (tool, state) = tool(false);
    let result = tool
        .execute(json!({"package":"@scope/cli","manager":"cargo"}))
        .await
        .unwrap();
    assert!(!result.is_error);
    let request = state.request.lock().unwrap();
    let request = request.as_ref().unwrap();
    assert_eq!(request.program, OsString::from("cargo"));
    assert_eq!(
        request.args,
        vec![OsString::from("install"), OsString::from("@scope/cli")]
    );
    assert_eq!(request.shell_command, None);
    assert_eq!(request.timeout, Some(INSTALL_TIMEOUT));
    assert_eq!(state.events.lock().unwrap().len(), 1);
}
