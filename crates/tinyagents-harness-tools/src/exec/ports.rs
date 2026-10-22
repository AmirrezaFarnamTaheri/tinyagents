use std::{
    any::Any, collections::BTreeMap, ffi::OsString, path::PathBuf, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use serde_json::Value;
use tinytools::ToolRunContext;

use super::ExecProgress;

/// A concrete operation presented to the host's execution policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOperation {
    Shell {
        command: String,
        category: Option<String>,
    },
    Install {
        package: String,
        manager: String,
    },
}

#[derive(Debug, Clone)]
pub struct ExecAuthorizationRequest {
    pub operation: ExecOperation,
    pub timeout: Option<Duration>,
}

/// Opaque host-owned data selected during authorization.
#[derive(Clone)]
pub struct ExecBackendCapability(Arc<dyn Any + Send + Sync>);

impl ExecBackendCapability {
    #[must_use]
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Arc::new(value))
    }

    #[must_use]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.as_ref().downcast_ref()
    }
}

#[derive(Clone)]
pub struct ExecPermit {
    pub action_dir: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
    pub backend_selection: String,
    pub backend_capability: ExecBackendCapability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecDenied(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecRefusalKind {
    BackendUnavailable,
    Configuration,
    Preflight,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecAuditOutcome {
    GateDenied,
    BackendRefused { kind: ExecRefusalKind },
    LaunchFailed,
    Completed { success: bool, timed_out: bool },
}

#[derive(Debug, Clone)]
pub struct ExecAuditEvent {
    pub operation: ExecOperation,
    pub approval_required: bool,
    pub authorized: bool,
    pub allowed: bool,
    pub backend_selection: Option<String>,
    pub process_attempted: bool,
    pub outcome: ExecAuditOutcome,
    pub duration: Duration,
}

#[derive(Clone)]
pub struct ExecRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub action_dir: PathBuf,
    pub environment: BTreeMap<OsString, OsString>,
    pub timeout: Option<Duration>,
    pub shell_command: Option<String>,
    pub backend_selection: String,
    pub backend_capability: ExecBackendCapability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecBackendOutcome {
    Refused {
        kind: ExecRefusalKind,
        reason: String,
    },
    LaunchFailed {
        error: String,
    },
    Completed(ExecOutput),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellFlavor {
    Posix,
    Cmd,
}

#[async_trait]
pub trait ExecGate: Send + Sync {
    async fn prepare_authorization(&self, args: &Value) -> anyhow::Result<Option<Value>>;
    fn external_effect_with_args(&self, args: &Value) -> bool;
    async fn authorize(
        &self,
        request: &ExecAuthorizationRequest,
        context: Option<&dyn ToolRunContext>,
    ) -> Result<ExecPermit, ExecDenied>;
    fn record(&self, event: ExecAuditEvent);
}

#[async_trait]
pub trait ExecBackend: Send + Sync {
    fn shell_flavor(&self) -> ShellFlavor;
    async fn execute(
        &self,
        request: ExecRequest,
        context: Option<&dyn ToolRunContext>,
        progress: Option<ExecProgress>,
    ) -> ExecBackendOutcome;
}
