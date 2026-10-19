//! Process utilities and gated execution tools.

pub mod detect_tools;
pub mod ports;
pub mod progress;
mod shell_description;
mod shell_support;
pub mod timeout;

pub use ports::{
    ExecAuditEvent, ExecAuditOutcome, ExecAuthorizationRequest, ExecBackend, ExecBackendOutcome,
    ExecBackendCapability, ExecDenied, ExecGate, ExecOperation, ExecOutput, ExecPermit, ExecRefusalKind,
    ExecRequest, ShellFlavor,
};
pub use progress::ExecProgress;
mod tool;
pub use tool::ExecTool;

#[cfg(test)]
#[path = "ports_tests.rs"]
mod ports_tests;

