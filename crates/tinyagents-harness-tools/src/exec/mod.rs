//! Process utilities and gated execution tools.

pub mod detect_tools;
pub mod ports;
pub mod progress;
pub mod timeout;

pub use ports::{
    ExecAuditEvent, ExecAuditOutcome, ExecAuthorizationRequest, ExecBackend, ExecBackendOutcome,
    ExecBackendCapability, ExecDenied, ExecOperation, ExecOutput, ExecPermit, ExecRefusalKind,
    ExecRequest, ShellFlavor,
};
pub use progress::ExecProgress;

#[cfg(test)]
#[path = "ports_tests.rs"]
mod ports_tests;

#[cfg(test)]
#[path = "progress_tests.rs"]
mod progress_tests;
