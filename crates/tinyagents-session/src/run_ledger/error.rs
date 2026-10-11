//! Typed failures for the run-ledger storage contract.
//!
//! Existing SQLite APIs still return `TinyAgentsError`. This new contract wraps
//! those errors without flattening them, and retains native driver error kinds
//! when the optional driver implementation is enabled.

/// The result returned by a bound run-ledger operation.
pub type LedgerResult<T> = std::result::Result<T, LedgerError>;

/// A run-ledger failure with its owning implementation's detail preserved.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LedgerError {
    /// An existing SQLite/session operation failed.
    #[error(transparent)]
    Legacy(Box<tinyagents_harness::TinyAgentsError>),
    /// A command-center control was invalid or its classic operation failed.
    #[error(transparent)]
    Control(Box<super::command_center::ControlError>),
    /// A driver operation failed, retaining capability/conflict/admission kinds.
    #[cfg(feature = "storage-drivers")]
    #[error(transparent)]
    Driver(Box<tinystoragedrivers_core::StorageError>),
}

impl From<tinyagents_harness::TinyAgentsError> for LedgerError {
    fn from(error: tinyagents_harness::TinyAgentsError) -> Self {
        Self::Legacy(Box::new(error))
    }
}

impl From<super::command_center::ControlError> for LedgerError {
    fn from(error: super::command_center::ControlError) -> Self {
        Self::Control(Box::new(error))
    }
}

#[cfg(feature = "storage-drivers")]
impl From<tinystoragedrivers_core::StorageError> for LedgerError {
    fn from(error: tinystoragedrivers_core::StorageError) -> Self {
        Self::Driver(Box::new(error))
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
