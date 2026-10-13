//! Command-center control verbs.
//!
//! The read-only projection in [`super::view`] shows what background agent work
//! is in flight; these verbs let a reviewer *act* on a single row. Each verb is
//! a durable transition on the run ledger (`crate::run_ledger`):
//!
//! - **stop** — cancel a non-terminal run (→ `cancelled`).
//! - **retry** — re-queue a finished-with-error run (`failed` / `cancelled` /
//!   `interrupted` → `pending`), clearing the stale error + completion time.
//! - **continue** — answer an `awaiting_user` run so it can resume (→ `running`).
//! - **follow_up** — record a follow-up instruction against any run, leaving its
//!   status unchanged (mirrors recording a parent→child message).
//!
//! These mirror the in-memory `AgentOrchestrationSession` control plane
//! (`close_agent` / `resume_agent` / `follow_up` / `message_agent`) but operate
//! on the *durable* ledger, so they survive restart and apply to any tracked
//! run rather than only the children of a live session. They persist the new
//! status (via [`transition_agent_run_status_from`], which can clear `error` /
//! `completed_at` — the upsert path cannot) and append a `run_event` recording
//! the action for the run's timeline, in one atomic commit that only applies
//! if the run is still in the status the verb was checked against. The verbs
//! are applied through
//! [`RunLedgerStore::apply_control`](crate::run_ledger::RunLedgerStore::apply_control).
//!
//! The allowed-transition matrix lives in the pure [`plan_transition`], which is
//! unit-tested without a database, mirroring [`super::view::build_view`].

use thiserror::Error;

use crate::run_ledger::AgentRunStatus;

/// A control action a reviewer can take on a command-center row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlVerb {
    /// Cancel an in-flight run.
    Stop,
    /// Re-queue a run that finished with an error.
    Retry,
    /// Answer an `awaiting_user` run so it can resume.
    Continue,
    /// Record a follow-up instruction against a run.
    FollowUp,
}

impl ControlVerb {
    /// Parse the wire `action` string. Returns `None` for an unknown verb.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "stop" => Some(Self::Stop),
            "retry" => Some(Self::Retry),
            "continue" => Some(Self::Continue),
            "follow_up" => Some(Self::FollowUp),
            _ => None,
        }
    }

    /// Stable wire string for this verb.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Retry => "retry",
            Self::Continue => "continue",
            Self::FollowUp => "follow_up",
        }
    }

    /// Whether this verb requires a non-empty user `message`.
    ///
    /// `continue` carries the answer that unblocks an `awaiting_user` run, and
    /// `follow_up` carries the new instruction — both are meaningless empty.
    pub fn requires_message(self) -> bool {
        matches!(self, Self::Continue | Self::FollowUp)
    }
}

/// Why a control verb could not be applied.
#[derive(Debug, Error)]
pub enum ControlError {
    /// No run matched the supplied id.
    #[error("agent run '{0}' not found")]
    RunNotFound(String),
    /// The verb is not legal from the run's current status.
    #[error("'{verb}' is not allowed while run is '{status}'")]
    InvalidTransition {
        verb: &'static str,
        status: &'static str,
    },
    /// The verb requires a message and none was supplied.
    #[error("'{0}' requires a non-empty message")]
    MessageRequired(&'static str),
    /// A durable run-ledger read/write failed.
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}

/// Lets `?` carry a run-ledger failure straight into [`ControlError`].
///
/// The ledger speaks `TinyAgentsError`, a distinct type from the
/// `anyhow::Error` in the `Storage` variant, so without this every ledger call
/// would need its own `map_err`.
impl From<tinyagents_harness::TinyAgentsError> for ControlError {
    fn from(err: tinyagents_harness::TinyAgentsError) -> Self {
        Self::Storage(err.into())
    }
}

/// The durable status a verb moves a run to, plus the event type to record.
///
/// `error` / `completed_at` handling is verb-specific and applied in
/// [`apply_control`]; only the status move + event name are decided here so the
/// transition legality stays purely testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ControlPlan {
    pub(super) target_status: AgentRunStatus,
    pub(super) event_type: &'static str,
}

/// Decide whether `verb` is legal from `current` and, if so, where it lands.
///
/// Pure: no I/O. The matrix is exhaustive on the verb so a new verb fails to
/// compile until its transition rule is decided.
pub(super) fn plan_transition(
    current: AgentRunStatus,
    verb: ControlVerb,
) -> Result<ControlPlan, ControlError> {
    let invalid = || ControlError::InvalidTransition {
        verb: verb.as_str(),
        status: current.as_str(),
    };
    match verb {
        // Stop only makes sense while the run is still live.
        ControlVerb::Stop => {
            if current.is_terminal() {
                Err(invalid())
            } else {
                Ok(ControlPlan {
                    target_status: AgentRunStatus::Cancelled,
                    event_type: "control_stopped",
                })
            }
        }
        // Retry re-queues a run that finished with an error (failed) or was
        // stopped (cancelled / interrupted). A successfully completed run has
        // nothing to retry; a live run is not yet retryable.
        ControlVerb::Retry => match current {
            AgentRunStatus::Failed | AgentRunStatus::Cancelled | AgentRunStatus::Interrupted => {
                Ok(ControlPlan {
                    target_status: AgentRunStatus::Pending,
                    event_type: "control_retry",
                })
            }
            _ => Err(invalid()),
        },
        // Continue answers a run that is explicitly blocked on the user.
        ControlVerb::Continue => {
            if current == AgentRunStatus::AwaitingUser {
                Ok(ControlPlan {
                    target_status: AgentRunStatus::Running,
                    event_type: "control_continued",
                })
            } else {
                Err(invalid())
            }
        }
        // Follow-up just records a new instruction; the run keeps its status
        // (you can follow up on a completed run as easily as a running one).
        ControlVerb::FollowUp => Ok(ControlPlan {
            target_status: current,
            event_type: "control_follow_up",
        }),
    }
}
