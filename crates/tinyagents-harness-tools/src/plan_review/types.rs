//! Wire-compatible plan-review decisions and internal tool outcomes.

use serde::{Deserialize, Serialize};

/// The user's decision on a parked plan, plus internal tool outcomes preserved
/// for the generic plan-review tool's established result behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum PlanReviewResolution {
    /// Run the plan as proposed.
    Approve,
    /// Internal outcome when no interactive review surface exists.
    #[serde(skip)]
    NonInteractiveApprove,
    /// Internal outcome when the current run is not in plan mode.
    #[serde(skip)]
    NotApplicable,
    /// Drop the plan.
    Reject,
    /// Revise the plan using user feedback.
    Revise { feedback: String },
}

impl PlanReviewResolution {
    /// Stable decision label for events and logs; revise feedback is omitted.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
            Self::Revise { .. } => "revise",
            Self::NonInteractiveApprove => "non_interactive_approve",
            Self::NotApplicable => "not_applicable",
        }
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
