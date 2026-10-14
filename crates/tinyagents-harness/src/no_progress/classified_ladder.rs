//! The classified-failure escalation ladder: what a failed call's recovery
//! class does once the host's [`RecoveryPolicy`](super::RecoveryPolicy) has
//! named it.
//!
//! [`ClassifiedFailureTracker`] counts attempts against a budget. This module
//! is the step around it: record the failure, and either halt with the
//! tracker's root-cause summary, steer the model once with the class's
//! corrective instruction, or stay silent. A success clears the blockers it
//! proves gone. The host keeps the classification table, the per-run state and
//! the steering; this keeps the class vocabulary and what each class means.

use super::{ClassifiedFailure, ClassifiedFailureTracker, NoProgress, first_error_line};

/// Every recovery class the ladder knows. A success of `(tool, scope)` clears
/// the blocker of each class for that operation and scope.
pub const RECOVERY_CLASSES: &[&str] = &[
    "permission",
    "authentication",
    "site_refused",
    "policy",
    "blocked_by_policy",
    "task_failed",
    "unsupported",
    "missing_window",
    "missing_app",
    "not_found",
    "transient",
    "uncertain_side_effect",
    "validation",
    "unavailable",
    "service_refused",
    "invalid_arguments",
];

/// Suffix appended to the halt summary of an `uncertain_side_effect` failure.
pub const UNCERTAIN_SIDE_EFFECT_HALT_SUFFIX: &str =
    " The action may already have happened; reconcile its external state before any retry.";

/// What one classified failure asks of the driver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClassifiedStep {
    /// The budget is spent: halt the run with this root-cause summary.
    Halt(String),
    /// Within budget; steer the next model request with this instruction.
    Nudge(String),
    /// Within budget and the class has no instruction of its own.
    Within,
}

/// Whether a class gets a corrective instruction while it is within budget.
pub fn class_has_nudge(class: &str) -> bool {
    matches!(
        class,
        "missing_window"
            | "missing_app"
            | "validation"
            | "invalid_arguments"
            | "uncertain_side_effect"
            | "unavailable"
            | "service_refused"
            | "blocked_by_policy"
            | "task_failed"
    )
}

/// The corrective instruction for `class`, from the failed `tool` and the first
/// line of its (already scrubbed) failure text. `None` for a class without one.
///
/// `missing_app` reads as the generic desktop-target wording; a host that knows
/// which shell it runs under replaces it for a shell command.
pub fn recovery_nudge(class: &str, tool: &str, failure_line: &str) -> Option<String> {
    Some(match class {
        "service_refused" => format!(
            "The `{tool}` tool cannot be used in this session: the service refused the request ({failure_line}). Do not call `{tool}` again; continue with your other tools."
        ),
        "blocked_by_policy" => format!(
            "The `{tool}` call was blocked by policy and did not run ({failure_line}). Try one narrower, permitted alternative (a scoped path, a bounded command, a read instead of a write) or continue with other tools; do not resend it unchanged. Another refusal of this operation ends the turn."
        ),
        "task_failed" => format!(
            "The `{tool}` task ended without finishing ({failure_line}). Follow its hint once: change the goal, inputs or starting point it names, or continue with other tools. Do not rerun it unchanged."
        ),
        "validation" => "The last call failed validation. Correct its schema or arguments once before trying again.".to_owned(),
        "invalid_arguments" => format!(
            "The `{tool}` call was rejected before it ran: its arguments did not match the tool's schema ({failure_line}). Read the tool's parameters and correct the call; do not resend it unchanged."
        ),
        "uncertain_side_effect" => "The last command timed out and was killed; it may have partly run. Check its effect before repeating anything, then retry at most once as a smaller, bounded step (fewer items per call, a per-item timeout such as `timeout 5`, or background it and poll).".to_owned(),
        "unavailable" => format!(
            "The `{tool}` tool is unavailable for the rest of this run: a module it needs failed to load and will not recover until the app restarts. Do not call `{tool}` again; continue with your other tools."
        ),
        "missing_window" | "missing_app" => "The desktop target was not found. Rediscover the current app and window once before trying again.".to_owned(),
        _ => return None,
    })
}

/// Record one classified failure of `tool` on `scope` and decide the step.
///
/// `budget` is the retries the class allows after the first failure. The halt
/// summary is the tracker's, with the reconcile-first suffix for an
/// `uncertain_side_effect`. `failure_text` should already be scrubbed.
pub fn classified_step(
    tracker: &ClassifiedFailureTracker,
    class: &str,
    tool: &str,
    scope: &str,
    budget: usize,
    failure_text: &str,
) -> ClassifiedStep {
    let key = ClassifiedFailure::new(class, tool, scope);
    if let NoProgress::Halt(mut summary) = tracker.record(&key, budget) {
        if class == "uncertain_side_effect" {
            summary.push_str(UNCERTAIN_SIDE_EFFECT_HALT_SUFFIX);
        }
        return ClassifiedStep::Halt(summary);
    }
    if class_has_nudge(class) {
        let line = first_error_line(failure_text);
        if let Some(instruction) = recovery_nudge(class, tool, &line) {
            return ClassifiedStep::Nudge(instruction);
        }
    }
    ClassifiedStep::Within
}

/// Clear every class's blocker for `(tool, scope)`: only a success against that
/// operation and scope shows its blocker changed.
pub fn clear_recovered(tracker: &ClassifiedFailureTracker, tool: &str, scope: &str) {
    tracker.clear_operation(tool, scope);
}

/// A tool-owned JSON error contract (`status_code` / `status` / `code`) read as
/// a recovery decision. Only explicit fields count; arbitrary response data is
/// not a failure signal. `None` when `error` is not such a contract.
pub fn json_error_recovery(error: &str) -> Option<super::RecoveryDecision> {
    use super::RecoveryDecision as D;
    let value: serde_json::Value = serde_json::from_str(error).ok()?;
    let status = ["/status_code", "/status", "/error/status_code"]
        .iter()
        .find_map(|p| value.pointer(p).and_then(serde_json::Value::as_u64));
    match status {
        Some(401) => return Some(D::new("authentication", 0)),
        Some(403) => return Some(D::new("permission", 0)),
        Some(400 | 422) => return Some(D::new("validation", 1)),
        Some(429 | 500 | 502 | 503 | 504) => return Some(D::new("transient", 2)),
        _ => {}
    }
    let code = ["/code", "/error/code"]
        .iter()
        .find_map(|p| value.pointer(p).and_then(serde_json::Value::as_str));
    match code {
        Some("PERMISSION_DENIED") => Some(D::new("permission", 0)),
        Some("UNAUTHENTICATED") => Some(D::new("authentication", 0)),
        Some("INVALID_ARGUMENT") => Some(D::new("validation", 1)),
        Some("WINDOW_NOT_FOUND") => Some(D::new("missing_window", 1)),
        Some("APP_NOT_FOUND") => Some(D::new("missing_app", 1)),
        Some("UNIMPLEMENTED") => Some(D::new("unsupported", 0)),
        Some("UNAVAILABLE" | "RESOURCE_EXHAUSTED") => Some(D::new("transient", 2)),
        _ => None,
    }
}

#[cfg(test)]
#[path = "classified_ladder_tests.rs"]
mod tests;
