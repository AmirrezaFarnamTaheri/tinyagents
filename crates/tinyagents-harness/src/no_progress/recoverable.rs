//! The recoverable-failure ladder and the recovery-class mechanism.
//!
//! Transient failures (timeouts, resets, rate limits, 5xx) get more headroom
//! than the deterministic ladder in [`NoProgressTracker`](super::NoProgressTracker):
//! an identical call may be retried [`LoopGuardThresholds::recoverable_repeat_failure`]
//! times and a run of consecutive failures with varied arguments
//! [`LoopGuardThresholds::recoverable_no_progress_failure`] times before the
//! ladder halts. The counting is mechanism; what the halt *says* is policy,
//! supplied by a [`FailurePolicy`].
//!
//! [`RecoveryPolicy`] is the matching seam for classifying a failed call into a
//! `(class, retry budget)` pair that [`ClassifiedFailureTracker`](super::ClassifiedFailureTracker)
//! then counts. The host owns the table (it reads host tool groups and
//! platform shell facts); the pure text predicates it shares live here.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::middleware::library::LoopGuardThresholds;

/// Wording for the halts a [`RecoverableFailureLadder`] raises.
///
/// Implemented by the host so the user-visible copy stays its own.
pub trait FailurePolicy: Send + Sync {
    /// `tool` failed identically `count` times; `failure_text` is the latest.
    fn identical_halt_summary(&self, tool: &str, count: u32, failure_text: &str) -> String;
    /// `consecutive` recoverable failures in a row, the latest from `tool`.
    fn no_progress_halt_summary(&self, consecutive: u32, tool: &str, failure_text: &str) -> String;
}

/// Counts recoverable failures per `(tool, argument fingerprint)` and in a
/// consecutive streak, and reports when the extended headroom is spent.
pub struct RecoverableFailureLadder {
    thresholds: LoopGuardThresholds,
    identical: Mutex<HashMap<String, u32>>,
    consecutive: AtomicU32,
}

impl Default for RecoverableFailureLadder {
    fn default() -> Self {
        Self::new(LoopGuardThresholds::default())
    }
}

impl RecoverableFailureLadder {
    /// A ladder with the given thresholds.
    pub fn new(thresholds: LoopGuardThresholds) -> Self {
        Self {
            thresholds,
            identical: Mutex::new(HashMap::new()),
            consecutive: AtomicU32::new(0),
        }
    }

    /// Clear the consecutive streak (any success or non-recoverable failure).
    /// The per-signature counts persist for the run.
    pub fn reset_streak(&self) {
        self.consecutive.store(0, Ordering::SeqCst);
    }

    /// Clear everything, for a new run.
    pub fn reset(&self) {
        self.reset_streak();
        if let Ok(mut counts) = self.identical.lock() {
            counts.clear();
        }
    }

    /// Record one recoverable failure. Returns the policy's halt summary once
    /// the identical count reaches `recoverable_repeat_failure` or the streak
    /// reaches `recoverable_no_progress_failure`.
    pub fn record(
        &self,
        policy: &dyn FailurePolicy,
        tool: &str,
        arg_fingerprint: &str,
        failure_text: &str,
    ) -> Option<String> {
        let key = format!("{tool}\u{1f}{arg_fingerprint}");
        let count = self
            .identical
            .lock()
            .ok()
            .map(|mut counts| {
                let c = counts.entry(key).or_insert(0);
                *c += 1;
                *c
            })
            .unwrap_or(0);
        let consecutive = self.consecutive.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::debug!(
            tool,
            count,
            consecutive,
            "[no_progress] recoverable failure recorded with extended headroom"
        );
        if count >= self.thresholds.recoverable_repeat_failure {
            return Some(policy.identical_halt_summary(tool, count, failure_text));
        }
        if consecutive >= self.thresholds.recoverable_no_progress_failure {
            return Some(policy.no_progress_halt_summary(consecutive, tool, failure_text));
        }
        None
    }
}

/// How a failed call is budgeted: its class and the retries it is allowed
/// after the first failure (see [`ClassifiedFailureTracker::record`](super::ClassifiedFailureTracker::record)).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryDecision {
    /// Stable class name, part of the ledger key.
    pub class: &'static str,
    /// Retries allowed after the first failure; zero stops on the first.
    pub budget: usize,
}

impl RecoveryDecision {
    /// A decision with `class` and `budget`.
    pub const fn new(class: &'static str, budget: usize) -> Self {
        Self { class, budget }
    }
}

/// Classifies a failed tool call. `None` leaves it to the exact-repeat guard.
pub trait RecoveryPolicy: Send + Sync {
    /// `tool` failed with `error`; `body_level_failure` marks a failure
    /// reported inside a successful result body.
    fn classify(&self, tool: &str, error: &str, body_level_failure: bool)
    -> Option<RecoveryDecision>;
}

/// Retries allowed for a tool rejecting its own arguments against its schema:
/// nothing ran and the refusal carries the expected schema, so a changed
/// attempt most likely lands. Bounded; an unchanged repeat is still caught by
/// the generic ladder.
pub const ARGUMENT_SCHEMA_RECOVERY: usize = 3;

/// Prefix of the shared renderer for a command that ran and did not exit 0.
pub const COMMAND_EXIT_REPORT_PREFIX: &str = "Command failed (";

/// Whether `error` is a finished command's exit report rather than a failure
/// of the tool itself.
pub fn is_command_exit_report(error: &str) -> bool {
    error.trim_start().starts_with(COMMAND_EXIT_REPORT_PREFIX)
}

/// Whether `error` reports a failure from before the tool did anything: its
/// runtime could not be resolved or built, or its process never spawned.
pub fn is_pre_execution_failure(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    [
        "runtime unavailable",
        "runtime resolution",
        "failed to build runtime command",
        "failed to spawn",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
}

/// Whether `error` is the tool rejecting its arguments before running.
pub fn is_argument_rejection(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("schema validation") || lower.contains("invalid arguments")
}

/// The first line of a failure, plus the first stderr line when the text is a
/// command exit report, bounded to 200 characters. The caller scrubs `text`
/// first; this function does not.
pub fn first_error_line(text: &str) -> String {
    let first = text
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim();
    let stderr = text
        .split_once("[stderr]\n")
        .and_then(|(_, tail)| tail.lines().find(|l| !l.trim().is_empty()))
        .map(str::trim)
        .filter(|l| !l.is_empty() && *l != first);
    let line = match stderr {
        Some(err) => format!("{first} — {err}"),
        None => first.to_owned(),
    };
    line.chars().take(200).collect()
}

#[cfg(test)]
#[path = "recoverable_tests.rs"]
mod tests;
