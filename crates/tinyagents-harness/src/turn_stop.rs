//! How a turn that "completed" was actually stopped by the harness.
//!
//! Three host-side mechanisms end a turn early while it still reaches the
//! normal completion path (`TurnCompleted` / `SubagentCompleted`), so without
//! this they read as clean completions in traces and to a delegating parent:
//!
//! * the repeated-failure **breaker** halts the run with a stop note;
//! * the **wind-down** at the turn deadline pauses the run before the backstop;
//! * the **iteration cap** stops a run whose last response still asked for
//!   tools.
//!
//! [`TurnStop`] carries that fact from where it is decided to the progress
//! events, the trace collector and the parent tool result. It is
//! content-free by construction: a typed breaker cause carries only stable
//! class and operation identifiers, never the quoted tool output or target.

/// Which mechanism stopped the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStopKind {
    /// The repeated-failure / classified-failure breaker halted the run.
    Breaker,
    /// The turn deadline's wind-down paused the run before the backstop.
    WindDown,
    /// The run reached its model-call cap with work still pending.
    IterationCap,
}

/// Stable typed class for the failure that caused a breaker stop.
///
/// The value comes from the failure classifier at the point the stop is
/// decided. It deliberately does not derive from the human-readable stop
/// note, whose wording may change independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnFailureClass(String);

impl TurnFailureClass {
    /// Construct a class from the classifier's stable identifier.
    pub fn new(class: impl Into<String>) -> Self {
        Self(identifier(&class.into()).unwrap_or_else(|| "unknown".to_owned()))
    }

    /// Stable identifier suitable for logs and host summaries.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TurnStopKind {
    /// Stable wire / trace label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Breaker => "breaker",
            Self::WindDown => "wind_down",
            Self::IterationCap => "iteration_cap",
        }
    }
}

/// A content-free description of how a turn was stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnStop {
    pub kind: TurnStopKind,
    /// Breaker only: the failure class (`uncertain_side_effect`,
    /// `permission`, …), or the stop-reason kind for notes without one
    /// (`repeated_failure`, `transient`, …).
    pub failure_class: Option<TurnFailureClass>,
    /// Breaker only: the blocked operation (a tool name), when the note names
    /// one.
    pub operation: Option<String>,
}

/// The `Incomplete` reason the sub-agent runner reports for an iteration-cap
/// stop. Shared so the parent can tell a cap from a breaker note.
pub const SUBAGENT_ITERATION_CAP_REASON: &str = "reached its tool-call limit before finishing";

impl TurnStop {
    /// A wind-down stop.
    pub fn wind_down() -> Self {
        Self {
            kind: TurnStopKind::WindDown,
            failure_class: None,
            operation: None,
        }
    }

    /// An iteration-cap stop.
    pub fn iteration_cap() -> Self {
        Self {
            kind: TurnStopKind::IterationCap,
            failure_class: None,
            operation: None,
        }
    }

    /// A legacy breaker stop using the caller's explicit fallback class.
    ///
    /// The note is retained only for the caller; it is not parsed. New code
    /// should pass the actual classified cause to [`Self::breaker_with_cause`].
    pub fn breaker(note: &str, fallback_class: impl FnOnce(&str) -> &'static str) -> Self {
        let key = fallback_class(note);
        let class = key.strip_prefix("turn_stop.").unwrap_or(key);
        Self::breaker_with_cause(note, TurnFailureClass::new(class), None)
    }

    /// A breaker stop carrying the classifier's typed cause. `note` is kept
    /// solely for the legacy human-readable summary; its text is never parsed.
    pub fn breaker_with_cause(
        _note: &str,
        failure_class: TurnFailureClass,
        operation: Option<String>,
    ) -> Self {
        Self {
            kind: TurnStopKind::Breaker,
            failure_class: Some(failure_class),
            operation: operation.as_deref().and_then(identifier),
        }
    }

    /// Build a stop directly from the typed classified-failure result.
    pub fn from_classified_cause(cause: crate::no_progress::ClassifiedStopCause) -> Self {
        Self::breaker_with_cause(
            &cause.summary,
            Self::failure_class(cause.class),
            Some(cause.operation),
        )
    }

    fn failure_class(class: String) -> TurnFailureClass {
        TurnFailureClass::new(class)
    }

    /// How a top-level run ended, from the outcome's flags. The breaker is the
    /// explicit cause and wins; a wind-down pause can also leave the run
    /// without a final response (which reads as a cap), so it beats the cap.
    pub fn classify(
        breaker_halt: Option<&str>,
        wind_down: bool,
        hit_cap: bool,
        fallback_class: impl FnOnce(&str) -> &'static str,
    ) -> Option<Self> {
        if let Some(note) = breaker_halt {
            return Some(Self::breaker(note, fallback_class));
        }
        if wind_down {
            return Some(Self::wind_down());
        }
        hit_cap.then(Self::iteration_cap)
    }

    /// Classify a turn using its typed breaker cause when one is available.
    pub fn classify_with_cause(
        breaker_halt: Option<&str>,
        failure_cause: Option<crate::no_progress::ClassifiedStopCause>,
        wind_down: bool,
        hit_cap: bool,
        fallback_class: impl FnOnce(&str) -> &'static str,
    ) -> Option<Self> {
        if let Some(cause) = failure_cause {
            return Some(Self::from_classified_cause(cause));
        }
        Self::classify(breaker_halt, wind_down, hit_cap, fallback_class)
    }

    /// The stop behind a sub-agent's `Incomplete { reason }`: the runner's
    /// cap reason, else the breaker note it carries verbatim.
    pub fn from_incomplete_reason(
        reason: &str,
        fallback_class: impl FnOnce(&str) -> &'static str,
    ) -> Self {
        if reason == SUBAGENT_ITERATION_CAP_REASON {
            Self::iteration_cap()
        } else {
            Self::breaker(reason, fallback_class)
        }
    }

    /// Content-free one-line summary, e.g.
    /// `stopped: breaker uncertain_side_effect on web_answer_tool`.
    pub fn status_message(&self) -> String {
        let mut out = format!("stopped: {}", self.kind.as_str());
        if let Some(class) = &self.failure_class {
            out.push(' ');
            out.push_str(class.as_str());
        }
        if let Some(operation) = &self.operation {
            out.push_str(" on ");
            out.push_str(operation);
        }
        out
    }
}

/// Keep only bounded machine identifiers in content-free stop summaries.
fn identifier(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.chars().count() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
    valid.then(|| value.to_owned())
}

#[cfg(test)]
#[path = "turn_stop_tests.rs"]
mod tests;
