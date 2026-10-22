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

use crate::close::{CloseToolResult, interpolate_once};
use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Host-provided prose and line templates for deterministic stop summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopCopy {
    pub opening_template: &'static str,
    pub what_happened_heading: &'static str,
    pub success_template: &'static str,
    pub failure_template: &'static str,
    pub hidden_calls_template: &'static str,
    pub proceed_prompt: &'static str,
}

/// English renderer copy for stopped-turn summaries.
pub const ENGLISH_STOP_COPY: StopCopy = StopCopy {
    opening_template: "I stopped this turn early because {reason}.\n",
    what_happened_heading: "\n**What happened**\n",
    success_template: "- `{name}` succeeded{count}",
    failure_template: "- `{name}` failed{count}{error}",
    hidden_calls_template: "- …and {count} more tool call(s)",
    proceed_prompt: "\nTell me how you'd like to proceed, or ask me to try again.",
};

/// Summary row grouped by operation and scrubbed result text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopSummaryGroup {
    pub name: String,
    pub success: bool,
    pub error: Option<String>,
    pub count: usize,
}

/// Group repeated results after the host has scrubbed failure text.
pub fn group_stop_results<F>(
    results: &[CloseToolResult],
    mut scrubbed_error_line: F,
) -> Vec<StopSummaryGroup>
where
    F: FnMut(&str) -> String,
{
    let mut groups: Vec<StopSummaryGroup> = Vec::new();
    for result in results {
        let error = if result.success {
            None
        } else {
            Some(scrubbed_error_line(&result.content)).filter(|line| !line.is_empty())
        };
        if let Some(group) = groups
            .iter_mut()
            .find(|g| g.name == result.name && g.success == result.success && g.error == error)
        {
            group.count += 1;
        } else {
            groups.push(StopSummaryGroup {
                name: result.name.clone(),
                success: result.success,
                error,
                count: 1,
            });
        }
    }
    groups
}

/// Render a bounded stop summary with caller-selected product copy.
pub fn render_stop_summary_with_copy(
    reason: &str,
    groups: &[StopSummaryGroup],
    copy: &StopCopy,
) -> String {
    use std::fmt::Write as _;
    let mut out = interpolate_once(copy.opening_template, &[("{reason}", reason)]);
    if !groups.is_empty() {
        out.push_str(copy.what_happened_heading);
        let shown_from = groups.len().saturating_sub(8);
        for group in groups.iter().skip(shown_from) {
            let name = truncate_stop_name(&group.name, 60);
            let count = if group.count == 1 {
                String::new()
            } else {
                format!(" {} times", group.count)
            };
            let line = if group.success {
                interpolate_once(
                    copy.success_template,
                    &[("{name}", &name), ("{count}", &count)],
                )
            } else {
                let error = group
                    .error
                    .as_ref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default();
                interpolate_once(
                    copy.failure_template,
                    &[("{name}", &name), ("{count}", &count), ("{error}", &error)],
                )
            };
            let _ = writeln!(out, "{line}");
        }
        let hidden: usize = groups
            .iter()
            .take(shown_from)
            .map(|group| group.count)
            .sum();
        if hidden > 0 {
            let hidden = hidden.to_string();
            let _ = writeln!(
                out,
                "{}",
                interpolate_once(copy.hidden_calls_template, &[("{count}", &hidden)])
            );
        }
    }
    out.push_str(copy.proceed_prompt);
    out
}

fn truncate_stop_name(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((idx, _)) => format!("{}...", text[..idx].trim_end()),
        None => text.to_owned(),
    }
}

/// Which mechanism stopped the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

impl std::ops::Deref for TurnFailureClass {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
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

impl Serialize for TurnStop {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let failure_class = self.failure_class.as_ref().map(TurnFailureClass::as_str);
        let operation = self.operation.as_deref().and_then(identifier_ref);
        let mut state = serializer.serialize_struct("TurnStop", 3)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("failure_class", &failure_class)?;
        state.serialize_field("operation", &operation)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for TurnStop {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SerializedTurnStop {
            kind: TurnStopKind,
            #[serde(default)]
            failure_class: Option<String>,
            #[serde(default)]
            operation: Option<String>,
        }

        let serialized = SerializedTurnStop::deserialize(deserializer)?;
        let failure_class = serialized
            .failure_class
            .map(|class| {
                identifier(&class)
                    .map(TurnFailureClass)
                    .ok_or_else(|| D::Error::custom("invalid turn-stop failure class"))
            })
            .transpose()?;
        let operation = serialized
            .operation
            .map(|operation| {
                identifier(&operation)
                    .ok_or_else(|| D::Error::custom("invalid turn-stop operation"))
            })
            .transpose()?;
        Ok(Self {
            kind: serialized.kind,
            failure_class,
            operation,
        })
    }
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
        if let Some(operation) = self.operation.as_deref().and_then(identifier_ref) {
            out.push_str(" on ");
            out.push_str(operation);
        }
        out
    }

    /// Returns a copy whose public string field has been reduced to a safe
    /// machine identifier. Callers may construct `TurnStop` directly because
    /// its fields are public, so projection boundaries must normalize it.
    pub(crate) fn sanitized(&self) -> Self {
        Self {
            kind: self.kind,
            failure_class: self.failure_class.clone(),
            operation: self.operation.as_deref().and_then(identifier),
        }
    }
}

/// Keep only bounded machine identifiers in content-free stop summaries.
fn identifier(value: &str) -> Option<String> {
    identifier_ref(value).map(str::to_owned)
}

fn identifier_ref(value: &str) -> Option<&str> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.chars().count() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
    valid.then_some(value)
}

#[cfg(test)]
#[path = "turn_stop_tests.rs"]
mod tests;
