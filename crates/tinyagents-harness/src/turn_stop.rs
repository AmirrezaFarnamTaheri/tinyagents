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

use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

    /// Stable user-copy key for this classifier identifier.
    ///
    /// The mapping is intentionally exact and does not inspect any stop
    /// summary or translated copy. Unknown classes use the generic key.
    pub fn copy_key(&self) -> TurnStopCopyKey {
        match self.as_str() {
            "transient" => TurnStopCopyKey::Transient,
            "uncertain_side_effect" => TurnStopCopyKey::UncertainSideEffect,
            "authentication" => TurnStopCopyKey::Authentication,
            "permission" => TurnStopCopyKey::Permission,
            "policy" | "blocked_by_policy" => TurnStopCopyKey::Policy,
            "missing_program" | "unsupported" | "missing_app" => TurnStopCopyKey::MissingProgram,
            "not_found" | "missing_window" => TurnStopCopyKey::NotFound,
            "invalid_calls" | "validation" | "invalid_arguments" => TurnStopCopyKey::InvalidCalls,
            "unavailable" | "service_refused" => TurnStopCopyKey::Unavailable,
            "site_refused" => TurnStopCopyKey::SiteRefused,
            "repeated_failure" => TurnStopCopyKey::RepeatedFailure,
            "varied_failures" => TurnStopCopyKey::VariedFailures,
            "repeating_step" => TurnStopCopyKey::RepeatingStep,
            "out_of_credits" => TurnStopCopyKey::OutOfCredits,
            "provider_config" => TurnStopCopyKey::ProviderConfig,
            "missing_connection" => TurnStopCopyKey::MissingConnection,
            _ => TurnStopCopyKey::Generic,
        }
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

/// Stable host localization key for a terminal turn-stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TurnStopCopyKey {
    Transient,
    UncertainSideEffect,
    Authentication,
    Permission,
    Policy,
    MissingProgram,
    NotFound,
    InvalidCalls,
    Unavailable,
    SiteRefused,
    RepeatedFailure,
    VariedFailures,
    RepeatingStep,
    OutOfCredits,
    ProviderConfig,
    MissingConnection,
    Generic,
}

impl TurnStopCopyKey {
    /// Stable localization key used by host translation tables.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "turn_stop.transient",
            Self::UncertainSideEffect => "turn_stop.uncertain_side_effect",
            Self::Authentication => "turn_stop.authentication",
            Self::Permission => "turn_stop.permission",
            Self::Policy => "turn_stop.policy",
            Self::MissingProgram => "turn_stop.missing_program",
            Self::NotFound => "turn_stop.not_found",
            Self::InvalidCalls => "turn_stop.invalid_calls",
            Self::Unavailable => "turn_stop.unavailable",
            Self::SiteRefused => "turn_stop.site_refused",
            Self::RepeatedFailure => "turn_stop.repeated_failure",
            Self::VariedFailures => "turn_stop.varied_failures",
            Self::RepeatingStep => "turn_stop.repeating_step",
            Self::OutOfCredits => "turn_stop.out_of_credits",
            Self::ProviderConfig => "turn_stop.provider_config",
            Self::MissingConnection => "turn_stop.missing_connection",
            Self::Generic => "turn_stop.generic",
        }
    }
}

/// Typed copy request shared by turn stops and stop-hook halt builders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CopyRequest<'a> {
    TurnStop {
        key: TurnStopCopyKey,
        operation: Option<&'a str>,
    },
    RetryExhausted {
        tool: &'a str,
        count: u32,
        last_error: &'a str,
    },
    NoProgress {
        tool: &'a str,
        consecutive: u32,
        last_error: &'a str,
    },
    Budget {
        tool: &'a str,
        last_error: &'a str,
    },
    ProviderConfig {
        tool: &'a str,
        last_error: &'a str,
    },
}

/// Optional host localization for generated stop copy.
///
/// Returning `None` for a request uses the harness's neutral English default,
/// so hosts can override only the keys they localize.
pub trait CopyProvider: Send + Sync {
    /// Return host-provided copy for a typed stop request, if available.
    fn copy_override(&self, _request: &CopyRequest<'_>) -> Option<String> {
        None
    }
}

/// Uses only the neutral English defaults for stop copy.
#[derive(Debug, Default, Clone, Copy)]
pub struct NeutralCopyProvider;

impl CopyProvider for NeutralCopyProvider {}

/// Bound detail embedded in a halt summary to 600 Unicode scalar values.
pub fn truncate_for_halt(text: &str) -> String {
    truncate_with_suffix(text, 600, "\n… [truncated]")
}

pub(crate) fn truncate_with_suffix(text: &str, max_chars: usize, suffix: &str) -> String {
    if text.chars().count() <= max_chars {
        text.to_owned()
    } else {
        format!(
            "{}{suffix}",
            text.chars().take(max_chars).collect::<String>()
        )
    }
}

/// Render a request using the host override when present, otherwise neutral
/// library-owned English copy.
pub fn render_copy(provider: &dyn CopyProvider, request: &CopyRequest<'_>) -> String {
    let bounded_error = match request {
        CopyRequest::RetryExhausted { last_error, .. }
        | CopyRequest::NoProgress { last_error, .. }
        | CopyRequest::Budget { last_error, .. }
        | CopyRequest::ProviderConfig { last_error, .. } => Some(truncate_for_halt(last_error)),
        CopyRequest::TurnStop { .. } => None,
    };
    let bounded_request = match (request, bounded_error.as_deref()) {
        (CopyRequest::RetryExhausted { tool, count, .. }, Some(last_error)) => {
            CopyRequest::RetryExhausted {
                tool,
                count: *count,
                last_error,
            }
        }
        (
            CopyRequest::NoProgress {
                tool, consecutive, ..
            },
            Some(last_error),
        ) => CopyRequest::NoProgress {
            tool,
            consecutive: *consecutive,
            last_error,
        },
        (CopyRequest::Budget { tool, .. }, Some(last_error)) => {
            CopyRequest::Budget { tool, last_error }
        }
        (CopyRequest::ProviderConfig { tool, .. }, Some(last_error)) => {
            CopyRequest::ProviderConfig { tool, last_error }
        }
        (CopyRequest::TurnStop { key, operation }, _) => CopyRequest::TurnStop {
            key: *key,
            operation: *operation,
        },
        _ => unreachable!("detail request has bounded detail"),
    };
    if let Some(copy) = provider.copy_override(&bounded_request) {
        return copy;
    }

    match &bounded_request {
        CopyRequest::TurnStop { key, .. } => {
            let reason = match key {
                TurnStopCopyKey::Transient => {
                    "a service I needed kept timing out or was temporarily unavailable"
                }
                TurnStopCopyKey::UncertainSideEffect => {
                    "an action timed out and I could not confirm whether it went through, so I did not repeat it"
                }
                TurnStopCopyKey::Authentication => "a service rejected my credentials",
                TurnStopCopyKey::Permission => "I don't have permission for a step this needs",
                TurnStopCopyKey::Policy => "a step this needs is blocked by your security settings",
                TurnStopCopyKey::MissingProgram => {
                    "a program or feature this needs is not available on this device"
                }
                TurnStopCopyKey::NotFound => "something this needs could not be found",
                TurnStopCopyKey::InvalidCalls => "a tool kept rejecting my requests as invalid",
                TurnStopCopyKey::Unavailable => "a tool this needs is not available right now",
                TurnStopCopyKey::SiteRefused => "a website kept refusing my requests",
                TurnStopCopyKey::RepeatedFailure => "the same step kept failing",
                TurnStopCopyKey::VariedFailures => "several different tool calls in a row failed",
                TurnStopCopyKey::RepeatingStep => {
                    "I kept repeating the same step without getting anything new"
                }
                TurnStopCopyKey::OutOfCredits => {
                    "the account is out of inference credits; add credits, then ask me to try again"
                }
                TurnStopCopyKey::ProviderConfig => {
                    "the configured model or provider rejected the request; check the model and API key"
                }
                TurnStopCopyKey::MissingConnection => {
                    "a service this needs isn't connected; connect it, then ask me to try again"
                }
                TurnStopCopyKey::Generic => "my tool calls were not getting anywhere",
            };
            reason.to_owned()
        }
        CopyRequest::RetryExhausted {
            tool,
            count,
            last_error,
        } => format!(
            "Stopping: the `{tool}` call was retried {count} times with identical arguments and kept \
             failing — repeating it will not help. Last error:\n{}\n\nThis looked recoverable at \
             first, but the same call exhausted the extended transient-failure headroom. Report this \
             back instead of retrying.",
            last_error,
        ),
        CopyRequest::NoProgress {
            tool,
            consecutive,
            last_error,
        } => format!(
            "Stopping: {consecutive} recoverable-looking tool failures happened in a row with no \
             successful progress. Last error (from `{tool}`):\n{}\n\nThe turn is still bounded by the \
             iteration/cost limits, but this many consecutive transient failures means the goal is \
             not currently reachable. Report this back instead of retrying.",
            last_error,
        ),
        CopyRequest::Budget { tool, last_error } => format!(
            "Stopping: the `{tool}` step failed because the account is out of inference budget/credits \
             — every retry hits the same wall. Add credits to your account (or, when using a custom/BYO \
             provider, top up that provider's own account) and try again. Details:\n{}",
            last_error,
        ),
        CopyRequest::ProviderConfig { tool, last_error } => format!(
            "Stopping: the `{tool}` step failed because the configured model/provider rejected the request \
             (e.g. an unknown model, a non-chat/embedding model, a missing credential, or a region block) \
             — retrying will not help. Fix the model or API key, then try again. Details:\n{}",
            last_error,
        ),
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

    /// Render user-facing copy from this stop's typed class.
    ///
    /// Wind-down and iteration-cap stops have no failure class and use the
    /// generic key. This display copy is deliberately separate from the
    /// content-free [`Self::status_message`] and serialized representation.
    pub fn user_copy(&self, provider: &dyn CopyProvider) -> String {
        render_copy(
            provider,
            &CopyRequest::TurnStop {
                key: self
                    .failure_class
                    .as_ref()
                    .map_or(TurnStopCopyKey::Generic, TurnFailureClass::copy_key),
                operation: self.operation.as_deref().and_then(identifier_ref),
            },
        )
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
