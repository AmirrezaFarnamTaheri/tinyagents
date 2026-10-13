//! Harness [`RunPolicy`] construction for an interactive agent turn, plus the
//! wall-clock ceilings and model-call cap normalisation that shape it.
//!
//! The host reads its own configuration (environment variables, config file)
//! and hands the raw override strings in through [`TurnPolicyOverrides`]; this
//! module owns the defaults, the parsing and the policy they produce, so every
//! host gets the same ceilings. Nothing here reads the process environment.

use crate::retry::RetryPolicy;
use crate::runtime::{InvalidArgsPolicy, PayloadCapture, RunPolicy, UnknownToolPolicy};

/// Default per-turn wall-clock ceiling, in seconds. A pure runaway guard over
/// the whole turn: hang detection is the per-call ceiling below, so a long
/// productive turn is not handed a shrinking remainder as its next call's
/// budget.
pub const DEFAULT_AGENT_TURN_TIMEOUT_SECS: u64 = 3_600;

/// Default wall-clock ceiling for a **single model call**, in seconds. Every
/// call (and every retry attempt) gets a fresh `min(this, turn remainder)`
/// budget. Deliberately generous: a hidden-reasoning call can be app-silent
/// for several minutes, so this is a backstop, not a latency target. Tool
/// calls are exempt by design.
pub const DEFAULT_MODEL_CALL_TIMEOUT_SECS: u64 = 900;

/// Per-model-call ceiling for a local / self-hosted provider, in seconds. A
/// local model prefills the whole prompt before its first token, which takes
/// many minutes on consumer hardware for a large prompt.
pub const LOCAL_MODEL_CALL_TIMEOUT_SECS: u64 = 3_600;

/// Per-turn ceiling for a local / self-hosted provider, in seconds. Must stay
/// above [`LOCAL_MODEL_CALL_TIMEOUT_SECS`].
pub const LOCAL_AGENT_TURN_TIMEOUT_SECS: u64 = 14_400;

/// Extra seconds an outer backstop allows a local turn beyond the harness turn
/// ceiling, so the harness's own (more informative) timeout fires first.
pub const LOCAL_WEB_TURN_BACKSTOP_GRACE_SECS: u64 = 300;

/// Default silence allowed between output events of a streaming call, in
/// seconds, once the first output has arrived.
pub const DEFAULT_STREAM_IDLE_TIMEOUT_SECS: u64 = 120;

/// Default circuit breaker: consecutive stream idle timeouts on one model
/// before retrying that model stops.
pub const DEFAULT_MAX_CONSECUTIVE_STREAM_IDLE_TIMEOUTS: usize = 5;

/// Legacy default model-call cap used when a caller passes `0` for "unset".
/// Passing `0` straight through would abort before the first provider call.
pub const DEFAULT_MAX_ITERATIONS: usize = 10;

/// Raw host overrides for the ceilings, exactly as the host read them (for
/// example from environment variables). `None` or an unparseable value means
/// "use the default"; a `0` disables the ceiling.
#[derive(Debug, Clone, Default)]
pub struct TurnPolicyOverrides {
    /// Per-turn wall-clock ceiling, seconds.
    pub turn_timeout_secs: Option<String>,
    /// Per-model-call ceiling, seconds.
    pub model_call_timeout_secs: Option<String>,
    /// Stream idle window, seconds.
    pub stream_idle_timeout_secs: Option<String>,
    /// Stream first-event window, seconds. Off unless set positive.
    pub stream_first_event_timeout_secs: Option<String>,
    /// Consecutive stream idle timeouts before the breaker trips.
    pub max_consecutive_stream_idle_timeouts: Option<String>,
}

/// What a host asks of [`run_policy`].
#[derive(Debug, Clone)]
pub struct TurnPolicyOptions {
    /// Model-call cap (the iteration limit). Normalise with
    /// [`effective_max_iterations`] first.
    pub max_model_calls: usize,
    /// Tool-call cap for the turn.
    pub max_tool_calls: usize,
    /// Sub-agent recursion depth cap.
    pub max_depth: usize,
    /// Whether the turn's provider is local / self-hosted (longer ceilings).
    pub local_provider: bool,
    /// Enables the response cache. Interactive turns pass `false`.
    pub response_cache_enabled: bool,
    /// Host overrides for the ceilings.
    pub overrides: TurnPolicyOverrides,
}

/// Normalise a caller-supplied iteration cap: `0` means "unset".
#[must_use]
pub fn effective_max_iterations(max_iterations: usize) -> usize {
    if max_iterations == 0 {
        DEFAULT_MAX_ITERATIONS
    } else {
        max_iterations
    }
}

fn parse_secs_ms(value: Option<&str>, default_secs: u64) -> Option<u64> {
    let secs = value
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default_secs);
    (secs > 0).then(|| secs.saturating_mul(1_000))
}

/// Per-turn wall-clock ceiling in milliseconds. `0` yields `None` (no
/// ceiling); absent or unparseable falls back to the hosted or local default.
#[must_use]
pub fn turn_wall_clock_ms(override_secs: Option<&str>, local: bool) -> Option<u64> {
    parse_secs_ms(
        override_secs,
        if local {
            LOCAL_AGENT_TURN_TIMEOUT_SECS
        } else {
            DEFAULT_AGENT_TURN_TIMEOUT_SECS
        },
    )
}

/// Per-model-call ceiling in milliseconds, with the same override rules as
/// [`turn_wall_clock_ms`].
#[must_use]
pub fn model_call_wall_clock_ms(override_secs: Option<&str>, local: bool) -> Option<u64> {
    parse_secs_ms(
        override_secs,
        if local {
            LOCAL_MODEL_CALL_TIMEOUT_SECS
        } else {
            DEFAULT_MODEL_CALL_TIMEOUT_SECS
        },
    )
}

/// Outer backstop for a turn on a local provider: the local turn ceiling plus
/// [`LOCAL_WEB_TURN_BACKSTOP_GRACE_SECS`], or `None` when the turn ceiling is
/// disabled. It must sit above the harness ceiling or it pre-empts it.
#[must_use]
pub fn local_turn_backstop_secs(turn_override_secs: Option<&str>) -> Option<u64> {
    turn_wall_clock_ms(turn_override_secs, true)
        .map(|ms| ms / 1_000 + LOCAL_WEB_TURN_BACKSTOP_GRACE_SECS)
}

/// Stream idle window in milliseconds. `0` disables.
#[must_use]
pub fn stream_idle_timeout_ms(override_secs: Option<&str>) -> Option<u64> {
    parse_secs_ms(override_secs, DEFAULT_STREAM_IDLE_TIMEOUT_SECS)
}

/// First-event window in milliseconds. Off (`None`) unless set to a positive
/// value: reasoning-hidden and local models stay silent for minutes before
/// the first token.
#[must_use]
pub fn stream_first_event_timeout_ms(override_secs: Option<&str>) -> Option<u64> {
    override_secs
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(|secs| secs.saturating_mul(1_000))
}

/// Consecutive stream idle timeouts before the breaker trips. `0` disables.
#[must_use]
pub fn max_consecutive_stream_idle_timeouts(override_value: Option<&str>) -> Option<usize> {
    let n = override_value
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CONSECUTIVE_STREAM_IDLE_TIMEOUTS);
    (n > 0).then_some(n)
}

/// Write the three stream-silence limits from raw override values.
pub fn apply_stream_limits(
    limits: &mut crate::limits::RunLimits,
    idle: Option<&str>,
    first_event: Option<&str>,
    breaker: Option<&str>,
) {
    limits.stream_idle_timeout_ms = stream_idle_timeout_ms(idle);
    limits.stream_first_event_timeout_ms = stream_first_event_timeout_ms(first_event);
    limits.max_consecutive_stream_idle_timeouts = max_consecutive_stream_idle_timeouts(breaker);
}

/// Consecutive identical tool failures that trip the repeated-failure circuit
/// breaker.
pub const REPEATED_TOOL_FAILURE_THRESHOLD: usize = 3;

/// The retry schedule for interactive turns: 4 retries over about 45 s
/// (3/6/12/24 s with jitter), so a throttled upstream is outlasted. The floor
/// under worst-case jitter is about 33.75 s.
#[must_use]
pub fn turn_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 5,
        initial_backoff_ms: 3_000,
        max_backoff_ms: 30_000,
        multiplier: 2.0,
        jitter: true,
        backoff_sleep: true,
        max_retry_after_ms: RetryPolicy::DEFAULT_MAX_RETRY_AFTER_MS,
        retry_on: None,
    }
}

/// Build the harness [`RunPolicy`] for an interactive agent turn.
///
/// The loop enforces limits from the harness policy (not the per-run
/// `RunConfig`), so the model-call cap must be set here or it falls back to
/// the harness default. Retry is owned by the harness (see
/// [`turn_retry_policy`]); `max_retries_per_call` is raised alongside it
/// because the loop clamps `max_attempts` to it, so raising one alone would
/// change nothing.
#[must_use]
pub fn run_policy(options: &TurnPolicyOptions) -> RunPolicy {
    let overrides = &options.overrides;
    let mut policy = RunPolicy::default();
    // A streaming response can finish with reasoning but no visible answer.
    // Reissue that unusable call once; if it repeats, the empty-response error
    // stays actionable.
    policy.empty_response_retries = 1;
    policy.limits.max_model_calls = options.max_model_calls;
    policy.limits.max_tool_calls = options.max_tool_calls;
    policy.limits.max_depth = options.max_depth;
    // With no deadline the harness awaits each call UNBOUNDED, so a wedged
    // model stream or delegated sub-agent would park the loop with no terminal
    // event. A turn ceiling arms the per-call timeout.
    policy.limits.max_wall_clock_ms =
        turn_wall_clock_ms(overrides.turn_timeout_secs.as_deref(), options.local_provider);
    // Per-call ceiling: hang detection scoped to the one call that wedged.
    policy.limits.max_model_call_ms = model_call_wall_clock_ms(
        overrides.model_call_timeout_secs.as_deref(),
        options.local_provider,
    );
    apply_stream_limits(
        &mut policy.limits,
        overrides.stream_idle_timeout_secs.as_deref(),
        overrides.stream_first_event_timeout_secs.as_deref(),
        overrides.max_consecutive_stream_idle_timeouts.as_deref(),
    );
    // Each executed tool row ends with `[took 12.3s]`, so the model can budget
    // the rest of the turn against the ceiling.
    policy.tool_result_durations = true;
    policy.limits.max_retries_per_call = 4;
    policy.retry = turn_retry_policy();
    // `ReturnToolError` rather than `Rewrite`: it injects a recoverable
    // "unknown tool" result naming the requested tool, which hosts classify.
    policy.unknown_tool = UnknownToolPolicy::ReturnToolError;
    // Schema-invalid arguments become a corrective tool error rather than
    // aborting the run, after schema-guided coercion of JSON-encoded nested
    // objects some providers emit.
    policy.invalid_args = InvalidArgsPolicy::NormalizeThenReturnToolError;
    // Prompt-prefix protection: the cache-guard records layout changes and the
    // loop injects a stable-prefix cache key into the provider options.
    policy.cache.protect_prompt_prefix = true;
    // Interactive turns must never be served a cached model response.
    policy.cache.response_cache_enabled = options.response_cache_enabled;
    policy.capture = PayloadCapture::all();
    policy
}

#[cfg(test)]
#[path = "turn_policy_tests.rs"]
mod tests;
