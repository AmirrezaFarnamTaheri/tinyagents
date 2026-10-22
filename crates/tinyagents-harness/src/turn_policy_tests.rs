use super::*;

fn options(local: bool) -> TurnPolicyOptions {
    TurnPolicyOptions {
        max_model_calls: 10,
        max_tool_calls: 80,
        max_depth: 3,
        local_provider: local,
        response_cache_enabled: false,
        overrides: TurnPolicyOverrides::default(),
    }
}

#[test]
fn limits_reach_the_execution_policy() {
    let mut opts = options(false);
    opts.max_tool_calls = 2;
    let policy = run_policy(&opts);
    assert_eq!(policy.limits.max_tool_calls, 2);
    assert_eq!(policy.limits.max_model_calls, 10);
    assert_eq!(policy.limits.max_depth, 3);
}

/// The model reads how long each tool call took, so it can budget the rest of
/// the turn against it.
#[test]
fn tool_results_carry_their_duration() {
    assert!(run_policy(&options(false)).tool_result_durations);
}

#[test]
fn stream_timeouts_are_set_explicitly_with_first_event_off() {
    let mut limits = RunPolicy::default().limits;
    apply_stream_limits(&mut limits, None, None, None);
    assert_eq!(limits.stream_idle_timeout_ms, Some(120_000));
    assert_eq!(limits.stream_first_event_timeout_ms, None);
    assert_eq!(limits.max_consecutive_stream_idle_timeouts, Some(5));
}

#[test]
fn stream_timeout_overrides_parse() {
    assert_eq!(stream_idle_timeout_ms(Some("30")), Some(30_000));
    assert_eq!(stream_idle_timeout_ms(Some("0")), None);
    assert_eq!(stream_idle_timeout_ms(Some("junk")), Some(120_000));
    assert_eq!(stream_first_event_timeout_ms(Some("45")), Some(45_000));
    assert_eq!(stream_first_event_timeout_ms(Some("0")), None);
    assert_eq!(stream_first_event_timeout_ms(None), None);
    assert_eq!(max_consecutive_stream_idle_timeouts(Some("0")), None);
    assert_eq!(max_consecutive_stream_idle_timeouts(Some("2")), Some(2));
}

#[test]
fn turn_and_call_ceilings_parse_with_zero_disabling() {
    assert_eq!(turn_wall_clock_ms(None, false), Some(3_600_000));
    assert_eq!(turn_wall_clock_ms(Some("0"), false), None);
    assert_eq!(turn_wall_clock_ms(Some(" 90 "), false), Some(90_000));
    assert_eq!(turn_wall_clock_ms(Some("junk"), true), Some(14_400_000));
    assert_eq!(model_call_wall_clock_ms(None, false), Some(900_000));
    assert_eq!(model_call_wall_clock_ms(Some("0"), true), None);
}

#[test]
fn local_providers_get_longer_ceilings_than_hosted_ones() {
    let hosted = run_policy(&options(false));
    let local = run_policy(&options(true));
    assert_eq!(hosted.limits.max_model_call_ms, Some(900_000));
    assert_eq!(local.limits.max_model_call_ms, Some(3_600_000));
    assert_eq!(hosted.limits.max_wall_clock_ms, Some(3_600_000));
    assert_eq!(local.limits.max_wall_clock_ms, Some(14_400_000));
    assert!(local.limits.max_model_call_ms < local.limits.max_wall_clock_ms);
    // Stream-silence hang detection is identical for both.
    assert_eq!(
        hosted.limits.stream_idle_timeout_ms,
        local.limits.stream_idle_timeout_ms
    );
}

#[test]
fn overrides_win_over_the_defaults() {
    let mut opts = options(false);
    opts.overrides.turn_timeout_secs = Some("120".into());
    opts.overrides.model_call_timeout_secs = Some("0".into());
    let policy = run_policy(&opts);
    assert_eq!(policy.limits.max_wall_clock_ms, Some(120_000));
    assert_eq!(policy.limits.max_model_call_ms, None);
}

#[test]
fn local_backstop_sits_above_the_local_turn_ceiling() {
    assert_eq!(
        local_turn_backstop_secs(None),
        Some(LOCAL_AGENT_TURN_TIMEOUT_SECS + LOCAL_WEB_TURN_BACKSTOP_GRACE_SECS)
    );
    assert_eq!(local_turn_backstop_secs(Some("0")), None);
}

/// `max_attempts` is clamped to `max_retries_per_call + 1`, so raising only
/// the attempts would silently change nothing.
#[test]
fn retry_schedule_is_not_clamped_away() {
    let policy = run_policy(&options(false));
    assert_eq!(policy.retry.max_attempts, 5);
    assert!(policy.limits.max_retries_per_call as u32 + 1 >= policy.retry.max_attempts as u32);
    assert!(policy.retry.jitter);
}

#[test]
fn unknown_tools_and_bad_args_come_back_as_tool_errors() {
    let policy = run_policy(&options(false));
    assert!(matches!(
        policy.unknown_tool,
        UnknownToolPolicy::ReturnToolError
    ));
    assert!(matches!(
        policy.invalid_args,
        InvalidArgsPolicy::NormalizeThenReturnToolError
    ));
    assert_eq!(policy.empty_response_retries, 1);
    assert!(policy.cache.protect_prompt_prefix);
    assert!(!policy.cache.response_cache_enabled);
}

#[test]
fn a_zero_iteration_cap_means_unset() {
    assert_eq!(effective_max_iterations(0), DEFAULT_MAX_ITERATIONS);
    assert_eq!(effective_max_iterations(7), 7);
}

#[test]
fn interactive_output_cap_is_available_without_changing_run_policy_defaults() {
    assert_eq!(AGENT_TURN_MAX_OUTPUT_TOKENS, 16_384);
    let policy = RunPolicy::default();
    assert!(policy.default_reasoning.is_none());
    assert_eq!(policy.limits, crate::limits::RunLimits::default());
}
