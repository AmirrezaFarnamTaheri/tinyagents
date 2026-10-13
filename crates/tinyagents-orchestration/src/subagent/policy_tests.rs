use std::sync::Arc;
use std::time::Duration;

use tinyagents_harness::retry::RetryPolicy;

use super::*;

fn transient() -> TinyAgentsError {
    TinyAgentsError::Tool("flaky".into())
}

fn retrying() -> SubAgentPolicy {
    SubAgentPolicy::default().with_retry(
        RetryPolicy::default()
            .with_max_attempts(3)
            .with_backoff_sleep(false),
    )
}

#[test]
fn default_policy_never_retries() {
    assert!(!may_retry(
        &SubAgentPolicy::default(),
        0,
        &transient(),
        false
    ));
}

#[test]
fn retries_a_retryable_failure_until_attempts_run_out() {
    let policy = retrying();
    assert!(may_retry(&policy, 0, &transient(), false));
    assert!(may_retry(&policy, 1, &transient(), false));
    assert!(!may_retry(&policy, 2, &transient(), false));
}

#[test]
fn never_retries_a_permanent_failure() {
    let permanent = TinyAgentsError::Validation("bad".into());
    assert!(!may_retry(&retrying(), 0, &permanent, false));
}

#[test]
fn terminal_budget_refusals_never_retry_even_with_a_custom_predicate() {
    let policy = retrying().with_retry(
        RetryPolicy::default()
            .with_max_attempts(3)
            .with_retry_on(std::sync::Arc::new(|_| true)),
    );
    let refusal = TinyAgentsError::LimitExceeded("budget exceeded: max tokens".into());

    assert!(!may_retry(&policy, 0, &refusal, false));
}

#[test]
fn stream_idle_breaker_errors_remain_retryable_for_child_attempts() {
    let policy = retrying().with_retry(RetryPolicy::default().with_max_attempts(3).with_retry_on(
        std::sync::Arc::new(|error| matches!(error, TinyAgentsError::StreamIdleTimeout(_))),
    ));
    let breaker = TinyAgentsError::StreamIdleTimeout("provider stalled".into());

    assert!(may_retry(&policy, 0, &breaker, false));
}

#[test]
fn does_not_retry_after_tools_ran_unless_the_policy_allows_it() {
    assert!(!may_retry(&retrying(), 0, &transient(), true));
    let allowed = retrying().with_retry_after_tool_calls(true);
    assert!(may_retry(&allowed, 0, &transient(), true));
}

#[test]
fn the_orchestration_path_names_the_same_policy_type_as_the_graph() {
    let policy: tinyagents_graph::SubAgentPolicy =
        SubAgentPolicy::default().with_timeout(Duration::from_secs(1));
    assert_eq!(policy.timeout, Some(Duration::from_secs(1)));
}

#[derive(Debug, PartialEq)]
struct OriginTag(&'static str);

fn prepared_with_origin(factory_sets_host: bool) -> PreparedSubagent<()> {
    use tinyagents_harness::context::RunConfig;
    let mut policy = SubAgentPolicy::default();
    policy.retry.max_attempts = 2;
    PreparedSubagent::new(
        "t".to_string(),
        "agent".to_string(),
        vec![],
        ToolSnapshot::new(vec![]).unwrap(),
        RunContext::new(RunConfig::new("first"), ()).with_host(OriginTag("web")),
    )
    .with_policy(policy)
    .with_retry_context(Arc::new(move |attempt| {
        let ctx = RunContext::new(RunConfig::new(format!("retry-{attempt}")), ());
        Ok(if factory_sets_host {
            ctx.with_host(OriginTag("cron"))
        } else {
            ctx
        })
    }))
}

#[test]
fn retry_attempts_inherit_the_first_attempts_host_extensions() {
    let mut source = AttemptSource::from_prepared(&prepared_with_origin(false));
    let retry = source.next(1, CancellationToken::new()).unwrap();
    assert_eq!(
        retry.run_context.host::<OriginTag>(),
        Some(&OriginTag("web"))
    );
}

#[test]
fn retry_factory_host_extensions_win_over_the_inherited_ones() {
    let mut source = AttemptSource::from_prepared(&prepared_with_origin(true));
    let retry = source.next(1, CancellationToken::new()).unwrap();
    assert_eq!(
        retry.run_context.host::<OriginTag>(),
        Some(&OriginTag("cron"))
    );
}
