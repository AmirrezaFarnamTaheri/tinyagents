use super::*;

#[test]
fn selector_recovery_preserves_registry_failures_and_retries_selector_denials() {
    let registry_error = SubAgentJobError::Registry("poisoned registry".into());
    let fatal = selector_error(registry_error.clone(), true);
    assert_eq!(
        fatal.downcast_ref::<SubAgentJobError>(),
        Some(&registry_error)
    );

    let denial = SubAgentJobError::NotFound("missing".into());
    let retry = selector_error(denial, true);
    assert!(matches!(
        retry.downcast_ref::<TinyAgentsError>(),
        Some(TinyAgentsError::ModelRetry(message)) if message == SELECTOR_RETRY_HINT
    ));
}

#[test]
fn selector_recovery_disabled_keeps_denial_typed() {
    let denial = SubAgentJobError::NotFound("missing".into());
    let error = selector_error(denial.clone(), false);
    assert_eq!(error.downcast_ref::<SubAgentJobError>(), Some(&denial));
}
