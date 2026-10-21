use super::*;

use crate::stop::halt_summary;
use tinyagents_harness::turn_stop::{CopyProvider, CopyRequest, truncate_for_halt};

struct Localized;

impl CopyProvider for Localized {
    fn copy_override(&self, request: &CopyRequest<'_>) -> Option<String> {
        match request {
            CopyRequest::RetryExhausted { .. } => Some("localized retry summary".into()),
            _ => None,
        }
    }
}

#[test]
fn halt_builders_keep_machine_class_independent_from_partial_copy_overrides() {
    let retry = halt_summary::retry_exhausted(&Localized, "file_read", 3, "secret detail");
    assert_eq!(retry.class.as_str(), "transient");
    assert_eq!(retry.summary, "localized retry summary");

    let budget = halt_summary::budget(&Localized, "web_search", "insufficient credits");
    assert_eq!(budget.class.as_str(), "out_of_credits");
    assert!(budget.summary.contains("web_search"));
    assert!(budget.summary.contains("insufficient credits"));
    assert!(budget.summary.contains("every retry hits the same wall"));
    assert!(budget.summary.contains("custom/BYO provider"));

    let config = halt_summary::provider_config(&Localized, "model_call", "unknown model");
    assert_eq!(config.class.as_str(), "provider_config");
    assert!(config.summary.contains("unknown model"));
    assert!(config.summary.contains("non-chat/embedding model"));
    assert!(config.summary.contains("retrying will not help"));
    assert!(!config.summary.contains("Connections"));
}

#[test]
fn neutral_summary_defaults_match_host_wording_without_host_branding() {
    let provider = tinyagents_harness::turn_stop::NeutralCopyProvider;
    assert_eq!(
        halt_summary::retry_exhausted(&provider, "web_fetch", 5, "503 upstream").summary,
        "Stopping: the `web_fetch` call was retried 5 times with identical arguments and kept \
         failing — repeating it will not help. Last error:\n503 upstream\n\nThis looked recoverable \
         at first, but the same call exhausted the extended transient-failure headroom. Report this \
         back instead of retrying."
    );
    assert_eq!(
        halt_summary::no_progress(&provider, 4, "shell", "boom").summary,
        "Stopping: 4 recoverable-looking tool failures happened in a row with no successful \
         progress. Last error (from `shell`):\nboom\n\nThe turn is still bounded by the \
         iteration/cost limits, but this many consecutive transient failures means the goal is \
         not currently reachable. Report this back instead of retrying."
    );
    assert_eq!(
        halt_summary::budget(&provider, "spawn", "x").summary,
        "Stopping: the `spawn` step failed because the account is out of inference budget/credits \
         — every retry hits the same wall. Add credits to your account (or, when using a custom/BYO \
         provider, top up that provider's own account) and try again. Details:\nx"
    );
    assert_eq!(
        halt_summary::provider_config(&provider, "spawn", "x").summary,
        "Stopping: the `spawn` step failed because the configured model/provider rejected the \
         request (e.g. an unknown model, a non-chat/embedding model, a missing credential, or a \
         region block) — retrying will not help. Fix the model or API key, then try again. Details:\nx"
    );
}

#[test]
fn host_can_override_only_branded_settings_copy() {
    struct HostSettingsCopy;
    impl CopyProvider for HostSettingsCopy {
        fn copy_override(&self, request: &CopyRequest<'_>) -> Option<String> {
            match request {
                CopyRequest::ProviderConfig { .. } => {
                    Some("Fix the model or API key in Connections.".into())
                }
                CopyRequest::TurnStop {
                    key: tinyagents_harness::turn_stop::TurnStopCopyKey::MissingConnection,
                    ..
                } => Some("Connect this service in Connections.".into()),
                _ => None,
            }
        }
    }

    let provider = HostSettingsCopy;
    let config = halt_summary::provider_config(&provider, "spawn", "unknown model");
    assert_eq!(config.class.as_str(), "provider_config");
    assert_eq!(config.summary, "Fix the model or API key in Connections.");
    assert_eq!(
        tinyagents_harness::turn_stop::render_copy(
            &provider,
            &CopyRequest::TurnStop {
                key: tinyagents_harness::turn_stop::TurnStopCopyKey::MissingConnection,
                operation: None,
            }
        ),
        "Connect this service in Connections."
    );
    let neutral = halt_summary::provider_config(&Localized, "spawn", "unknown model");
    assert_eq!(neutral.class.as_str(), "provider_config");
    assert!(!neutral.summary.contains("Connections"));
}

#[test]
fn no_progress_copy_retains_count_and_tool_context() {
    let halt = halt_summary::no_progress(&Localized, 6, "file_read", "not found");
    assert_eq!(halt.class.as_str(), "transient");
    assert!(halt.summary.contains("6"));
    assert!(halt.summary.contains("file_read"));
    assert!(halt.summary.contains("not found"));
}

#[test]
fn halt_detail_truncation_counts_unicode_characters_and_preserves_the_suffix() {
    let short = "é".repeat(600);
    assert_eq!(truncate_for_halt(&short), short);

    let long = "🦀".repeat(601);
    let truncated = truncate_for_halt(&long);
    assert!(truncated.starts_with(&"🦀".repeat(600)));
    assert!(truncated.ends_with("\n… [truncated]"));
}

#[test]
fn terminal_inference_classifier_requires_a_trusted_envelope() {
    assert_eq!(
        terminal_inference_failure_kind("insufficient budget"),
        None,
        "arbitrary tool stderr is not delegated inference evidence"
    );
    assert_eq!(
        terminal_inference_failure_kind(
            r#"failed and did not complete: {"error":{"message":"model not found","code":"model_not_found"}}"#
        ),
        Some(TerminalInferenceFailureKind::ProviderConfig)
    );
    assert_eq!(
        terminal_inference_failure_kind("failed and did not complete: Insufficient budget"),
        Some(TerminalInferenceFailureKind::BudgetExhausted),
        "billing classification takes precedence when both typed classifiers match"
    );
    assert_eq!(
        terminal_inference_failure_kind(
            "failed and did not complete: Insufficient budget; model not found"
        ),
        Some(TerminalInferenceFailureKind::BudgetExhausted),
        "billing classification takes precedence when both typed classifiers match"
    );
    assert_eq!(
        terminal_inference_failure_kind(
            "failed and did not complete: 403 insufficient permissions"
        ),
        None,
        "permission and scope errors are not provider configuration failures"
    );
}

#[test]
fn builders_pass_bounded_unicode_detail_to_partial_copy_overrides() {
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<String>>>);
    impl CopyProvider for Capture {
        fn copy_override(&self, request: &CopyRequest<'_>) -> Option<String> {
            let last_error = match request {
                CopyRequest::RetryExhausted { last_error, .. }
                | CopyRequest::NoProgress { last_error, .. }
                | CopyRequest::Budget { last_error, .. }
                | CopyRequest::ProviderConfig { last_error, .. } => last_error,
                CopyRequest::TurnStop { .. } => return None,
                _ => return None,
            };
            self.0.lock().unwrap().push((*last_error).to_owned());
            Some("localized".into())
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider = Capture(seen.clone());
    let at_limit = "é".repeat(600);
    let over_limit = "🦀".repeat(601);
    let builders = [
        halt_summary::retry_exhausted(&provider, "shell", 4, &at_limit),
        halt_summary::no_progress(&provider, 6, "shell", &over_limit),
        halt_summary::budget(&provider, "shell", &over_limit),
        halt_summary::provider_config(&provider, "shell", &at_limit),
    ];
    assert!(builders.iter().all(|halt| halt.summary == "localized"));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            at_limit.clone(),
            format!("{}\n… [truncated]", "🦀".repeat(600)),
            format!("{}\n… [truncated]", "🦀".repeat(600)),
            at_limit,
        ]
    );
}
