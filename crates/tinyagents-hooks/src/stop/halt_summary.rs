//! Typed stop summaries shared by stop hooks and their host copy provider.

use tinyagents_harness::turn_stop::{CopyProvider, CopyRequest, TurnFailureClass, render_copy};

/// Machine stop class and its separately rendered user-facing summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaltSummary {
    /// Stable classifier identifier used by traces and host policy.
    pub class: TurnFailureClass,
    /// Display copy; never used to recover or alter `class`.
    pub summary: String,
}

/// Permanently non-retryable delegated inference failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalInferenceFailureKind {
    /// The account or provider billing budget is exhausted.
    BudgetExhausted,
    /// The configured model or provider rejected the request.
    ProviderConfig,
}

/// Recognize a terminal delegated-inference failure from its trusted envelope.
///
/// A provider error alone is not enough: the result must contain a harness
/// generated delegation/reliable-chain marker before TinyInference's typed
/// billing and provider-configuration classifiers are consulted. Budget wins
/// when both classifiers match.
pub fn terminal_inference_failure_kind(result: &str) -> Option<TerminalInferenceFailureKind> {
    let lower = result.to_ascii_lowercase();
    let has_envelope = [
        "all providers/models failed",
        "may not be available on your provider",
        "failed and did not complete",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    if !has_envelope {
        return None;
    }
    if tinyinference_providers::is_budget_message(
        result,
        tinyinference_providers::BudgetMatch::Billing,
    ) {
        Some(TerminalInferenceFailureKind::BudgetExhausted)
    } else if tinyinference_providers::is_provider_config_rejection_message(result) {
        Some(TerminalInferenceFailureKind::ProviderConfig)
    } else {
        None
    }
}

/// Build the summary for an exhausted identical-retry allowance.
pub fn retry_exhausted(
    provider: &dyn CopyProvider,
    tool: &str,
    count: u32,
    last_error: &str,
) -> HaltSummary {
    HaltSummary {
        class: TurnFailureClass::new("transient"),
        summary: render_copy(
            provider,
            &CopyRequest::RetryExhausted {
                tool,
                count,
                last_error,
            },
        ),
    }
}

/// Build the summary for a consecutive no-progress failure streak.
pub fn no_progress(
    provider: &dyn CopyProvider,
    consecutive: u32,
    tool: &str,
    last_error: &str,
) -> HaltSummary {
    HaltSummary {
        class: TurnFailureClass::new("transient"),
        summary: render_copy(
            provider,
            &CopyRequest::NoProgress {
                tool,
                consecutive,
                last_error,
            },
        ),
    }
}

/// Build the summary for a delegated inference billing failure.
pub fn budget(provider: &dyn CopyProvider, tool: &str, last_error: &str) -> HaltSummary {
    HaltSummary {
        class: TurnFailureClass::new("out_of_credits"),
        summary: render_copy(provider, &CopyRequest::Budget { tool, last_error }),
    }
}

/// Build the summary for a terminal model/provider configuration rejection.
pub fn provider_config(provider: &dyn CopyProvider, tool: &str, last_error: &str) -> HaltSummary {
    HaltSummary {
        class: TurnFailureClass::new("provider_config"),
        summary: render_copy(provider, &CopyRequest::ProviderConfig { tool, last_error }),
    }
}

#[cfg(test)]
#[path = "halt_summary_tests.rs"]
mod tests;
