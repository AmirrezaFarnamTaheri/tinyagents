# OpenHuman adoption map for TinyAgents #484

This document maps the existing OpenHuman triage copies to the public
`tinyagents-harness` APIs added for this issue. It is intended to let the host
follow-up remove its copied parser and retry-state machinery without moving
OpenHuman policy into TinyAgents.

## Source-to-API mapping

| OpenHuman source copy | TinyAgents replacement | Adoption notes |
| --- | --- | --- |
| `crates/openhuman-core/src/triage/decision.rs::extract_json_slice`, `last_fenced_json_block`, and `last_balanced_brace_object` | `tinyagents_harness::structured::parse_lenient_last_object(&str) -> Option<(serde_json::Value, JsonRepair)>` | Normalize the returned `Value["action"]` string to lowercase in OpenHuman before deserializing it into `TriageDecision`; keep OpenHuman's required-target validation. This is a small Value-based replacement for the current string-rewrite helper, while action normalization remains host policy. The parser selects a complete outer object and leaves default `parse_lenient` behavior unchanged. |
| `decision.rs`'s `strip_trailing_commas` call | Included in `parse_lenient_last_object` | Commas before `}` or `]` are removed outside quoted strings before the normal repair ladder runs. |
| `evaluator/chain.rs::{OutageState, RetryState, OutageAttempt, retry_state, begin_outage_attempt, clear_outage, record_outage}` | `tinyagents_harness::retry::PersistentFailoverBreaker<K>`, `PersistentFailoverAttempt<K>`, `BreakerRejection`, and `PersistentFailoverFailure` | Keep one cloneable breaker in the host's process-level `OnceLock`. Use `try_acquire(key, now_ms)` before the cloud attempt, `record_success()` after success/terminal host outcomes, and `record_failure(now_ms)` after the existing cloud-outage path. Translate admission/backoff/failure-limit state into `TriageOutcome`; the TinyAgents API does not render product outcomes. |
| `OUTAGE_BACKOFF_BASE_MS`, `OUTAGE_BACKOFF_CAP_MS`, `OUTAGE_FAILURE_LIMIT` | `PersistentFailoverBreakerConfig` | Defaults match the current host constants: 30 seconds, 15 minutes, and 8 consecutive failures. Keep the values configurable at the host boundary if that is desired. The clock is caller-supplied monotonic milliseconds; no test sleeps or wall clock are required. |
| `format!("{}:{}", cloud.provider_name, cloud.model)` retry key | Caller-supplied `K` to `PersistentFailoverBreaker<K>` | Preserve the complete retry identity. Prefer a tuple/newtype key such as `(provider_name, model)` over delimiter concatenation. Add tenant or credential-scope identity if the host's ownership boundary requires tenant isolation. The breaker does not construct or normalize keys. |
| `evaluator/arm.rs::is_transient_string` | `tinyagents_harness::retry::FailoverReason::classify` | Exact legacy phrases are covered below. Existing OpenHuman dispatch-error classification still has its own wrapper and should map the resulting typed reason to `ArmError` rather than copy these phrase checks. |

## Transient string coverage

The legacy host string matcher lowercased text and matched these substrings:

| Existing host input | TinyAgents reason |
| --- | --- |
| `timed out`, `timeout` | `Timeout` |
| whole-word `connection` | `Transport` |
| `connect error` | `Transport` |
| `broken pipe` | `Transport` |
| `reset by peer` | `Transport` |
| `deadline exceeded` | `Transport` |
| `temporarily unavailable` | `Transport` |
| standalone numeric token from 500 through 599 | `Transport`, except existing special cases such as 503/529 (`Overloaded`) and recognized timeout statuses (`Timeout`) |

The new phrase checks run after context, billing, auth, model-not-found,
rate-limit, timeout, and overload classification. An existing 4xx status stays
`Format` unless a stronger established classification applies. This keeps
provider-specific and permanent/fatal reasons ahead of broad transport hints;
for example, `reconnection` does not match the whole-word `connection` check.

OpenHuman retains its prompt-guard rejection strings, strict budget-message
matching, `LlmPermit` acquisition/release, `ArmError` mapping, provider/local
fallback choice, `Retry-After` handling, event rendering, and all
`TriageOutcome` wording. None of those product policies belongs in the
generic TinyAgents helper.
