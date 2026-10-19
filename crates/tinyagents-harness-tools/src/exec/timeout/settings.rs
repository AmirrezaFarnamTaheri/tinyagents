//! Wall-clock bounds for tool execution: pure resolution, no host policy.
//!
//! The host decides where the number comes from (config, an operator env var)
//! and pushes it in through [`configure`]; this module only bounds, resolves
//! and stores it. The inherited value lives in an atomic so a later push
//! reaches every reader on its next call.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tinytools::ToolTimeout;

/// Default tool-execution timeout in seconds when nothing else is configured.
pub const DEFAULT_TIMEOUT_SECS: u64 = 120;
/// Smallest accepted timeout. `0` would disable the timeout entirely, so it is
/// rejected and falls back to the default.
pub const MIN_TIMEOUT_SECS: u64 = 1;
/// Largest accepted timeout (1 hour).
pub const MAX_TIMEOUT_SECS: u64 = 3600;
/// Effective-unbounded cap (24h) for sandbox backends, which require a finite
/// deadline.
pub const SANDBOX_UNBOUNDED_CAP_SECS: u64 = 86_400;
/// Slack added on top of an explicit per-call budget before the hard deadline
/// fires, so a tool finishing right at its budget is not killed by jitter.
pub const TOOL_TIMEOUT_GRACE_SECS: u64 = 5;

/// A resolved per-call deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedTimeout {
    /// Hard deadline, or `None` to run unbounded.
    pub deadline: Option<Duration>,
    /// The user-facing budget in milliseconds (`0` when unbounded).
    pub budget_ms: u64,
}

/// Bounds plus the inherited timeout, with the inherited value atomic.
#[derive(Debug)]
pub struct TimeoutSettings {
    inherited_ms: AtomicU64,
    min_ms: u64,
    max_ms: u64,
    grace_ms: u64,
}

impl TimeoutSettings {
    /// Settings with explicit bounds, all in milliseconds.
    #[must_use]
    pub fn new(inherited_ms: u64, min_ms: u64, max_ms: u64, grace_ms: u64) -> Self {
        Self {
            inherited_ms: AtomicU64::new(inherited_ms),
            min_ms,
            max_ms,
            grace_ms,
        }
    }

    /// Settings with the crate's default bounds, inheriting `inherited_secs`.
    #[must_use]
    pub fn with_inherited_secs(inherited_secs: u64) -> Self {
        Self::new(
            inherited_secs.saturating_mul(1000),
            MIN_TIMEOUT_SECS * 1000,
            MAX_TIMEOUT_SECS * 1000,
            TOOL_TIMEOUT_GRACE_SECS * 1000,
        )
    }

    /// The timeout `Inherit` tools get.
    #[must_use]
    pub fn inherited_timeout(&self) -> Duration {
        Duration::from_millis(self.inherited_ms.load(Ordering::Relaxed))
    }

    /// Replace the inherited timeout; readers see it on their next call.
    pub fn set_inherited_timeout_ms(&self, ms: u64) {
        self.inherited_ms.store(ms, Ordering::Relaxed);
    }

    /// Resolve a tool's policy. An explicit millisecond request is rounded up
    /// to whole seconds, clamped to the bounds, and padded with the grace.
    #[must_use]
    pub fn resolve(&self, policy: ToolTimeout) -> ResolvedTimeout {
        match policy {
            ToolTimeout::Inherit => {
                let ms = self.inherited_ms.load(Ordering::Relaxed);
                ResolvedTimeout {
                    deadline: Some(Duration::from_millis(ms)),
                    budget_ms: ms,
                }
            }
            ToolTimeout::Unbounded => ResolvedTimeout {
                deadline: None,
                budget_ms: 0,
            },
            ToolTimeout::Millis(req) => {
                let rounded = req.saturating_add(999) / 1000 * 1000;
                let budget = rounded.clamp(self.min_ms, self.max_ms);
                ResolvedTimeout {
                    deadline: Some(Duration::from_millis(budget.saturating_add(self.grace_ms))),
                    budget_ms: budget,
                }
            }
        }
    }
}

static SETTINGS: OnceLock<TimeoutSettings> = OnceLock::new();

/// Install the process-global settings. The first call installs `settings`;
/// later calls only push its inherited timeout into the installed one, so
/// handles obtained earlier stay live. Returns the global.
pub fn configure(settings: TimeoutSettings) -> &'static TimeoutSettings {
    let ms = settings.inherited_ms.load(Ordering::Relaxed);
    let global = SETTINGS.get_or_init(|| settings);
    global.set_inherited_timeout_ms(ms);
    log::debug!("[tool_timeout] configured inherited timeout {ms}ms");
    global
}

/// The process-global settings, seeded with [`DEFAULT_TIMEOUT_SECS`] when the
/// host never called [`configure`].
#[must_use]
pub fn settings() -> &'static TimeoutSettings {
    SETTINGS.get_or_init(|| TimeoutSettings::with_inherited_secs(DEFAULT_TIMEOUT_SECS))
}

/// Parse a raw value into a bounded timeout; anything missing, non-numeric or
/// outside `MIN_TIMEOUT_SECS..=MAX_TIMEOUT_SECS` yields [`DEFAULT_TIMEOUT_SECS`].
#[must_use]
pub fn parse_tool_timeout_secs(raw: Option<&str>) -> u64 {
    override_from(raw).unwrap_or(DEFAULT_TIMEOUT_SECS)
}

/// A raw operator override, if it is a valid in-range number of seconds.
#[must_use]
pub fn override_from(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| (MIN_TIMEOUT_SECS..=MAX_TIMEOUT_SECS).contains(&n))
}

/// Env override wins when valid; otherwise the (bounded) config value applies.
#[must_use]
pub fn resolve_effective(config_secs: u64, env_raw: Option<&str>) -> u64 {
    override_from(env_raw)
        .unwrap_or_else(|| parse_tool_timeout_secs(Some(&config_secs.to_string())))
}

/// Resolve an explicit per-call request for an otherwise unbounded tool.
/// `None`/`Some(0)` run unbounded; otherwise clamp to `MIN..=cap`.
#[must_use]
pub fn explicit_call_timeout_secs(requested: Option<u64>, cap: u64) -> Option<u64> {
    let cap = cap.clamp(MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS);
    match requested {
        None | Some(0) => None,
        Some(n) => Some(n.clamp(MIN_TIMEOUT_SECS, cap)),
    }
}

/// [`explicit_call_timeout_secs`] as a [`Duration`].
#[must_use]
pub fn explicit_call_timeout_duration(requested: Option<u64>, cap: u64) -> Option<Duration> {
    explicit_call_timeout_secs(requested, cap).map(Duration::from_secs)
}
