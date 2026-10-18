//! Public activity, trigger, and middleware state types.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Fewest model calls that must remain after the answer for the check to run.
/// The check takes one; leaving at least two more keeps it clear of a host's
/// penultimate/final-call wrap-up and leaves room to act on what it finds.
pub const MIN_REMAINING_MODEL_CALLS: usize = 3;

/// Default for
/// [`with_min_remaining_wall_clock`](VerifyBeforeFinishMiddleware::with_min_remaining_wall_clock).
pub const DEFAULT_MIN_REMAINING_WALL_CLOCK: Duration = Duration::from_secs(120);

/// What a run did before its answer, as seen by a [`FinishCheckTrigger`].
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinishActivity {
    /// Model responses in this run that requested at least one tool call.
    pub tool_rounds: usize,
    /// Names of every tool those rounds requested.
    pub tools_called: BTreeSet<String>,
    /// Concrete tool names whose executors actually started in this run.
    ///
    /// Packed calls contribute the pinned inner name; refused and
    /// wrapper-short-circuited calls contribute nothing. An execution attempt
    /// can fail, time out, or be cancelled after entry, so membership does not
    /// establish success. Legacy resume state defaults to an empty set rather
    /// than treating transcript requests as execution evidence.
    #[serde(default)]
    pub tools_executed: BTreeSet<String>,
    /// Started targets that declared a permission above `ReadOnly` or an
    /// external effect for their actual execution arguments.
    ///
    /// Literal read-only previews remain in [`Self::tools_executed`] without
    /// appearing here. This is conservative evidence that a mutating attempt
    /// started, not proof that the attempted mutation succeeded.
    #[serde(default)]
    pub tools_with_side_effects: BTreeSet<String>,
}

impl FinishActivity {
    /// Whether the run requested `tool` at least once.
    pub fn called(&self, tool: &str) -> bool {
        self.tools_called.contains(tool)
    }

    /// Whether the concrete `tool` executor started at least once.
    pub fn executed(&self, tool: &str) -> bool {
        self.tools_executed.contains(tool)
    }

    /// Whether `tool` started with arguments declaring potential side effects.
    pub fn executed_with_side_effects(&self, tool: &str) -> bool {
        self.tools_with_side_effects.contains(tool)
    }

    /// Trigger predicate: at least `min_rounds` tool rounds, or `tool` was
    /// requested at least once. Threshold and tool name are host config.
    pub fn rounds_or_called(
        min_rounds: usize,
        tool: impl Into<String>,
    ) -> impl Fn(&FinishActivity) -> bool + Send + Sync + 'static {
        let tool = tool.into();
        move |activity| activity.tool_rounds >= min_rounds || activity.called(&tool)
    }
}

/// Decides whether a run's activity warrants the check.
pub type FinishCheckTrigger = Arc<dyn Fn(&FinishActivity) -> bool + Send + Sync>;

/// Per-run bookkeeping, keyed by [`crate::context::RunContext::instance_id`].
#[derive(Default)]
pub(super) struct RunState {
    pub(super) activity: FinishActivity,
    pub(super) fired: bool,
    pub(super) restore_deferred: bool,
    pub(super) lifecycle: std::sync::Weak<()>,
}

/// Asks a multi-step run, once, to check its final answer against the request
/// before that answer ends the run. See the module docs.
///
/// Generic over the application state and run-context payload: nothing here
/// reads either. The host supplies the check text and decides which runs get
/// it ([`with_min_tool_rounds`](Self::with_min_tool_rounds) or
/// [`with_trigger`](Self::with_trigger)).
pub struct VerifyBeforeFinishMiddleware {
    pub(super) check: String,
    pub(super) trigger: FinishCheckTrigger,
    pub(super) min_remaining_wall_clock: Duration,
    pub(super) wall_clock_limit: Option<Duration>,
    pub(super) runs: Mutex<HashMap<u64, RunState>>,
    pub(super) wrap_up: Option<Arc<crate::middleware::library::FinalCallWrapUpMiddleware>>,
}
