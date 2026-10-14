//! Per-turn controls and the report a [`crate::turn_driver::TurnDriver`] fills.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};

use tinyagents_harness::steering::{SteeringCommandKind, SteeringPolicy};

use super::steering::{SteeringFeed, SteeringRegistrar};
use super::tool_set::ToolSetReport;

/// Identity of a delegated child run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubagentScope {
    /// The definition/agent id the child runs as.
    pub agent_id: String,
    /// The detached task id; keys steering registration and thread labels.
    pub task_id: String,
}

/// Whether a turn is a root conversation turn or a delegated child run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TurnScope {
    /// A top-level, interactive turn.
    #[default]
    Root,
    /// A delegated child run.
    Subagent(SubagentScope),
}

impl TurnScope {
    /// Whether this is a delegated child run.
    pub fn is_subagent(&self) -> bool {
        matches!(self, Self::Subagent(_))
    }

    /// The scope's tag on the run configuration (`scope:root`/`scope:subagent`).
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Root => "scope:root",
            Self::Subagent(_) => "scope:subagent",
        }
    }

    /// The steering commands a run of this class accepts. A root turn takes
    /// injected messages and pauses; a detached child additionally takes
    /// resume, cancel and redirect from its orchestrator.
    pub fn steering_policy(&self) -> SteeringPolicy {
        let policy = SteeringPolicy::new()
            .allow(SteeringCommandKind::InjectMessage)
            .allow(SteeringCommandKind::Pause);
        match self {
            Self::Root => policy,
            Self::Subagent(_) => policy
                .allow(SteeringCommandKind::Resume)
                .allow(SteeringCommandKind::Cancel)
                .allow(SteeringCommandKind::Redirect),
        }
    }
}

/// A tool call that ended the turn early (for example a clarification
/// question that must reach the user before the loop can continue).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EarlyExit {
    /// The early-exit tool that fired.
    pub tool: String,
    /// The tool's model-facing output, surfaced as the turn's text.
    pub question: String,
}

/// Receives the terminal "turn completed" signal.
pub trait TurnCompletionSink: Send + Sync {
    /// Called at most once per turn, with the finished report.
    fn turn_completed(&self, report: &TurnReport);
}

/// What the driver learned about how a turn ended.
#[derive(Clone, Default)]
pub struct TurnReport {
    /// The scope the turn ran under.
    pub scope: TurnScope,
    /// The first early-exit tool call, when one fired.
    pub early_exit: Option<EarlyExit>,
    /// The run stopped at its model-call cap with work pending and
    /// [`TurnControls::pause_at_cap`] was set.
    pub hit_cap: bool,
    /// Model calls the run dispatched.
    pub model_calls: usize,
    /// The model-call cap that applied.
    pub max_iterations: usize,
    /// How tool registration resolved (candidates, registered, refused).
    pub tools: ToolSetReport,
    /// The completion signal was deferred to the caller. Call
    /// [`Self::emit_completed`] once its post-run work is done.
    pub completion_deferred: bool,
    completion: Option<Arc<dyn TurnCompletionSink>>,
    completed: Arc<AtomicBool>,
}

impl TurnReport {
    pub(crate) fn with_completion(mut self, sink: Option<Arc<dyn TurnCompletionSink>>) -> Self {
        self.completion = sink;
        self
    }

    /// Whether the completion signal has fired.
    pub fn completed_emitted(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Fires the completion sink exactly once across every clone of this
    /// report. Returns whether this call was the one that fired it.
    pub fn emit_completed(&self) -> bool {
        let Some(sink) = &self.completion else {
            return false;
        };
        if self.completed.swap(true, Ordering::SeqCst) {
            return false;
        }
        sink.turn_completed(self);
        true
    }
}

impl fmt::Debug for TurnReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnReport")
            .field("scope", &self.scope)
            .field("early_exit", &self.early_exit)
            .field("hit_cap", &self.hit_cap)
            .field("model_calls", &self.model_calls)
            .field("max_iterations", &self.max_iterations)
            .field("tools", &self.tools)
            .field("completion_deferred", &self.completion_deferred)
            .field("completed_emitted", &self.completed_emitted())
            .finish()
    }
}

/// Shared slot the driver writes the [`TurnReport`] into. Clone it into
/// [`TurnControls`] before the turn and read it after `Session::turn` returns
/// (also after a failure).
#[derive(Clone, Default)]
pub struct TurnReportSlot(Arc<Mutex<Option<TurnReport>>>);

impl TurnReportSlot {
    pub(crate) fn set(&self, report: TurnReport) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(report);
    }

    /// The report of the last turn that used this slot, if the driver ran.
    pub fn get(&self) -> Option<TurnReport> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Host-neutral controls for one turn driven by a
/// [`crate::turn_driver::TurnDriver`]. They ride on
/// [`crate::TurnOptions::controls`] and reach the driver in
/// [`crate::DriverRequest::controls`]; a driver that is not a `TurnDriver`
/// ignores them.
#[derive(Clone, Default)]
pub struct TurnControls {
    /// Model-call cap (`None` or `Some(0)` keeps the policy default).
    pub max_iterations: Option<usize>,
    /// Stop gracefully at the cap, keeping the partial transcript, instead of
    /// failing the run with a limit error.
    pub pause_at_cap: bool,
    /// Tools whose successful call pauses the loop and surfaces its output.
    pub early_exit_tools: BTreeSet<String>,
    /// Do not fire the completion sink at the end of the turn; the caller
    /// fires it after its own post-run work via [`TurnReport::emit_completed`].
    pub defer_turn_completed: bool,
    /// Root turn or delegated child run.
    pub scope: TurnScope,
    /// Feeds mid-flight steering messages (a run queue) into the run.
    pub steering: Option<Arc<dyn SteeringFeed>>,
    /// Registers a child run's steering handle for its orchestrator.
    pub registrar: Option<Arc<dyn SteeringRegistrar>>,
    /// Label stamped on delivery/requeue observations. Defaults to the
    /// child's task id, then to the options' thread id.
    pub thread_label: Option<String>,
    /// Where the driver publishes the [`TurnReport`].
    pub report: TurnReportSlot,
}

impl fmt::Debug for TurnControls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TurnControls")
            .field("max_iterations", &self.max_iterations)
            .field("pause_at_cap", &self.pause_at_cap)
            .field("early_exit_tools", &self.early_exit_tools)
            .field("defer_turn_completed", &self.defer_turn_completed)
            .field("scope", &self.scope)
            .field("steering", &self.steering.is_some())
            .field("registrar", &self.registrar.is_some())
            .field("thread_label", &self.thread_label)
            .finish()
    }
}
