//! [`TurnDriver`]: the generic turn loop a host plugs into a [`crate::Session`].
//!
//! A host supplies a [`TurnPlanner`] that, per turn, returns a [`TurnPlan`]
//! (models, tools, context middleware, tool policy). The driver assembles one
//! [`AgentHarness`] from it and applies the per-turn [`TurnControls`]: model-call
//! cap and pause-at-cap, early-exit tools, mid-flight steering, delegated-child
//! scope, and deferred completion. Everything product-shaped stays in the plan.
//!
//! # Middleware order
//!
//! `before_*` hooks run in registration order and `after_*` hooks in reverse:
//!
//! 1. [`TurnPlan::context`], in the order given (outermost first);
//! 2. tool wrap: the [`TurnPlan::tool_policy`] gate.
//!
//! Early exit is applied on the tool adapter itself, so it observes the raw tool
//! result before any middleware rewrites it.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use tinyagents_harness::agent_loop::PartialRunOutcome;
use tinyagents_harness::context::RunContext;
use tinyagents_harness::events::EventListener;
use tinyagents_harness::host::PrebuiltModelResolver;
use tinyagents_harness::middleware::AgentRun;
use tinyagents_harness::middleware::{
    ApprovalResolver, Middleware, ToolCallPolicy, ToolPolicyGateMiddleware,
};
use tinyagents_harness::runtime::{AgentHarness, RunPolicy};
use tinyagents_harness::steering::SteeringHandle;
use tinyagents_harness::tool::EarlyExitHook;
use tinyagents_harness::turn_run_finalize::{TurnEndInputs, detect_hit_cap_with_cause};
use tinyinference_llm::message::Message;
use tinyinference_llm::model::ChatModel;

use super::steering::{FeedGuard, cleanup_only_guard};
use super::tool_set::{ToolSetBuilder, ToolSetReport};
use super::types::{TurnCompletionSink, TurnControls, TurnReport, TurnScope};
use crate::driver::{outcome_from_partial, same_tools};
use crate::{DriverFailure, DriverOutcome, DriverRequest, RuntimeError, SessionDriver};

/// Everything a host decides for one turn.
pub struct TurnPlan<State, C> {
    /// Models to register; the first is the default.
    pub models: Vec<(String, Arc<dyn ChatModel<State>>)>,
    /// The run policy (retry, fallback, dialect, limits). The driver overrides
    /// only the model-call cap, from [`TurnControls::max_iterations`](super::TurnControls::max_iterations).
    pub policy: RunPolicy,
    /// The tools this turn can call.
    pub tools: ToolSetBuilder<State, C>,
    /// Context and policy middleware, outermost first.
    pub context: Vec<Arc<dyn Middleware<State, C>>>,
    /// Enforced at the tool boundary: deny and require-approval verdicts
    /// short-circuit the call with a model-visible error.
    pub tool_policy: Option<Arc<dyn ToolCallPolicy<C>>>,
    /// Settles require-approval verdicts; without it they fail closed.
    pub approval: Option<Arc<dyn ApprovalResolver<C>>>,
    /// Extra listeners subscribed to the run's event sink.
    pub listeners: Vec<Arc<dyn EventListener>>,
    /// Fired once when the turn completes (see [`TurnControls::defer_turn_completed`](super::TurnControls::defer_turn_completed)).
    pub completion: Option<Arc<dyn TurnCompletionSink>>,
    /// Refuse to run when the harness's declared tools differ from the
    /// session's frozen [`tinyagents_harness::tool::ToolSnapshot`]. On by default.
    pub enforce_tool_snapshot: bool,
}

impl<State, C> TurnPlan<State, C>
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    /// A plan over `models` (first is default) with no tools or middleware.
    pub fn new(models: Vec<(String, Arc<dyn ChatModel<State>>)>) -> Self {
        Self {
            models,
            policy: RunPolicy::default(),
            tools: ToolSetBuilder::new(),
            context: Vec::new(),
            tool_policy: None,
            approval: None,
            listeners: Vec::new(),
            completion: None,
            enforce_tool_snapshot: true,
        }
    }
}

/// Produces the [`TurnPlan`] for each turn.
#[async_trait]
pub trait TurnPlanner<State, C>: Send + Sync
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    /// Plans one turn. The request is the session's frozen view; the planner
    /// reads history, tools, context data and [`TurnControls`](super::TurnControls) from it.
    async fn plan(&self, request: &DriverRequest<C>) -> Result<TurnPlan<State, C>, RuntimeError>;
}

#[async_trait]
impl<State, C, F> TurnPlanner<State, C> for F
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
    F: Fn(&DriverRequest<C>) -> Result<TurnPlan<State, C>, RuntimeError> + Send + Sync,
{
    async fn plan(&self, request: &DriverRequest<C>) -> Result<TurnPlan<State, C>, RuntimeError> {
        self(request)
    }
}

/// A [`SessionDriver`] that plans, assembles and runs a harness per turn.
pub struct TurnDriver<State: Send + Sync + 'static, C: Send + Sync + 'static = ()> {
    planner: Arc<dyn TurnPlanner<State, C>>,
    state: Arc<State>,
}

impl<State: Send + Sync + 'static, C: Send + Sync + 'static> TurnDriver<State, C> {
    /// Binds a planner and the immutable host state the harness runs over.
    pub fn new(planner: Arc<dyn TurnPlanner<State, C>>, state: Arc<State>) -> Self {
        Self { planner, state }
    }
}

/// Publishes what is known when a turn fails before the harness runs.
fn publish_failed(controls: &TurnControls, max_iterations: usize, tools: ToolSetReport) {
    controls.report.set(TurnReport {
        scope: controls.scope.clone(),
        max_iterations,
        tools,
        completion_deferred: controls.defer_turn_completed,
        ..TurnReport::default()
    });
}

fn failure(error: RuntimeError) -> DriverFailure {
    DriverFailure {
        error,
        partial: None,
        outcome: None,
    }
}

/// A turn that has been planned and wired but not yet run.
///
/// [`TurnDriver::prepare`] returns it so a host that drives the harness itself
/// (a hosted invocation, a bespoke stream) can still take the plan's models,
/// tools, middleware and steering wiring from the driver. [`Self::run`] drives
/// the harness directly; [`Self::split`] hands the harness and run context to
/// the host and keeps the post-run bookkeeping in a [`TurnTail`].
pub struct PreparedTurn<State: Send + Sync + 'static, C: Send + Sync + 'static> {
    /// The assembled harness: models, tools, middleware and policy.
    pub harness: AgentHarness<State, C>,
    /// The run context, with steering, events and the cap pauser wired.
    pub run_context: RunContext<C>,
    /// The session's frozen history, the run's input.
    pub history: Vec<Message>,
    /// Whether the session asked for a streamed run.
    pub stream: bool,
    state: Arc<State>,
    tail: TurnTail,
}

/// The post-run half of a [`PreparedTurn`]: it keeps steering forwarded until
/// the run settles, then reads the early exit and publishes the [`TurnReport`].
pub struct TurnTail {
    guard: Option<FeedGuard>,
    hook: Option<EarlyExitHook>,
    controls: TurnControls,
    max_iterations: usize,
    tools: ToolSetReport,
    completion: Option<Arc<dyn TurnCompletionSink>>,
    outcomes: tinyagents_harness::turn_outcome::ToolOutcomeSink,
}

impl TurnTail {
    /// The model-call cap the run enforces (the stricter of config and policy).
    pub fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// How tool registration resolved for this turn.
    pub fn tools(&self) -> &ToolSetReport {
        &self.tools
    }

    /// Stops steering forwarding, reads the early exit, publishes the report to
    /// [`TurnControls::report`] and returns it. `run` is the finished run, or
    /// `None` when the turn failed before producing one (no cap is detected
    /// then).
    pub fn finish(self, run: Option<&AgentRun>) -> TurnReport {
        let tool_outcomes = tinyagents_harness::turn_outcome::drain_tool_outcomes(&self.outcomes);
        // Stop forwarding and deregister now, not at end of scope.
        drop(self.guard);
        let early_exit = self.hook.and_then(|hook| hook.take());
        let hit_cap = run.is_some_and(|run| {
            detect_hit_cap_with_cause(
                run,
                &TurnEndInputs {
                    max_iterations: self.max_iterations,
                    pause_at_cap: self.controls.pause_at_cap,
                    early_exit: early_exit.is_some(),
                    breaker_halt: None,
                    wrap_up_injected: false,
                },
                run.failure_cause.as_ref(),
            )
        });
        let report = TurnReport {
            scope: self.controls.scope.clone(),
            early_exit,
            hit_cap,
            model_calls: run.map_or(0, |run| run.model_calls),
            max_iterations: self.max_iterations,
            tools: self.tools,
            tool_outcomes,
            completion_deferred: self.controls.defer_turn_completed,
            ..TurnReport::default()
        }
        .with_completion(self.completion);
        self.controls.report.set(report.clone());
        report
    }
}

/// A finished turn: the harness's full outcome and the driver's report.
pub struct TurnRun {
    /// The run and its error, exactly as the harness returned them.
    pub partial: PartialRunOutcome,
    /// What the driver learned about how the turn ended.
    pub report: TurnReport,
    /// How many history messages were the run's input.
    pub input_len: usize,
}

impl<State: Send + Sync + 'static, C: Send + Sync + 'static> PreparedTurn<State, C> {
    /// How tool registration resolved for this turn.
    pub fn tools(&self) -> &ToolSetReport {
        self.tail.tools()
    }

    /// The model-call cap the run enforces (the stricter of config and policy).
    pub fn max_iterations(&self) -> usize {
        self.tail.max_iterations()
    }

    /// Drives the harness over the run context and settles the turn.
    pub async fn run(self) -> TurnRun {
        let Self {
            harness,
            run_context,
            history,
            stream,
            state,
            tail,
        } = self;
        let input_len = history.len();
        // Boxed: a child turn runs nested inside its parent's drive future.
        let partial = if stream {
            Box::pin(harness.invoke_streaming_in_context_collecting_partial(
                state.as_ref(),
                run_context,
                history,
            ))
            .await
        } else {
            Box::pin(harness.invoke_in_context_collecting_partial(
                state.as_ref(),
                run_context,
                history,
            ))
            .await
        };
        let succeeded = partial.error.is_none();
        let report = tail.finish(succeeded.then_some(&partial.run));
        TurnRun {
            partial,
            report,
            input_len,
        }
    }

    /// Hands the harness, run context and history to the caller and keeps the
    /// steering wiring in the returned [`TurnTail`]. The caller drives the
    /// harness itself, then calls [`TurnTail::finish`].
    pub fn split(self) -> (HarnessParts<State, C>, TurnTail) {
        let Self {
            harness,
            run_context,
            history,
            stream,
            state,
            tail,
        } = self;
        (
            HarnessParts {
                harness,
                run_context,
                history,
                stream,
                state,
            },
            tail,
        )
    }
}

/// The harness half of a split [`PreparedTurn`].
pub struct HarnessParts<State: Send + Sync + 'static, C: Send + Sync + 'static> {
    /// The assembled harness.
    pub harness: AgentHarness<State, C>,
    /// The wired run context.
    pub run_context: RunContext<C>,
    /// The run's input history.
    pub history: Vec<Message>,
    /// Whether a streamed run was requested.
    pub stream: bool,
    /// The immutable host state the harness runs over.
    pub state: Arc<State>,
}

impl<State: Send + Sync + 'static, C: Send + Sync + 'static> TurnDriver<State, C> {
    /// Plans and wires one turn without running it. Failures before the harness
    /// would run publish their [`TurnReport`] and return as a [`DriverFailure`].
    #[allow(clippy::result_large_err)] // `DriverFailure` is the session driver's own error type
    pub async fn prepare(
        &self,
        mut request: DriverRequest<C>,
    ) -> Result<PreparedTurn<State, C>, DriverFailure> {
        // One steering handle per run; early exit, the cap pause, the run queue
        // and the planner's own breakers all go through it. It is set on the
        // context before planning so the planner can wire middleware to it.
        let scope = request.controls.scope.clone();
        let handle = request
            .run_context
            .steering
            .clone()
            .unwrap_or_else(|| SteeringHandle::new(scope.steering_policy()));
        if request.run_context.steering.is_none() {
            request.run_context = request.run_context.with_steering(handle.clone());
        }
        let plan = match self.planner.plan(&request).await {
            Ok(plan) => plan,
            Err(error) => {
                publish_failed(&request.controls, 0, ToolSetReport::default());
                return Err(failure(error));
            }
        };
        let DriverRequest {
            history,
            tools: snapshot,
            mut run_context,
            stream,
            controls,
        } = request;
        let TurnPlan {
            models,
            mut policy,
            mut tools,
            context,
            tool_policy,
            approval,
            listeners,
            completion,
            enforce_tool_snapshot,
        } = plan;

        // The cap: an explicit control wins over the policy default; a zero
        // means "unset". The harness takes the stricter of config and policy,
        // so raise the policy too.
        let explicit_cap = controls.max_iterations.filter(|n| *n > 0);
        if let Some(n) = explicit_cap {
            policy.limits.max_model_calls = n;
            let capped = run_context.config.max_model_calls.map_or(n, |c| c.min(n));
            run_context.config.max_model_calls = Some(capped);
        }
        // The harness enforces the stricter of an explicit config cap and the
        // policy cap; the pauser must trip on that same number.
        let max_iterations = run_context
            .config
            .max_model_calls
            .map_or(policy.limits.max_model_calls, |cap| {
                cap.min(policy.limits.max_model_calls)
            });

        let mut harness: AgentHarness<State, C> = AgentHarness::new();
        harness.with_policy(policy);
        if let Some((primary_name, primary_model)) = models.first().cloned() {
            let routes = models.iter().skip(1).cloned().collect::<Vec<_>>();
            let resolver =
                PrebuiltModelResolver::new(primary_model, routes).with_primary_name(primary_name);
            harness.with_model_resolver(Arc::new(resolver));
        }
        let outcome_sink = tinyagents_harness::turn_outcome::new_tool_outcome_sink();
        harness.push_middleware(Arc::new(
            tinyagents_harness::middleware::library::ToolOutcomeCaptureMiddleware::new(Arc::clone(
                &outcome_sink,
            )),
        ));
        for middleware in context {
            harness.push_middleware(middleware);
        }
        if let Some(policy) = tool_policy {
            let mut gate = ToolPolicyGateMiddleware::new(policy);
            if let Some(resolver) = approval {
                gate = gate.with_approval_resolver(resolver);
            }
            harness.push_tool_middleware(Arc::new(gate));
        }

        let hook =
            (!controls.early_exit_tools.is_empty()).then(|| EarlyExitHook::new(handle.clone()));
        if scope.is_subagent() {
            tools.mark_subagent();
        }
        let early_exit_args: Option<(&BTreeSet<String>, &EarlyExitHook)> =
            hook.as_ref().map(|hook| (&controls.early_exit_tools, hook));
        let tool_report = tools.register(&mut harness, early_exit_args);

        if enforce_tool_snapshot && !same_tools(&harness.tools().declared_specs(), snapshot.specs())
        {
            publish_failed(&controls, max_iterations, tool_report);
            return Err(failure(RuntimeError::ToolSnapshotMismatch));
        }

        run_context.config.tags.push(scope.tag().to_string());
        for listener in listeners {
            run_context.events.subscribe(listener);
        }
        if controls.pause_at_cap {
            let pauser = match tinyagents_harness::events::CapPauser::with_recorder(
                handle.clone(),
                max_iterations,
                run_context.run_id().as_str(),
                controls.cap_pause_recorder.clone(),
            ) {
                Ok(pauser) => pauser,
                Err(error) => {
                    publish_failed(&controls, max_iterations, tool_report);
                    return Err(failure(RuntimeError::Driver(error.to_string())));
                }
            };
            run_context.events.subscribe(pauser);
        }

        let task_id = match &scope {
            TurnScope::Subagent(child) => Some(child.task_id.clone()),
            TurnScope::Root => None,
        };
        let label = controls
            .thread_label
            .clone()
            .or_else(|| task_id.clone())
            .or_else(|| run_context.config.thread_id.as_ref().map(|t| t.to_string()))
            .unwrap_or_default();
        let cleanup: Option<tinyagents_harness::run_queue::ForwarderCleanup> =
            match (&controls.registrar, &task_id) {
                (Some(registrar), Some(task_id)) => {
                    registrar.register(task_id, &handle);
                    let (registrar, task_id) = (registrar.clone(), task_id.clone());
                    Some(Box::new(move || registrar.deregister(&task_id)))
                }
                _ => None,
            };
        // Armed before the pre-run drain so a registration can never outlive
        // the turn, whatever path it exits by.
        let guard = match (&controls.steering, cleanup) {
            (Some(feed), cleanup) => {
                let guard = feed.arm(handle.clone(), label.clone(), cleanup);
                feed.prime(&handle, &label).await;
                Some(guard)
            }
            (None, Some(cleanup)) => Some(cleanup_only_guard(handle.clone(), cleanup, label)),
            (None, None) => None,
        };

        Ok(PreparedTurn {
            harness,
            run_context,
            history,
            stream,
            state: self.state.clone(),
            tail: TurnTail {
                guard,
                hook,
                controls,
                max_iterations,
                tools: tool_report,
                completion,
                outcomes: outcome_sink,
            },
        })
    }
}

#[async_trait]
impl<State: Send + Sync + 'static, C: Send + Sync + 'static> SessionDriver<C>
    for TurnDriver<State, C>
{
    async fn execute(&self, request: DriverRequest<C>) -> Result<DriverOutcome, DriverFailure> {
        let scope = request.controls.scope.clone();
        let defer = request.controls.defer_turn_completed;
        let prepared = self.prepare(request).await?;
        let TurnRun {
            partial,
            report,
            input_len,
        } = prepared.run().await;

        let mut outcome = outcome_from_partial(partial, input_len);
        let tool_outcomes = report.tool_outcomes.clone();
        match &mut outcome {
            Ok(done) => done.tool_outcomes = tool_outcomes,
            Err(failure) => {
                if let Some(partial) = &mut failure.partial {
                    partial.tool_outcomes = tool_outcomes;
                }
            }
        }
        // An early-exit tool's output stands in for the paused assistant turn,
        // which carries the call and no answer.
        if let (Some(exit), Ok(done)) = (&report.early_exit, &mut outcome) {
            done.output = Some(exit.question.clone());
        }
        if outcome.is_ok() && !defer && !scope.is_subagent() {
            report.emit_completed();
        }
        outcome
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
