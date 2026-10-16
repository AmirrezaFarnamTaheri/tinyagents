//! Failure-scope driver for host policies that classify tool results while
//! sharing the harness's repeat and recoverable-failure accounting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyinference_llm::tool::ToolCall;
use tinytools::ToolResult;

use crate::context::RunContext;
use crate::error::Result;
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::no_progress::{
    ClassifiedFailureTracker, ClassifiedStepWithCause, FailurePolicy, NoProgress,
    NoProgressTracker, RecoverableFailureLadder, ToolAttempt, classified_step_with_cause,
    clear_recovered, fingerprint_arguments,
};
use tinyinference_llm::model::ModelResponse;
use crate::steering::{SteeringCommand, SteeringHandle};
use crate::turn_outcome::HaltSummarySlot;

use super::{EphemeralInstructionMiddleware, EphemeralInstructionQueue};

/// Host-derived identity for one tool call's failure accounting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureScope {
    /// Stable argument fingerprint used by repeat accounting.
    pub argument_fingerprint: String,
    /// Host resource identity used by classification and diagnostics.
    pub scope: String,
}

/// Classification returned by a host's failure-scope policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureAssessment {
    /// A successful result; clears the generic no-progress streak.
    Progress,
    /// Host-owned refusal/exemption that must not affect generic failure counts.
    Ignore,
    /// Deterministic failure counted by the generic no-progress ladder.
    Failure {
        /// Error text the model saw, already scrubbed by the host.
        error: String,
        /// A hard reject that should use the short repeat threshold.
        hard_reject: bool,
        /// A recoverable miss that should not count against the varied-failure
        /// backstop.
        recoverable_miss: bool,
    },
    /// Transient/recoverable failure with its separate extended budget.
    RecoverableFailure {
        /// Error text passed to the host's halt-copy callback.
        error: String,
    },
    /// Host-classified failure with a retry budget and optional corrective.
    ClassifiedFailure {
        /// Stable host-owned recovery class.
        class: String,
        /// Retries allowed after the first failure.
        budget: usize,
        /// Scrubbed failure text used for the typed stop cause.
        error: String,
        /// Host wording for the within-budget corrective, if this class has one.
        nudge: Option<String>,
    },
    /// Immediate host-classified halt, such as a terminal inference failure.
    Halt {
        /// Host-authored root-cause summary.
        summary: String,
        /// Optional stable class and operation metadata for the turn result.
        cause: Option<crate::no_progress::ClassifiedStopCause>,
    },
}

/// Host policy for deciding which failures share a scope and which ladder
/// should handle a completed result. OpenHuman supplies classification,
/// exemptions and user-facing copy; TinyAgents owns call correlation, counters,
/// recoverable thresholds, ephemeral nudge delivery and pause mechanics.
pub trait FailureScopePolicy<Ctx: Send + Sync>: FailurePolicy {
    /// Derives a stable failure scope from a tool's concrete call arguments.
    fn scope(&self, ctx: &RunContext<Ctx>, call: &ToolCall) -> FailureScope {
        let _ = ctx;
        FailureScope {
            argument_fingerprint: fingerprint_arguments(&call.arguments),
            scope: call.name.clone(),
        }
    }

    /// Classifies the final model-visible tool result.
    fn assess(
        &self,
        ctx: &RunContext<Ctx>,
        invocation: &ToolInvocationIdentity,
        scope: &FailureScope,
        result: &ToolResult,
    ) -> FailureAssessment;

    /// Optional model-response fast halt, called after every model response.
    fn terminal_model_halt(
        &self,
        _ctx: &RunContext<Ctx>,
        _response: &ModelResponse,
    ) -> Option<(String, Option<crate::no_progress::ClassifiedStopCause>)> {
        None
    }

    /// Host copy applied to a generic ladder's stop summary.
    fn halt_copy(&self, _kind: FailureStopKind, summary: String, _error: &str) -> String {
        summary
    }
}

/// Which generic stop mechanism produced a halt for host-authored copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureStopKind {
    /// Identical or varied deterministic failure ladder.
    NoProgress,
    /// Host-classified recovery budget was exhausted.
    Classified,
    /// Extended transient failure budget was exhausted.
    Recoverable,
    /// Host detected a terminal failure immediately.
    Terminal,
}

struct RunFailureState {
    tracker: NoProgressTracker,
    recoverable: RecoverableFailureLadder,
    classified: ClassifiedFailureTracker,
    step: AtomicUsize,
}

impl RunFailureState {
    fn new(identical_threshold: usize) -> Self {
        Self {
            tracker: NoProgressTracker::new(identical_threshold),
            recoverable: RecoverableFailureLadder::default(),
            classified: ClassifiedFailureTracker::default(),
            step: AtomicUsize::new(0),
        }
    }
}

/// Shared run-scoped driver for no-progress and recoverable tool failures.
pub struct FailureScopeMiddleware<Ctx: Send + Sync> {
    label: &'static str,
    handle: SteeringHandle,
    halt_summary: HaltSummarySlot,
    policy: Arc<dyn FailureScopePolicy<Ctx>>,
    identical_threshold: usize,
    calls: Mutex<HashMap<(u64, String), FailureScope>>,
    runs: Mutex<HashMap<u64, Arc<RunFailureState>>>,
    nudges: EphemeralInstructionQueue,
}

impl<Ctx: Send + Sync> FailureScopeMiddleware<Ctx> {
    /// Creates the driver with host policy, steering and shared halt summary.
    pub fn new(
        handle: SteeringHandle,
        halt_summary: HaltSummarySlot,
        policy: Arc<dyn FailureScopePolicy<Ctx>>,
    ) -> Self {
        Self::with_identical_threshold(handle, halt_summary, policy, 3)
    }

    /// Creates the driver with an explicit same-failure threshold. The tracker
    /// clamps the threshold so at least one corrective nudge precedes a halt.
    pub fn with_identical_threshold(
        handle: SteeringHandle,
        halt_summary: HaltSummarySlot,
        policy: Arc<dyn FailureScopePolicy<Ctx>>,
        identical_threshold: usize,
    ) -> Self {
        Self {
            label: "failure_scope",
            handle,
            halt_summary,
            policy,
            identical_threshold,
            calls: Mutex::default(),
            runs: Mutex::default(),
            nudges: EphemeralInstructionQueue::new(),
        }
    }

    /// Request-scoped middleware that inserts queued nudges into the next
    /// dispatched model request only.
    pub fn nudge_injector(&self) -> EphemeralInstructionMiddleware {
        self.nudges.middleware()
    }

    fn run_state(&self, instance_id: u64) -> Arc<RunFailureState> {
        let mut runs = self
            .runs
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        Arc::clone(
            runs.entry(instance_id)
                .or_insert_with(|| Arc::new(RunFailureState::new(self.identical_threshold))),
        )
    }

    fn halt<C>(
        &self,
        ctx: &mut RunContext<C>,
        summary: String,
        cause: Option<crate::no_progress::ClassifiedStopCause>,
    ) {
        let first = {
            let mut slot = self
                .halt_summary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.get_or_insert(summary).clone()
        };
        if ctx.halted_by_guard.is_none() {
            ctx.halted_by_guard = Some(first);
            ctx.halted_by_guard_cause = cause;
        }
        self.handle.send(SteeringCommand::Pause);
    }

    fn observe(
        &self,
        ctx: &mut RunContext<Ctx>,
        invocation: &ToolInvocationIdentity,
        scope: FailureScope,
        result: &ToolResult,
    ) {
        let tool = invocation.tool_name();
        let assessment = self.policy.assess(ctx, invocation, &scope, result);
        let run = self.run_state(ctx.instance_id());
        let step = run.step.fetch_add(1, Ordering::SeqCst) + 1;
        let argument_key = format!("{}\u{1f}{}", scope.argument_fingerprint, scope.scope);
        match assessment {
            FailureAssessment::Ignore => {}
            FailureAssessment::Progress => {
                let attempt = ToolAttempt::success(tool, &argument_key);
                let _ = run.tracker.record(step, &attempt);
                run.recoverable.reset_streak();
            }
            FailureAssessment::Halt { summary, cause } => {
                run.tracker.reset();
                run.recoverable.reset_streak();
                self.halt(ctx, summary, cause);
            }
            FailureAssessment::RecoverableFailure { error } => {
                if let Some(summary) =
                    run.recoverable
                        .record(self.policy.as_ref(), tool, &argument_key, &error)
                {
                    self.halt(ctx, summary, None);
                    run.recoverable.reset_streak();
                }
            }
            FailureAssessment::Failure {
                error,
                hard_reject,
                recoverable_miss,
            } => {
                run.recoverable.reset_streak();
                let attempt = ToolAttempt {
                    tool,
                    arg_fingerprint: &argument_key,
                    error: Some(&error),
                    hard_reject,
                    recoverable_miss,
                };
                match run.tracker.record(step, &attempt) {
                    NoProgress::Continue => {}
                    NoProgress::Nudge(instruction) => self.nudges.push(ctx, instruction),
                    NoProgress::Halt(summary) => {
                        run.tracker.reset();
                        self.halt(ctx, summary, None);
                    }
                }
            }
        }
    }

    fn clear_run(&self, instance_id: u64) {
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&instance_id);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(run, _), _| *run != instance_id);
    }
}

#[async_trait]
impl<Ctx: Send + Sync> Middleware<(), Ctx> for FailureScopeMiddleware<Ctx> {
    fn name(&self) -> &str {
        self.label
    }

    fn is_observer(&self) -> bool {
        true
    }

    async fn before_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        call: &mut ToolCall,
    ) -> Result<()> {
        let scope = self.policy.scope(ctx, call);
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((ctx.instance_id(), call.id.to_string()), scope);
        Ok(())
    }

    async fn after_tool(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        invocation: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> Result<()> {
        let scope = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(ctx.instance_id(), invocation.call_id().to_string()))
            .unwrap_or_else(|| FailureScope {
                argument_fingerprint: String::new(),
                scope: invocation.tool_name().to_owned(),
            });
        self.observe(ctx, invocation, scope, result);
        Ok(())
    }

    async fn after_agent(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &(),
        _run: &mut crate::middleware::AgentRun,
    ) -> Result<()> {
        self.clear_run(ctx.instance_id());
        Ok(())
    }

    async fn on_error(
        &self,
        ctx: &mut RunContext<Ctx>,
        _error: &crate::error::TinyAgentsError,
    ) -> Result<()> {
        self.clear_run(ctx.instance_id());
        Ok(())
    }
}

#[cfg(test)]
#[path = "failure_scope_tests.rs"]
mod tests;
