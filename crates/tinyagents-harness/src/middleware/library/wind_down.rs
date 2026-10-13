//! [`WindDownMiddleware`]: wind a top-level turn down before an outer
//! wall-clock backstop drops it.
//!
//! A host that runs a turn under an outer backstop (a web-chat timeout that
//! drops the turn future) loses everything the turn did when the backstop
//! fires. [`TurnDeadline`] describes the two deadlines the host derives from its
//! backstop; the middleware reacts to the first, [`install`] to the second:
//!
//! * **wind-down**: once elapsed time passes it, the middleware sends
//!   [`SteeringCommand::Pause`] at the next safe checkpoint, after a model call
//!   (that call's tool round still runs) or after a tool result. The loop
//!   drains the pause before its next provider call, so the host can write a
//!   grounded close from the completed tool results.
//! * **hard stop**: [`install`] clamps the harness `max_wall_clock_ms` to it,
//!   so a single tool call that runs across the wind-down point ends in the
//!   harness's own typed `Timeout` rather than a dropped turn.
//!
//! The thresholds (how far before the backstop each deadline sits) are host
//! policy: the host builds the [`TurnDeadline`]. [`TurnClockMiddleware`]
//! (`turn_clock`) tells the model about the same budget; this middleware is
//! what acts on it.
//!
//! [`TurnClockMiddleware`]: super::TurnClockMiddleware

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use async_trait::async_trait;

use crate::context::RunContext;
use crate::error::Result as TaResult;
use crate::middleware::{Middleware, ToolInvocationIdentity};
use crate::runtime::AgentHarness;
use crate::steering::{SteeringCommand, SteeringHandle};
use crate::turn_deadline::TurnDeadline;
use tinyinference_llm::model::ModelResponse;
use tinytools::ToolResult;

/// Callback run once, when the middleware sends its pause.
pub type WindDownCallback = Arc<dyn Fn() + Send + Sync>;

/// Pauses the run once its [`TurnDeadline`] has passed.
pub struct WindDownMiddleware {
    handle: SteeringHandle,
    deadline: TurnDeadline,
    /// Latched on the first pause, so `Pause` is sent exactly once.
    fired: Arc<AtomicBool>,
    on_pause: std::sync::OnceLock<WindDownCallback>,
}

impl WindDownMiddleware {
    /// Build the middleware over the run's steering `handle`.
    pub fn new(handle: SteeringHandle, deadline: TurnDeadline) -> Self {
        Self {
            handle,
            deadline,
            fired: Arc::new(AtomicBool::new(false)),
            on_pause: std::sync::OnceLock::new(),
        }
    }

    /// Run `callback` when the pause is sent (the host records the wind-down
    /// on its own turn state). Set once, before the run starts.
    pub fn set_on_pause(&self, callback: WindDownCallback) {
        let _ = self.on_pause.set(callback);
    }

    /// Whether this middleware has paused the run.
    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// Pause the run if the wind-down point has passed as of `now`. Returns
    /// whether this call sent the pause.
    pub fn maybe_pause(&self, checkpoint: &'static str, now: Instant) -> bool {
        if !self.deadline.should_wind_down(now) || self.fired.swap(true, Ordering::SeqCst) {
            return false;
        }
        tracing::warn!(
            target: "tinyagents::middleware",
            checkpoint,
            elapsed_ms = self.deadline.elapsed_at(now).as_millis() as u64,
            wind_down_ms = self.deadline.wind_down_after().as_millis() as u64,
            backstop_ms = self.deadline.backstop().as_millis() as u64,
            "[wind_down] wind-down point passed; pausing the run so the turn closes before the backstop"
        );
        self.handle.send(SteeringCommand::Pause);
        if let Some(callback) = self.on_pause.get() {
            callback();
        }
        true
    }
}

#[async_trait]
impl<State, Ctx> Middleware<State, Ctx> for WindDownMiddleware
where
    State: Send + Sync,
    Ctx: Send + Sync,
{
    fn name(&self) -> &str {
        "wind_down"
    }

    async fn after_model(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &State,
        _response: &mut ModelResponse,
    ) -> TaResult<()> {
        self.maybe_pause("after_model", Instant::now());
        Ok(())
    }

    async fn after_tool(
        &self,
        _ctx: &mut RunContext<Ctx>,
        _state: &State,
        _invocation: &ToolInvocationIdentity,
        _result: &mut ToolResult,
    ) -> TaResult<()> {
        self.maybe_pause("after_tool", Instant::now());
        Ok(())
    }
}

/// Install the wind-down for a top-level run that carries a deadline, and
/// clamp the harness wall-clock ceiling to its hard stop.
///
/// Runs without a deadline, and children (`is_child`), are left untouched: a
/// synchronous child is bounded by its parent's tool-call budget, which the
/// clamp already ends at the hard stop, and a detached child must not inherit
/// the deadline of the turn that spawned it. Without a steering `handle` the
/// clamp still applies but no middleware is added.
pub fn install<State, Ctx>(
    harness: &mut AgentHarness<State, Ctx>,
    handle: Option<&SteeringHandle>,
    deadline: Option<TurnDeadline>,
    is_child: bool,
) -> Option<Arc<WindDownMiddleware>>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
{
    let deadline = deadline.filter(|_| !is_child)?;
    let now = Instant::now();
    let mut policy = harness.policy().clone();
    let configured = policy.limits.max_wall_clock_ms;
    policy.limits.max_wall_clock_ms = deadline.clamp_wall_clock_ms(configured, now);
    tracing::debug!(
        target: "tinyagents::middleware",
        configured_ms = ?configured,
        clamped_ms = ?policy.limits.max_wall_clock_ms,
        elapsed_ms = deadline.elapsed_at(now).as_millis() as u64,
        hard_stop_ms = deadline.hard_stop_after().as_millis() as u64,
        "[wind_down] clamped harness wall clock to the turn's hard stop"
    );
    harness.with_policy(policy);
    let handle = handle?;
    let middleware = Arc::new(WindDownMiddleware::new(handle.clone(), deadline));
    harness.push_middleware(middleware.clone());
    Some(middleware)
}

#[cfg(test)]
#[path = "wind_down_tests.rs"]
mod tests;
