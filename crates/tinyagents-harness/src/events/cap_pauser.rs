//! Model-call cap pauser: an [`EventListener`] that pauses a run once a fixed
//! number of its **own** model calls have completed.
//!
//! The harness checks pending steering at the top of each turn *before* the
//! model-call limit check, so a [`SteeringCommand::Pause`] sent here
//! short-circuits the loop cleanly and the run returns its partial transcript
//! instead of erroring with `LimitExceeded`. The caller then inspects the run's
//! finish reason to decide whether to summarize a checkpoint.
//!
//! Only the capped run's own model calls count. The sink is shared with every
//! in-process nested run, and those children emit `ModelCompleted` too. The
//! crate mints a run's model call ids as `{run_id}-model-{n}` while a nested
//! child runs under its own `{name}-d{depth}-{seq}` id, so the call id alone
//! attributes a completion to its run at any nesting depth.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{Result, TinyAgentsError};
use crate::events::{AgentEvent, EventListener, EventRecord};
use crate::steering::{SteeringCommand, SteeringCommandKind, SteeringHandle};

/// Callback run with `(completed, cap)` immediately *before* the pause command
/// is sent, so a host can record the pause where a later dispatch must observe
/// it. The event queue notifies listeners in insertion order on the emitting
/// task, so anything stored here happens-before any tool call the loop
/// dispatches afterwards.
pub type PauseRecorder = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Pauses a run once `cap` of its own model calls have completed.
pub struct CapPauser {
    handle: SteeringHandle,
    cap: u32,
    run_id: String,
    completed: AtomicU32,
    on_pause: Option<PauseRecorder>,
}

impl CapPauser {
    /// Pause `handle` once `cap` of run `run_id`'s own model calls complete.
    ///
    /// Fails when `cap` is zero (a pause is only observed after a model call
    /// completes, so it cannot prevent the first call) or when the handle's
    /// steering policy does not permit [`SteeringCommandKind::Pause`] (the
    /// command would be queued and then skipped, so the cap would never hold).
    pub fn new(
        handle: SteeringHandle,
        cap: usize,
        run_id: impl Into<String>,
    ) -> Result<Arc<Self>> {
        Self::with_recorder(handle, cap, run_id, None)
    }

    /// Like [`CapPauser::new`], additionally running `on_pause` before the
    /// pause command is sent (see [`PauseRecorder`]).
    pub fn with_recorder(
        handle: SteeringHandle,
        cap: usize,
        run_id: impl Into<String>,
        on_pause: Option<PauseRecorder>,
    ) -> Result<Arc<Self>> {
        if cap == 0 {
            return Err(TinyAgentsError::Config(
                "cap pauser: a zero model-call cap cannot be enforced by pausing".into(),
            ));
        }
        if !handle.policy().is_allowed(SteeringCommandKind::Pause) {
            return Err(TinyAgentsError::Steering(
                "cap pauser: the steering policy does not permit Pause".into(),
            ));
        }
        Ok(Arc::new(Self {
            handle,
            cap: u32::try_from(cap).unwrap_or(u32::MAX),
            run_id: run_id.into(),
            completed: AtomicU32::new(0),
            on_pause,
        }))
    }

    /// Number of the run's own model calls observed so far.
    pub fn completed(&self) -> u32 {
        self.completed.load(Ordering::SeqCst)
    }

    /// `true` when `call_id` is one of the capped run's own model calls
    /// (`{run_id}-model-{n}`), not a nested run's.
    fn is_own_model_call(&self, call_id: &str) -> bool {
        call_id
            .strip_prefix(self.run_id.as_str())
            .and_then(|rest| rest.strip_prefix("-model-"))
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    }
}

impl EventListener for CapPauser {
    fn on_event(&self, record: &EventRecord) {
        let AgentEvent::ModelCompleted { call_id, .. } = &record.event else {
            return;
        };
        if !self.is_own_model_call(call_id.as_str()) {
            tracing::debug!(
                call_id = call_id.as_str(),
                run_id = %self.run_id,
                "[cap-pauser] ignoring a nested run's model completion"
            );
            return;
        }
        let n = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= self.cap {
            tracing::info!(
                completed = n,
                cap = self.cap,
                "[cap-pauser] model-call cap reached, requesting graceful pause"
            );
            if let Some(record_pause) = self.on_pause.as_ref() {
                record_pause(u64::from(n), u64::from(self.cap));
            }
            self.handle.send(SteeringCommand::Pause);
        }
    }
}

#[cfg(test)]
#[path = "cap_pauser_tests.rs"]
mod tests;
