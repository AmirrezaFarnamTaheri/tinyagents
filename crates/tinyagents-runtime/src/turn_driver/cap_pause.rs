//! Graceful stop at the model-call cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use tinyagents_harness::events::{AgentEvent, EventListener, EventRecord};
use tinyagents_harness::steering::{SteeringCommand, SteeringHandle};

/// Called as the pause is requested with `(completed_calls, cap)`, before the
/// advisory pause command is sent. A host records the pause here so that a
/// tool dispatch racing the loop boundary can observe it.
pub type CapPauseObserver = Arc<dyn Fn(u32, u32) + Send + Sync>;

/// Pauses the run once `cap` of its **own** model calls have completed, so the
/// loop ends with its partial transcript instead of a limit error.
///
/// Only calls minted for this run count (`{run_id}-model-{n}`): the sink is
/// shared with nested runs whose completions must not shorten this run.
pub struct CapPauser {
    handle: SteeringHandle,
    cap: u32,
    run_id: String,
    completed: AtomicU32,
    fired: Arc<AtomicBool>,
    observer: Option<CapPauseObserver>,
}

impl CapPauser {
    /// Pauses `handle` after `cap` completions of run `run_id`. `fired` is set
    /// when the pause is requested.
    pub fn new(
        handle: SteeringHandle,
        cap: usize,
        run_id: impl Into<String>,
        fired: Arc<AtomicBool>,
        observer: Option<CapPauseObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle,
            cap: u32::try_from(cap).unwrap_or(u32::MAX),
            run_id: run_id.into(),
            completed: AtomicU32::new(0),
            fired,
            observer,
        })
    }

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
            return;
        }
        let n = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= self.cap {
            tracing::info!(
                completed = n,
                cap = self.cap,
                "[turn-driver] model-call cap reached: requesting graceful pause"
            );
            self.fired.store(true, Ordering::SeqCst);
            if let Some(observer) = &self.observer {
                observer(n, self.cap);
            }
            self.handle.send(SteeringCommand::Pause);
        }
    }
}
