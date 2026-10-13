//! Per-model-call usage dedupe and a shared 1-based iteration cursor.
//!
//! A single model call can surface more than one `UsageRecorded` event (the
//! runtime's own plus an observe-only budget middleware's re-emit), all
//! carrying identical usage. A listener that accounts usage must record each
//! call exactly once. The run-scoped model-call identity is the iteration
//! cursor, bumped once per `ModelStarted`, so [`CallDedupe`] keys on it.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// A shared 1-based model-call (iteration) cursor. A listener advances it on
/// each `ModelStarted`; adapters read it to attribute out-of-band deltas.
pub type IterationCursor = Arc<AtomicU32>;

/// Advance `cursor` for a new model call and return the new iteration.
pub fn advance_iteration(cursor: &IterationCursor) -> u32 {
    cursor.fetch_add(1, Ordering::SeqCst) + 1
}

/// Records which model-call iterations were already accounted.
#[derive(Default)]
pub struct CallDedupe {
    seen: Mutex<HashSet<u32>>,
}

impl CallDedupe {
    /// `true` the first time `iteration` is claimed, `false` for any duplicate.
    pub fn first_claim(&self, iteration: u32) -> bool {
        self.seen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(iteration)
    }
}

#[cfg(test)]
#[path = "call_dedupe_tests.rs"]
mod tests;
