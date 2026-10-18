//! Process-local steering handles for detached task runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tinyagents_harness::ids::TaskId;
use tinyagents_harness::steering::SteeringHandle;

/// A registry mapping managed task ids to the live steering handle of the run.
///
/// Executors register a child's handle while its run is active. The registry
/// is shared with [`DetachedTaskRegistry`](crate::DetachedTaskRegistry), which
/// removes the handle when the detached task is released.
#[derive(Clone, Default)]
pub struct SteeringRegistry {
    inner: Arc<Mutex<HashMap<TaskId, SteeringHandle>>>,
}

impl SteeringRegistry {
    /// Creates an empty steering registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the steering handle for `task_id`, replacing any prior handle.
    pub fn register(&self, task_id: TaskId, handle: SteeringHandle) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.insert(task_id, handle);
        }
    }

    /// Removes and returns the handle registered for `task_id`, if any.
    pub fn deregister(&self, task_id: &TaskId) -> Option<SteeringHandle> {
        self.inner.lock().ok().and_then(|mut guard| guard.remove(task_id))
    }

    /// Returns the handle registered for `task_id`, if any.
    pub fn get(&self, task_id: &TaskId) -> Option<SteeringHandle> {
        self.inner.lock().ok().and_then(|guard| guard.get(task_id).cloned())
    }
}
