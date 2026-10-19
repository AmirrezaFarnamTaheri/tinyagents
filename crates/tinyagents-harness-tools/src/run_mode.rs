//! Reusable per-thread run-mode storage and change notification.
use parking_lot::Mutex;
use std::{collections::HashMap, hash::Hash, sync::Arc};
use tinyagents_harness::middleware::library::{RunMode, RunModeHandle};

pub trait ModeChangeNotifier<S>: Send + Sync {
    fn changed(&self, scope: &S, thread_id: &str, mode: RunMode);
}
pub trait RunModeStore<S>: Send + Sync {
    fn handle_for_thread(&self, scope: &S, thread_id: &str) -> RunModeHandle;
    fn get_mode(&self, scope: &S, thread_id: &str) -> RunMode;
    fn set_mode(&self, scope: &S, thread_id: &str, mode: RunMode);
}

pub struct InMemoryRunModeStore<S> {
    handles: Mutex<HashMap<(S, String), RunModeHandle>>,
    notifier: Arc<dyn ModeChangeNotifier<S>>,
}
impl<S: Clone + Eq + Hash> InMemoryRunModeStore<S> {
    pub fn new(notifier: Arc<dyn ModeChangeNotifier<S>>) -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
            notifier,
        }
    }
}
impl<S: Clone + Eq + Hash + Send + Sync + 'static> RunModeStore<S> for InMemoryRunModeStore<S> {
    fn handle_for_thread(&self, scope: &S, thread_id: &str) -> RunModeHandle {
        self.handles
            .lock()
            .entry((scope.clone(), thread_id.to_owned()))
            .or_default()
            .clone()
    }
    fn get_mode(&self, scope: &S, thread_id: &str) -> RunMode {
        self.handle_for_thread(scope, thread_id).get()
    }
    fn set_mode(&self, scope: &S, thread_id: &str, mode: RunMode) {
        self.handle_for_thread(scope, thread_id).set(mode);
        self.notifier.changed(scope, thread_id, mode);
    }
}

pub fn mode_label(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Build => "build",
        RunMode::Plan => "plan",
    }
}
pub fn parse_mode_label(label: &str) -> Option<RunMode> {
    match label {
        "build" => Some(RunMode::Build),
        "plan" => Some(RunMode::Plan),
        _ => None,
    }
}

#[cfg(test)]
#[path = "run_mode_tests.rs"]
mod tests;
