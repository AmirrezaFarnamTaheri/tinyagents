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
    update: Mutex<()>,
    notifier: Arc<dyn ModeChangeNotifier<S>>,
}
impl<S: Clone + Eq + Hash> InMemoryRunModeStore<S> {
    pub fn new(notifier: Arc<dyn ModeChangeNotifier<S>>) -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
            update: Mutex::new(()),
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
        self.handles
            .lock()
            .get(&(scope.clone(), thread_id.to_owned()))
            .map_or(RunMode::Build, RunModeHandle::get)
    }
    fn set_mode(&self, scope: &S, thread_id: &str, mode: RunMode) {
        let changed = {
            let _update = self.update.lock();
            let key = (scope.clone(), thread_id.to_owned());
            let mut handles = self.handles.lock();
            match handles.get(&key) {
                Some(handle) if handle.get() == mode => false,
                Some(handle) => {
                    handle.set(mode);
                    true
                }
                None if mode == RunMode::Build => false,
                None => {
                    handles.insert(key, RunModeHandle::new(mode));
                    true
                }
            }
        };
        if changed {
            self.notifier.changed(scope, thread_id, mode);
        }
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
