//! Process-local ownership of workflow drive tasks.
//!
//! A host supplies its task spawner (for example, a runtime context's `spawn`)
//! and the closure that drives a run. Every start, including a resume, gets a
//! fresh cancellation generation. A handle from an older generation cannot
//! stop a replacement loop.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tinyagents_harness::CancellationToken;

#[derive(Debug, Clone)]
struct Generation {
    id: u64,
    token: CancellationToken,
}

/// Handle for one scheduled drive generation.
#[derive(Debug, Clone)]
pub struct WorkflowRunHandle {
    run_id: Arc<str>,
    generation: u64,
    token: CancellationToken,
}

impl WorkflowRunHandle {
    /// Cancellation token the workflow engine should observe while driving.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Durable workflow id associated with this generation.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
}

#[derive(Debug, Default)]
struct Registry {
    next_generation: u64,
    current: HashMap<String, Generation>,
}

/// Starts and cancels host-scheduled workflow drive loops.
///
/// The supervisor owns only process-local task identity and cancellation.
/// Durable lifecycle transitions remain the responsibility of the caller.
#[derive(Debug, Clone, Default)]
pub struct WorkflowRunSupervisor {
    registry: Arc<Mutex<Registry>>,
}

impl WorkflowRunSupervisor {
    /// Install a fresh generation and schedule its drive future.
    ///
    /// `spawn` is a host seam: pass a closure backed by the host's task context.
    /// The future factory receives the generation's cancellation token so the
    /// caller can pass it directly to `WorkflowEngine::drive` or
    /// `drive_async`. Callers may wrap the future with their root-parent and
    /// origin scopes before handing it to the spawner.
    pub fn start<F, MakeFuture, Spawn>(
        &self,
        run_id: impl Into<String>,
        make_future: MakeFuture,
        spawn: Spawn,
    ) -> WorkflowRunHandle
    where
        F: Future<Output = ()> + Send + 'static,
        MakeFuture: FnOnce(CancellationToken) -> F,
        Spawn: FnOnce(F),
    {
        let run_id = run_id.into();
        let generation = {
            let mut registry = self.registry.lock().expect("workflow registry poisoned");
            registry.next_generation = registry.next_generation.wrapping_add(1);
            let generation = Generation {
                id: registry.next_generation,
                token: CancellationToken::new(),
            };
            registry.current.insert(run_id.clone(), generation.clone());
            generation
        };
        let handle = WorkflowRunHandle {
            run_id: Arc::from(run_id),
            generation: generation.id,
            token: generation.token.clone(),
        };
        spawn(make_future(generation.token));
        handle
    }

    /// Install a new generation for a resumed run and schedule its drive.
    pub fn resume<F, MakeFuture, Spawn>(
        &self,
        run_id: impl Into<String>,
        make_future: MakeFuture,
        spawn: Spawn,
    ) -> WorkflowRunHandle
    where
        F: Future<Output = ()> + Send + 'static,
        MakeFuture: FnOnce(CancellationToken) -> F,
        Spawn: FnOnce(F),
    {
        self.start(run_id, make_future, spawn)
    }

    /// Cancel the current generation for `run_id`, if one is scheduled.
    ///
    /// Returns `false` if no generation is registered.
    pub fn stop(&self, run_id: &str) -> bool {
        let registry = self.registry.lock().expect("workflow registry poisoned");
        let Some(generation) = registry.current.get(run_id) else {
            return false;
        };
        generation.token.cancel();
        true
    }

    /// Cancel only when `handle` still owns the run's current generation.
    /// This is safe for late callbacks from a stopped loop after resume.
    pub fn stop_generation(&self, handle: &WorkflowRunHandle) -> bool {
        let registry = self.registry.lock().expect("workflow registry poisoned");
        let Some(current) = registry.current.get(handle.run_id()) else {
            return false;
        };
        if current.id != handle.generation {
            return false;
        }
        current.token.cancel();
        true
    }

    /// Remove a finished generation without disturbing a newer replacement.
    pub fn finish(&self, handle: &WorkflowRunHandle) -> bool {
        let mut registry = self.registry.lock().expect("workflow registry poisoned");
        if registry
            .current
            .get(handle.run_id())
            .is_some_and(|current| current.id == handle.generation)
        {
            registry.current.remove(handle.run_id());
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;
