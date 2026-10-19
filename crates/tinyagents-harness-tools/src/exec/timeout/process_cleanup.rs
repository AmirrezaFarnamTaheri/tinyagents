//! Track command reaping for an embedder that awaits turn cancellation.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

tokio::task_local! {
    static ACTIVE: Vec<ProcessCleanup>;
}

/// Command waiters belonging to one turn. Clone before scoping the turn;
/// after dropping the turn future, [`wait`](Self::wait) awaits their reaping.
/// Tasks spawned by a host tool must explicitly inherit this scope.
#[derive(Clone, Default, Debug)]
pub struct ProcessCleanup(Arc<Mutex<Vec<watch::Receiver<bool>>>>);

impl ProcessCleanup {
    /// Whether the current task must acknowledge owned subprocess cleanup.
    /// Interpreter pools have no per-job cancellation acknowledgement and
    /// therefore cannot accept work from this scope.
    #[must_use]
    pub fn is_active() -> bool {
        ACTIVE.try_with(|_| ()).is_ok()
    }

    /// Run a future with command waiters registered to this turn.
    pub async fn scope<T>(&self, future: impl Future<Output = T>) -> T {
        let mut scopes = ACTIVE.try_with(Clone::clone).unwrap_or_default();
        scopes.push(self.clone());
        ACTIVE.scope(scopes, future).await
    }

    /// Register work that the current turn's [`wait`](Self::wait) must outlast
    /// until the returned hold is dropped: the explicit inheritance a host tool
    /// needs when it hands a command to a task or service of its own. Take it
    /// inside the scope (before spawning), move it into the detached work, and
    /// drop it once that work has stopped. Outside any scope it holds nothing.
    pub fn hold() -> CleanupHold {
        CleanupHold {
            _reaped: Reaped::register(),
        }
    }

    /// Wait for every registered command to exit and its output pipes to close.
    /// Call only after the scoped future has finished or been dropped, so no
    /// further commands can be registered by it.
    pub async fn wait(&self) {
        let waiters = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for mut waiter in waiters {
            let _ = waiter.wait_for(|done| *done).await;
        }
    }
}

/// Keeps the turn's cleanup waiting while alive. See [`ProcessCleanup::hold`].
#[must_use = "the cleanup stops waiting as soon as the hold is dropped"]
pub struct CleanupHold {
    _reaped: Reaped,
}

impl std::fmt::Debug for CleanupHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanupHold").finish_non_exhaustive()
    }
}

pub(super) struct Reaped(watch::Sender<bool>);

impl Reaped {
    pub(super) fn register() -> Self {
        let (done, waiter) = watch::channel(false);
        let _ = ACTIVE.try_with(|scopes| {
            for cleanup in scopes {
                cleanup
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(waiter.clone());
            }
        });
        Self(done)
    }
}

impl Drop for Reaped {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
