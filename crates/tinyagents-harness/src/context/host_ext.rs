//! Typed host extension slots on a run.
//!
//! A host (an application embedding the harness) often has per-run values the
//! harness itself knows nothing about: a turn origin, a workspace handle, a
//! progress sink. Ambient `task_local!` state carries them today, but
//! task-locals do not cross a process or module boundary and are invisible to
//! spawned work. [`HostExtensions`] is the explicit replacement: a small map
//! from a Rust type to one shared value of that type, carried on the
//! [`RunContext`](super::RunContext) and handed to every tool through
//! [`ToolExecutionContext`](crate::tool::ToolExecutionContext).
//!
//! The map is copy-on-write: cloning it (which a forked child context does)
//! is a refcount bump, and installing a value in the clone never alters the
//! parent's view.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Type-keyed map of shared, immutable host values.
///
/// Holds at most one value per type. Values are `Arc`-shared so a fork costs
/// a refcount bump; interior mutability, where a host needs it, lives inside
/// the value (for example a `Mutex` or an atomic).
#[derive(Clone, Default)]
pub struct HostExtensions {
    slots: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl HostExtensions {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a copy of this map with `value` installed, replacing any
    /// previous value of the same type. The receiver is untouched.
    #[must_use]
    pub fn with<X: Any + Send + Sync>(&self, value: X) -> Self {
        self.with_arc(Arc::new(value))
    }

    /// As [`with`](Self::with) for a value that is already shared.
    #[must_use]
    pub fn with_arc<X: Any + Send + Sync>(&self, value: Arc<X>) -> Self {
        let mut slots = (*self.slots).clone();
        slots.insert(TypeId::of::<X>(), value);
        Self {
            slots: Arc::new(slots),
        }
    }

    /// Returns a copy of this map with the `X` slot removed.
    #[must_use]
    pub fn without<X: Any + Send + Sync>(&self) -> Self {
        let mut slots = (*self.slots).clone();
        slots.remove(&TypeId::of::<X>());
        Self {
            slots: Arc::new(slots),
        }
    }

    /// The installed `X`, or `None` when nothing of that type is installed.
    #[must_use]
    pub fn get<X: Any + Send + Sync>(&self) -> Option<&X> {
        self.slots
            .get(&TypeId::of::<X>())
            .and_then(|slot| slot.downcast_ref::<X>())
    }

    /// The installed `X` as a shared handle, for callers that must keep it
    /// beyond the borrow of the context (a spawned task, say).
    #[must_use]
    pub fn get_arc<X: Any + Send + Sync>(&self) -> Option<Arc<X>> {
        self.slots
            .get(&TypeId::of::<X>())
            .cloned()
            .and_then(|slot| slot.downcast::<X>().ok())
    }

    /// Whether a value of type `X` is installed.
    #[must_use]
    pub fn contains<X: Any + Send + Sync>(&self) -> bool {
        self.slots.contains_key(&TypeId::of::<X>())
    }

    /// As [`with_arc`](Self::with_arc), but keeps an `X` that is already
    /// installed. A fork uses this to supply a default without clobbering
    /// the value a parent set.
    #[must_use]
    pub fn with_arc_if_absent<X: Any + Send + Sync>(&self, value: Arc<X>) -> Self {
        if self.contains::<X>() {
            self.clone()
        } else {
            self.with_arc(value)
        }
    }

    /// Returns a copy of this map with every slot of `other` installed over
    /// it; on a type present in both, `other` wins. Neither input changes.
    #[must_use]
    pub fn overlay(&self, other: &Self) -> Self {
        if other.is_empty() {
            return self.clone();
        }
        let mut slots = (*self.slots).clone();
        for (key, value) in other.slots.iter() {
            slots.insert(*key, Arc::clone(value));
        }
        Self {
            slots: Arc::new(slots),
        }
    }

    /// Number of installed slots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether no slot is installed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

impl std::fmt::Debug for HostExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostExtensions")
            .field("slots", &self.slots.len())
            .finish()
    }
}

#[cfg(test)]
#[path = "host_ext_tests.rs"]
mod tests;
