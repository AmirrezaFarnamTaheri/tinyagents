//! Driver-backed asynchronous subagent jobs and their lifecycle controls.
//!
//! Hosts create subagent requests through `SubagentDriver` and register the
//! resulting jobs here for status, cancellation, messaging, and descendant
//! settlement. This module does not construct or synchronously invoke child
//! harnesses.

mod jobs;
mod settlement;
mod status_map;
mod types;

pub use jobs::JobSettleGuard;
pub use settlement::RootRunDescendantLease;
pub use types::*;

#[cfg(test)]
#[path = "mod_registry_tests.rs"]
mod registry_tests;
