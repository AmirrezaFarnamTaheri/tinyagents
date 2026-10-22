//! Compatibility shim: the detached-task machinery moved to `tinyagents-tasks`.
//!
//! The detached-task runtime types still provided by `tinyagents-tasks` are
//! re-exported here so their existing `tinyagents_graph::orchestration::*`
//! paths keep compiling. Removed orchestration tool adapters and recovery
//! helpers are not part of this compatibility surface. Prefer
//! `tinyagents_tasks` in new code.

pub use tinyagents_tasks::*;

#[cfg(test)]
#[path = "orchestration_tests.rs"]
mod tests;
