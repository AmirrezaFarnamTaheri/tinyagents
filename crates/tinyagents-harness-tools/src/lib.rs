//! Default tool implementations and their stateful harness support.
//!
//! [`tinyagents_harness`] owns tool infrastructure such as rules, deferral,
//! schema preparation, discovery and selection. This crate owns the reusable
//! implementations that hosts may opt into by feature.

mod ask_clarification;
#[cfg(feature = "skills")]
mod canonical_json;
pub mod per_call;
mod plan_exit;
pub mod schema;
#[cfg(feature = "plan-review")]
pub mod plan_review;
pub mod thread_locks;
mod time;
mod time_parse;

#[cfg(feature = "goals")]
pub mod goals;
#[cfg(feature = "exec")]
pub mod exec;
#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "media")]
pub mod media;
#[cfg(feature = "net")]
pub mod net;
#[cfg(feature = "plan-review")]
pub mod plan_review;
#[cfg(feature = "skills")]
pub mod skills;
#[cfg(feature = "subagents")]
pub mod subagents;
#[cfg(feature = "todos")]
pub mod todos;

pub use ask_clarification::AskClarificationTool;
pub use plan_exit::{PLAN_EXIT_MARKER, PlanExitModeLookup, PlanExitTool};
pub use time::{CurrentTimeTool, ResolveTimeTool, register_time_tools, time_tools};

#[cfg(test)]
#[path = "time_parse_tests.rs"]
mod time_parse_tests;
#[cfg(test)]
#[path = "time_tests.rs"]
mod time_tests;
