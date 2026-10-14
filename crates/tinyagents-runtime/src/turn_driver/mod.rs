//! A generic turn driver and tool-registration builder.
//!
//! [`TurnDriver`] is a [`crate::SessionDriver`] that plans, assembles and runs
//! one harness per turn. A host supplies a [`TurnPlanner`] returning a
//! [`TurnPlan`] (models, tools, context middleware, tool policy) and passes the
//! per-turn [`TurnControls`] through [`crate::TurnOptions::controls`]:
//!
//! | Control | Effect |
//! | --- | --- |
//! | `max_iterations`, `pause_at_cap` | model-call cap; stop gracefully with the partial transcript instead of a limit error |
//! | `early_exit_tools` | a successful call pauses the loop and its output becomes the turn's text |
//! | `defer_turn_completed` | the completion sink is fired later by the caller |
//! | `scope` | root turn or delegated child (steering policy, spawn-tool refusal, no auto-completion) |
//! | `steering`, `registrar` | a run queue feeding mid-flight messages, and a registry for a child's steering handle |
//!
//! After `Session::turn` returns, read the [`TurnReport`] from
//! [`TurnControls::report`].

mod driver;
mod steering;
mod tool_set;
mod types;

pub use driver::{TurnDriver, TurnPlan, TurnPlanner};
pub use steering::{FeedGuard, RunQueueFeed, SteeringFeed, SteeringRegistrar};
pub use tool_set::{DispatchSelector, SharedToolSet, ToolSetBuilder, ToolSetReport};
pub use types::{
    EarlyExit, SubagentScope, TurnCompletionSink, TurnControls, TurnReport, TurnReportSlot,
    TurnScope,
};
