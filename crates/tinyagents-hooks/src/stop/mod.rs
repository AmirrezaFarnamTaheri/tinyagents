//! Policies evaluated after each completed model response.

mod hooks;
mod latch;

pub use hooks::{StopDecision, StopHook, StopHookMetrics, StopHookMiddleware, StopReport, TurnState};
pub use latch::StopLatch;
