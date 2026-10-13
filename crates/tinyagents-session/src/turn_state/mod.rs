//! Restart-survivable snapshots of in-flight agent turns.
//!
//! Persistence goes through the `TurnStates` port (in-memory or driver-backed;
//! at cold boot a host flags any non-terminal snapshot left over from an unclean
//! shutdown as interrupted). [`types`] holds the wire/storage
//! shapes (camelCase, mirroring a chat-runtime UI slice).
//!
//! [`mirror::TurnStateMirror`] is the writer: it owns the in-memory snapshot,
//! the flush policy, the size caps, interrupted-turn finalization and the
//! transcript bookkeeping helpers. Translating a host's own progress events into
//! mutations stays host work.

pub mod mirror;
pub mod types;

pub use mirror::TurnStateMirror;
pub use types::{
    SubagentActivity, SubagentToolCall, ToolTimelineEntry, ToolTimelineStatus, TurnLifecycle,
    TurnPhase, TurnState,
};

#[cfg(test)]
#[path = "shape_tests.rs"]
mod shape_test;
