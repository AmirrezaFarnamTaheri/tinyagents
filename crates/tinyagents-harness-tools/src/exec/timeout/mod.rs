//! Tool-execution timeouts and child-process management.
//!
//! - [`settings`]: bounds, resolution and the process-global inherited timeout.
//!   The host reads its config and env, then calls [`configure`].
//! - [`output_or_kill`] / [`output_unbounded`] / [`output_with_input`]: capture a
//!   command's output, killing its whole process group on a deadline or drop.
//! - [`ProcessCleanup`]: lets an embedder await reaping after cancelling a turn;
//!   [`CleanupHold`] extends that wait to work a host tool hands off to its own task.
//! - [`CommandEnvironment`]: a turn-owned child environment, never the host's.

#![allow(
    clippy::missing_errors_doc,
    clippy::ignored_unit_patterns,
    clippy::struct_field_names,
    clippy::collapsible_if
)]

mod command_environment;
mod group_exit;
mod output;
mod process_cleanup;
mod settings;

pub use command_environment::CommandEnvironment;
pub use output::{
    kill_process_group, output_or_kill, output_unbounded, output_with_input, own_process_group,
    set_task_wrapper,
};
pub use process_cleanup::{CleanupHold, ProcessCleanup};
pub use settings::{
    DEFAULT_TIMEOUT_SECS, MAX_TIMEOUT_SECS, MIN_TIMEOUT_SECS, ResolvedTimeout,
    SANDBOX_UNBOUNDED_CAP_SECS, TOOL_TIMEOUT_GRACE_SECS, TimeoutSettings, configure,
    explicit_call_timeout_duration, explicit_call_timeout_secs, override_from,
    parse_tool_timeout_secs, resolve_effective, settings,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
