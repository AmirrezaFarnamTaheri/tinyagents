//! The host's say over every filesystem tool call.
//!
//! The tools in this module read, write, search and patch files, but they do
//! not decide what they are *allowed* to touch. That decision (an autonomy
//! level, a workspace boundary, an approval prompt, an hourly action budget,
//! credential stores that are never reachable) belongs to the host whose threat
//! model and configuration it depends on. A tool asks an [`FsGate`] instead.
//!
//! The trait is exactly the set of questions the tools put to a policy, and
//! nothing wider: it carries no decision *types* of its own, only booleans and
//! resolved paths, so a host maps its own policy onto it without translating a
//! vocabulary.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tinytools::ToolRunContext;

/// The host policy a filesystem tool consults before it acts.
///
/// Implementations must be cheap to call: the tools ask on every invocation.
/// All methods take `&self`; an implementation that counts actions keeps that
/// state behind interior mutability.
#[async_trait]
pub trait FsGate: std::fmt::Debug + Send + Sync {
    /// Whether the host permits the agent to change anything at all right now.
    ///
    /// `false` blocks every mutating tool before it touches the disk.
    fn can_act(&self) -> bool;

    /// Whether the host is in a strictly read-only mode.
    ///
    /// Asked by `git_operations`, on top of [`can_act`](Self::can_act), before
    /// a write operation runs.
    fn is_read_only(&self) -> bool;

    /// Whether the action budget is already spent, without consuming any of it.
    fn is_rate_limited(&self) -> bool;

    /// Consume one unit of the action budget.
    ///
    /// Returns `false` when the call is over budget and must be refused.
    fn record_action(&self) -> bool;

    /// Whether a write must be confirmed by a human before it runs.
    ///
    /// Drives the tools' `external_effect_with_args` flag, which routes the
    /// call through the host's approval flow.
    fn write_needs_approval(&self) -> bool;

    /// The directory relative paths resolve against.
    fn action_dir(&self) -> &Path;

    /// Cheap, string-level check of whether `path` may be used at all.
    ///
    /// Used to filter search hits without touching the disk.
    fn is_path_string_allowed(&self, path: &str) -> bool;

    /// Batch form of [`is_path_string_allowed`](Self::is_path_string_allowed).
    ///
    /// Answers are in request order and the result has the same length as
    /// `paths`. The default asks the single-path question once per entry, so
    /// every existing gate behaves exactly as before. A host that can answer a
    /// whole batch more cheaply (one round trip to a policy module instead of
    /// one per path) overrides this.
    ///
    /// A caller must treat an answer whose length differs from `paths` as
    /// every path refused.
    fn paths_string_allowed(&self, paths: &[String]) -> Vec<bool> {
        paths
            .iter()
            .map(|path| self.is_path_string_allowed(path))
            .collect()
    }

    /// Resolve an existing path and confirm the host allows it.
    ///
    /// # Errors
    ///
    /// Returns the message shown to the model when the path is refused or
    /// cannot be resolved.
    async fn validate_path(&self, path: &str) -> Result<PathBuf, String>;

    /// Resolve a path whose *parent* must exist but whose target may not, and
    /// confirm the host allows it. Used for writes that create files.
    ///
    /// # Errors
    ///
    /// Returns the message shown to the model when the path is refused or
    /// cannot be resolved.
    async fn validate_parent_path(&self, path: &str) -> Result<PathBuf, String>;

    /// A gate for the same policy, rooted at `root`.
    ///
    /// The run's isolated workspace becomes both the directory relative paths
    /// resolve against and a readable-and-writable root. The grant is
    /// per-call: it must not mutate `self`, so concurrent turns cannot race
    /// each other, and it must not widen whatever the host treats as always
    /// forbidden. `root` originates from trusted in-process code (the session
    /// builder or a sub-agent runner), never from model-supplied text.
    fn scoped_to_workspace(&self, root: &Path) -> Arc<dyn FsGate>;
}

/// Resolve the gate a tool should validate paths against for this call.
///
/// When the harness threaded a workspace descriptor into the call (an
/// edit-capable worker running in an isolated checkout), the returned gate is
/// scoped to that workspace root; otherwise it is `gate` itself. Counting and
/// approval questions ([`FsGate::can_act`], [`FsGate::record_action`], ...)
/// are still asked of the unscoped gate.
pub(crate) fn gate_for_context(
    gate: &Arc<dyn FsGate>,
    context: Option<&dyn ToolRunContext>,
    tool: &str,
) -> Arc<dyn FsGate> {
    match context.and_then(ToolRunContext::workspace) {
        Some(workspace) => {
            tracing::debug!(
                tool,
                workspace_root = %workspace.root.display(),
                policy_id = %workspace.policy_id,
                "[tools:filesystem] granting TinyAgents workspace descriptor as action dir + trusted root"
            );
            gate.scoped_to_workspace(&workspace.root)
        }
        None => Arc::clone(gate),
    }
}
