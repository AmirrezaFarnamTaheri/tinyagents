//! Parallel fan-out of independent branches over [`tinyagents_graph::parallel`].
//!
//! A host builds a [`Plan`] of [`Task`]s, supplies an [`AdmissionPolicy`] that
//! decides (in its own vocabulary) whether each task may run and what it needs
//! from the shared workspace, and runs the staged [`Worker`]s with [`run`].
//!
//! - [`staging`]: admission, shared-workspace arbitration and the ownership
//!   boundary prompt. Conflicts are reported as data ([`Rejection`]) so the
//!   rejection sentence stays a host decision.
//! - [`workers`]: serial fallback for shared-workspace writers, bounded
//!   `map_reduce` fan-out otherwise. Results are returned in **input order**
//!   and every branch receives its own context from a factory (per-branch
//!   isolation).
//! - [`collect`]: aggregate results and cross-branch overlap detection.
//!
//! Nothing here performs I/O beyond what the host's branch executor does;
//! policy, progress and approvals remain host concerns.

pub mod collect;
pub mod hooks;
pub mod staging;
pub mod workers;

pub use collect::{BranchReport, Collected, OverlapWarning, overlap_warnings};
pub use staging::{
    AdmissionPolicy, AdmissionVerdict, Plan, Preflight, Rejection, RejectionKind, Task, Worker,
    WorkerDispatchMode, with_ownership_boundary,
};
pub use hooks::{RunHooks, resolve_concurrency, run_with_hooks};
pub use workers::run;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod hooks_tests;
