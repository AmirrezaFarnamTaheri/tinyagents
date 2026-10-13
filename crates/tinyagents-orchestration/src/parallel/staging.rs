//! Admission and shared-workspace arbitration for a batch of parallel tasks.

use std::collections::HashMap;
use std::path::PathBuf;

use tinyagents_graph::parallel::{
    ClaimConflict, DispatchMode, WorkspaceClaim, plan_shared_workspace_dispatch,
};

/// One branch requested by the caller. `P` is the host's own payload.
#[derive(Debug, Clone)]
pub struct Task<P> {
    /// Stable branch id (also the workspace-claim worker id).
    pub id: String,
    /// The instruction the branch runs.
    pub prompt: String,
    /// Free-form ownership boundary, folded into the prompt when present.
    pub ownership: Option<String>,
    /// Host payload carried through to the worker untouched.
    pub payload: P,
}

/// The host's verdict on one task, in terms the arbiter understands.
#[derive(Debug, Clone)]
pub enum AdmissionVerdict {
    /// The branch never writes the shared workspace.
    ReadOnly,
    /// The branch has its own isolated workspace root.
    Isolated,
    /// The branch writes the shared workspace under these relative paths.
    Writes(Vec<PathBuf>),
    /// The host refuses the branch; the string is the host's own sentence.
    Reject(String),
}

/// Host policy: identity, allowlists, sandbox mode and whether a branch can
/// write. Everything product-specific lives behind this trait.
pub trait AdmissionPolicy<P> {
    fn admit(&self, index: usize, task: &Task<P>) -> AdmissionVerdict;
}

/// How a prepared worker runs relative to its siblings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerDispatchMode {
    Parallel,
    /// Writes the shared workspace; the whole batch falls back to serial.
    SerialSharedWorkspaceWrite,
}

/// Why a task did not become a [`Worker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectionKind {
    /// The host policy refused it.
    Policy(String),
    /// Its claim conflicts with a sibling; render the data into a message.
    Conflict(ClaimConflict),
}

/// A rejected task, kept in input position.
#[derive(Debug, Clone)]
pub struct Rejection<P> {
    pub task: Task<P>,
    pub kind: RejectionKind,
}

/// A task cleared for execution.
#[derive(Debug, Clone)]
pub struct Worker<P> {
    pub id: String,
    /// Prompt with the ownership boundary applied.
    pub prompt: String,
    pub ownership: Option<String>,
    pub payload: P,
    pub dispatch_mode: WorkerDispatchMode,
}

/// Staging outcome for one input task.
#[derive(Debug, Clone)]
pub enum Preflight<P> {
    Prepared(Worker<P>),
    Rejected(Rejection<P>),
}

/// An ordered batch of tasks.
#[derive(Debug, Clone)]
pub struct Plan<P> {
    pub tasks: Vec<Task<P>>,
}

impl<P> Plan<P> {
    pub fn new(tasks: Vec<Task<P>>) -> Self {
        Self { tasks }
    }

    /// Admit every task, then arbitrate the shared-workspace claims of the
    /// admitted ones in a single planner call. The result is a pure function
    /// of the input order, one entry per input task.
    pub fn stage(self, policy: &impl AdmissionPolicy<P>) -> Vec<Preflight<P>> {
        enum Slot<P> {
            Admitted(Task<P>, WorkspaceClaim),
            Rejected(Task<P>, String),
        }
        let slots: Vec<Slot<P>> = self
            .tasks
            .into_iter()
            .enumerate()
            .map(|(i, task)| match policy.admit(i, &task) {
                AdmissionVerdict::Reject(msg) => Slot::Rejected(task, msg),
                AdmissionVerdict::ReadOnly => {
                    let c = WorkspaceClaim::read_only(task.id.clone());
                    Slot::Admitted(task, c)
                }
                AdmissionVerdict::Isolated => {
                    let c = WorkspaceClaim::isolated(task.id.clone());
                    Slot::Admitted(task, c)
                }
                AdmissionVerdict::Writes(paths) => {
                    let c = WorkspaceClaim::writing(task.id.clone(), paths);
                    Slot::Admitted(task, c)
                }
            })
            .collect();

        let claims: Vec<WorkspaceClaim> = slots
            .iter()
            .filter_map(|s| match s {
                Slot::Admitted(_, c) => Some(c.clone()),
                Slot::Rejected(..) => None,
            })
            .collect();
        let plan = plan_shared_workspace_dispatch(&claims);
        let conflicts: HashMap<usize, ClaimConflict> = plan.conflicts.iter().cloned().collect();

        let mut admitted_index = 0usize;
        slots
            .into_iter()
            .map(|slot| match slot {
                Slot::Rejected(task, msg) => Preflight::Rejected(Rejection {
                    task,
                    kind: RejectionKind::Policy(msg),
                }),
                Slot::Admitted(task, _) => {
                    let index = admitted_index;
                    admitted_index += 1;
                    if let Some(conflict) = conflicts.get(&index) {
                        return Preflight::Rejected(Rejection {
                            task,
                            kind: RejectionKind::Conflict(conflict.clone()),
                        });
                    }
                    let dispatch_mode = match plan.modes.get(index).copied().flatten() {
                        Some(DispatchMode::Serial) => {
                            WorkerDispatchMode::SerialSharedWorkspaceWrite
                        }
                        Some(DispatchMode::Parallel) | None => WorkerDispatchMode::Parallel,
                    };
                    let prompt = with_ownership_boundary(&task.prompt, task.ownership.as_deref());
                    Preflight::Prepared(Worker {
                        id: task.id,
                        prompt,
                        ownership: task.ownership,
                        payload: task.payload,
                        dispatch_mode,
                    })
                }
            })
            .collect()
    }
}

/// Fold an ownership boundary into the branch prompt.
pub fn with_ownership_boundary(prompt: &str, ownership: Option<&str>) -> String {
    match ownership.map(str::trim).filter(|s| !s.is_empty()) {
        Some(boundary) => format!(
            "[Ownership Boundary]\n{boundary}\n\n[Task]\n{prompt}\n\nDo not work outside the ownership boundary unless the parent explicitly asks you to."
        ),
        None => prompt.to_string(),
    }
}
