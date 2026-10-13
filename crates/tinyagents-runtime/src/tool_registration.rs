//! Fail-closed tool admission and registration onto an [`AgentHarness`].
//!
//! The host decides *which* tools exist and how each is wrapped; this module
//! owns the generic rules for which candidates register: de-duplication by
//! name, an allowlist whose empty set denies everything, thread-scoped tools
//! dropped on thread-less turns, and spawn/delegate tools never handed to a
//! sub-agent run whatever the allowlist says.

use std::collections::HashSet;
use std::sync::Arc;

use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::tool::ToolDispatch;
use tinytools::Tool;

/// How one admitted tool is registered on the harness.
pub enum ToolRegistration<State, C> {
    /// A plain tool.
    Tool(Arc<dyn Tool>),
    /// A tool that needs the typed parent run.
    Dispatch(Arc<dyn ToolDispatch<State, C>>),
}

/// Admission rules for one turn's tool registration.
#[derive(Clone, Default)]
pub struct ToolAdmission {
    /// `None` admits every candidate; `Some(set)` admits exactly its members,
    /// so `Some(empty)` is an explicit deny-all.
    pub allowed: Option<HashSet<String>>,
    /// Whether the turn runs as a sub-agent.
    pub is_subagent_run: bool,
    /// Whether the turn has a chat thread.
    pub has_thread: bool,
    /// Names of tools that cannot run without a thread.
    pub thread_scoped: HashSet<String>,
    /// Names of spawn/delegate tools a sub-agent must never receive.
    pub spawn_tools: HashSet<String>,
}

/// The outcome of [`plan_registration`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegistrationPlan {
    /// De-duplicated candidates, in first-seen order, after thread filtering.
    pub candidates: Vec<String>,
    /// The subset of `candidates` that register, in the same order.
    pub admitted: Vec<String>,
    /// Spawn tools an allowlist tried to readmit on a sub-agent run.
    pub spawn_refused: Vec<String>,
}

/// Decides which of `names` register under `admission`.
pub fn plan_registration<I, S>(names: I, admission: &ToolAdmission) -> RegistrationPlan
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen = HashSet::new();
    let mut plan = RegistrationPlan::default();
    for name in names {
        let name = name.as_ref();
        if !seen.insert(name.to_string()) {
            continue;
        }
        if !admission.has_thread && admission.thread_scoped.contains(name) {
            tracing::debug!(tool = name, "[tool-registration] dropped thread-scoped tool");
            continue;
        }
        plan.candidates.push(name.to_string());
        let allowed = admission
            .allowed
            .as_ref()
            .is_none_or(|set| set.contains(name));
        if !allowed {
            continue;
        }
        if admission.is_subagent_run && admission.spawn_tools.contains(name) {
            tracing::warn!(tool = name, "[tool-registration] refused spawn tool on sub-agent");
            plan.spawn_refused.push(name.to_string());
            continue;
        }
        plan.admitted.push(name.to_string());
    }
    plan
}

/// Registers every admitted tool onto `harness`, asking `build` how to wrap
/// each. Names `build` returns `None` for are skipped. Returns the number
/// registered together with the plan.
pub fn register_admitted<State, C, I, S>(
    harness: &mut AgentHarness<State, C>,
    names: I,
    admission: &ToolAdmission,
    mut build: impl FnMut(&str) -> Option<ToolRegistration<State, C>>,
) -> (usize, RegistrationPlan)
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let plan = plan_registration(names, admission);
    let mut count = 0;
    for name in &plan.admitted {
        match build(name) {
            Some(ToolRegistration::Tool(tool)) => {
                harness.register_tool(tool);
            }
            Some(ToolRegistration::Dispatch(dispatch)) => {
                harness.register_tool_dispatch(dispatch);
            }
            None => continue,
        }
        count += 1;
    }
    (count, plan)
}

#[cfg(test)]
#[path = "tool_registration_tests.rs"]
mod tests;
