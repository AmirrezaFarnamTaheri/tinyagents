//! A generic tool-registration builder over shared tool sets.
//!
//! A host owns its tools as `Arc<Vec<Box<dyn Tool>>>` sets that outlive any one
//! turn. [`ToolSetBuilder`] registers the admitted subset onto a turn's
//! [`AgentHarness`] through the harness's [`CanonicalSharedToolAdapter`], which
//! resolves each tool by name on every call, so nothing is cloned. Admission is
//! the fail-closed rules of [`crate::plan_registration`]; the host decides only
//! how a tool with special execution needs is wrapped ([`ToolSetBuilder::dispatch`])
//! and what ambient scope executions run under ([`ToolSetBuilder::exec_scope`]).

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::tool::{
    CanonicalSharedToolAdapter, EarlyExitHook, ToolDispatch, ToolExecScope,
};
use tinytools::{Tool, ToolExposure};

use crate::{
    RegistrationPlan, ToolAdmission, ToolRegistration, ToolSurfaceDiff, register_admitted,
    tool_surface_diff,
};

/// One shared, non-cloneable set of tools.
pub type SharedToolSet = Arc<Vec<Box<dyn Tool>>>;

/// Chooses a typed dispatch for tools that need the live parent run. Receives
/// the tool's name and its registered adapter; `None` registers a plain tool.
///
/// The returned dispatch must execute **through the adapter it was given**
/// (the harness's own canonical dispatch does). The adapter carries the turn's
/// early-exit hook and execution scope, so a dispatch that runs the underlying
/// tool some other way opts that tool out of both.
pub type DispatchSelector<State, C> =
    Arc<dyn Fn(&str, Arc<dyn Tool>) -> Option<Arc<dyn ToolDispatch<State, C>>> + Send + Sync>;

/// How registration resolved for one turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolSetReport {
    /// Names that were registered, in registration order.
    pub registered: Vec<String>,
    /// Admission outcome: candidates, admitted, refused spawn tools.
    pub plan: RegistrationPlan,
}

/// Registers an admitted subset of shared tool sets onto a harness.
pub struct ToolSetBuilder<State, C> {
    sets: Vec<SharedToolSet>,
    admission: ToolAdmission,
    deferred: HashSet<String>,
    dispatch: Option<DispatchSelector<State, C>>,
    scope: Option<Arc<dyn ToolExecScope>>,
}

impl<State, C> Default for ToolSetBuilder<State, C> {
    fn default() -> Self {
        Self {
            sets: Vec::new(),
            admission: ToolAdmission::default(),
            deferred: HashSet::new(),
            dispatch: None,
            scope: None,
        }
    }
}

impl<State, C> ToolSetBuilder<State, C>
where
    State: Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    /// An empty builder (registers nothing until a set is added).
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a shared tool set. Earlier sets win a name collision.
    #[must_use]
    pub fn set(mut self, set: SharedToolSet) -> Self {
        self.sets.push(set);
        self
    }

    /// Adds several shared tool sets.
    #[must_use]
    pub fn sets(mut self, sets: impl IntoIterator<Item = SharedToolSet>) -> Self {
        self.sets.extend(sets);
        self
    }

    /// The admission rules. `is_subagent_run` is also forced on by a
    /// sub-agent [`crate::turn_driver::TurnScope`].
    #[must_use]
    pub fn admission(mut self, admission: ToolAdmission) -> Self {
        self.admission = admission;
        self
    }

    /// Tools registered as `Deferred` whatever their own exposure says. A
    /// `Hidden` tool stays hidden: deferral only ever subtracts.
    #[must_use]
    pub fn deferred(mut self, names: impl IntoIterator<Item = String>) -> Self {
        self.deferred.extend(names);
        self
    }

    /// Selects typed dispatches for tools that need the live parent run.
    #[must_use]
    pub fn dispatch(mut self, selector: DispatchSelector<State, C>) -> Self {
        self.dispatch = Some(selector);
        self
    }

    /// Runs every tool execution inside `scope`.
    #[must_use]
    pub fn exec_scope(mut self, scope: Arc<dyn ToolExecScope>) -> Self {
        self.scope = Some(scope);
        self
    }

    /// The de-duplicated tool names across all sets, in first-seen order.
    pub fn candidate_names(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.sets
            .iter()
            .flat_map(|set| set.iter())
            .map(|tool| tool.name().to_string())
            .filter(|name| seen.insert(name.clone()))
            .collect()
    }

    /// Diffs this builder's de-duplicated tool names against a freshly
    /// synthesized name set. Hosts can use this at each turn boundary to
    /// withdraw revoked tools and decide whether wildcard policy admits new
    /// names, while rebuilding the builder from the current executable sets.
    pub fn surface_diff(&self, next: &HashSet<String>) -> ToolSurfaceDiff {
        let previous = self.candidate_names().into_iter().collect();
        tool_surface_diff(&previous, next)
    }

    /// Forces the sub-agent admission rule on.
    pub(crate) fn mark_subagent(&mut self) {
        self.admission.is_subagent_run = true;
    }

    /// Registers the admitted tools onto `harness`. With no allowlist, tools
    /// that declare `Hidden` exposure are skipped. Every name in
    /// `early_exit_tools` that registers pauses the run through `hook` after a
    /// successful call.
    pub fn register(
        &self,
        harness: &mut AgentHarness<State, C>,
        early_exit: Option<(&BTreeSet<String>, &EarlyExitHook)>,
    ) -> ToolSetReport {
        if self
            .admission
            .allowed
            .as_ref()
            .is_some_and(HashSet::is_empty)
        {
            tracing::warn!(
                subagent = self.admission.is_subagent_run,
                "[tool-set] allowlist resolved empty: registering no tools"
            );
        }
        // A wildcard admits every *visible* tool. A `Hidden` tool is exposed
        // only when an allowlist names it, because the adapter reports an
        // unforced hidden tool as `Direct`.
        let wildcard = self.admission.allowed.is_none();
        let names: Vec<String> = self
            .candidate_names()
            .into_iter()
            .filter(|name| {
                let hidden = self
                    .sets
                    .iter()
                    .flat_map(|set| set.iter())
                    .find(|tool| tool.name() == name)
                    .is_some_and(|tool| tool.exposure() == ToolExposure::Hidden);
                if wildcard && hidden {
                    tracing::debug!(tool = %name, "[tool-set] wildcard skips hidden tool");
                }
                !(wildcard && hidden)
            })
            .collect();
        let mut registered = Vec::new();
        let (_, plan) = register_admitted(harness, names, &self.admission, |name| {
            let mut adapter = CanonicalSharedToolAdapter::for_name(self.sets.clone(), name)?
                .deferred(self.deferred.contains(name));
            if let Some(scope) = &self.scope {
                adapter = adapter.with_exec_scope(scope.clone());
            }
            if let Some((tools, hook)) = early_exit
                && tools.contains(name)
            {
                adapter = adapter.with_early_exit(hook.clone());
            }
            let adapter: Arc<dyn Tool> = Arc::new(adapter);
            registered.push(name.to_string());
            Some(
                match self
                    .dispatch
                    .as_ref()
                    .and_then(|select| select(name, adapter.clone()))
                {
                    Some(dispatch) => ToolRegistration::Dispatch(dispatch),
                    None => ToolRegistration::Tool(adapter),
                },
            )
        });
        ToolSetReport { registered, plan }
    }
}

#[cfg(test)]
#[path = "tool_set_tests.rs"]
mod tests;
