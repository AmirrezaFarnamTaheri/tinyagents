//! A generic tool-registration builder over shared, non-cloneable tool sets.
//!
//! A host owns its tools as `Arc<Vec<Box<dyn Tool>>>` sets that outlive any one
//! turn. [`ToolSetBuilder`] registers the admitted subset onto a turn's
//! [`AgentHarness`] through a thin [`SharedToolAdapter`] that resolves the tool
//! by name on every call, so nothing is cloned. Admission is the fail-closed
//! rules of [`crate::plan_registration`]; the host decides only how a tool
//! with special execution needs is wrapped ([`ToolSetBuilder::dispatch`]) and
//! what ambient scope its execution runs under ([`ToolSetBuilder::exec_scope`]).

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::tool::ToolDispatch;
use tinytools::{Tool, ToolCallOptions, ToolExposure, ToolResult, ToolRunContext};

use crate::{RegistrationPlan, ToolAdmission, ToolRegistration, register_admitted};

/// One shared, non-cloneable set of tools.
pub type SharedToolSet = Arc<Vec<Box<dyn Tool>>>;

/// A boxed tool execution.
pub type ToolExecution<'a> = Pin<Box<dyn Future<Output = anyhow::Result<ToolResult>> + Send + 'a>>;

/// Wraps every tool execution in a host's ambient scope (a task-local, a
/// tracing span, an origin stamp) without the runtime knowing what it is.
pub trait ToolExecScope: Send + Sync {
    /// Runs `execution` inside the scope. `context` is the call's run context.
    fn scope<'a>(
        &'a self,
        context: Option<&'a dyn ToolRunContext>,
        execution: ToolExecution<'a>,
    ) -> ToolExecution<'a>;
}

/// Chooses a typed dispatch for tools that need the live parent run. Receives
/// the tool's name and its registered adapter; `None` registers a plain tool.
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
    /// An empty builder (admits nothing until a set is added).
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

    /// Forces the sub-agent admission rule on.
    pub(crate) fn mark_subagent(&mut self) {
        self.admission.is_subagent_run = true;
    }

    /// Registers the admitted tools onto `harness`.
    pub fn register(&self, harness: &mut AgentHarness<State, C>) -> ToolSetReport {
        if self.admission.allowed.as_ref().is_some_and(HashSet::is_empty) {
            tracing::warn!(
                subagent = self.admission.is_subagent_run,
                "[tool-set] allowlist resolved empty: registering no tools"
            );
        }
        let names = self.candidate_names();
        let mut registered = Vec::new();
        let (_, plan) = register_admitted(harness, names, &self.admission, |name| {
            let adapter = SharedToolAdapter::for_name(self.sets.clone(), name)?
                .deferred(self.deferred.contains(name))
                .scoped(self.scope.clone());
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

/// Resolves a tool from the shared sets on every call and forwards the full
/// `tinytools::Tool` contract to it.
pub struct SharedToolAdapter {
    sets: Vec<SharedToolSet>,
    name: String,
    description: String,
    parameters_schema: serde_json::Value,
    force_deferred: bool,
    scope: Option<Arc<dyn ToolExecScope>>,
}

impl SharedToolAdapter {
    /// An adapter for the first tool called `name`, or `None` when absent.
    pub fn for_name(sets: Vec<SharedToolSet>, name: &str) -> Option<Self> {
        let spec = sets
            .iter()
            .flat_map(|set| set.iter())
            .find(|tool| tool.name() == name)
            .map(|tool| tool.spec())?;
        Some(Self {
            sets,
            name: spec.name,
            description: spec.description,
            parameters_schema: spec.parameters,
            force_deferred: false,
            scope: None,
        })
    }

    /// Reports `Deferred` for a directly exposed tool.
    #[must_use]
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.force_deferred = deferred;
        self
    }

    /// Runs executions inside `scope`.
    #[must_use]
    pub fn scoped(mut self, scope: Option<Arc<dyn ToolExecScope>>) -> Self {
        self.scope = scope;
        self
    }

    fn resolved(&self) -> Option<&dyn Tool> {
        self.sets
            .iter()
            .flat_map(|set| set.iter())
            .find(|tool| tool.name() == self.name)
            .map(|tool| tool.as_ref())
    }
}

#[async_trait]
impl Tool for SharedToolAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> serde_json::Value {
        self.parameters_schema.clone()
    }

    fn policy(&self) -> tinytools::ToolPolicy {
        self.resolved().map(Tool::policy).unwrap_or_default()
    }

    /// `Deferred` is forwarded so the harness indexes it behind its search
    /// bridge. `Hidden` is not: the host owns exposure, and a hidden tool that
    /// reached registration was named on purpose.
    fn exposure(&self) -> ToolExposure {
        match self.resolved().map(Tool::exposure) {
            Some(ToolExposure::Deferred) => ToolExposure::Deferred,
            Some(ToolExposure::Hidden) if self.force_deferred => ToolExposure::Hidden,
            Some(ToolExposure::Direct) if self.force_deferred => ToolExposure::Deferred,
            _ => ToolExposure::Direct,
        }
    }

    fn family(&self) -> Option<&str> {
        self.resolved().and_then(Tool::family)
    }

    async fn prepare_authorization(
        &self,
        arguments: &serde_json::Value,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        match self.resolved() {
            Some(tool) => tool.prepare_authorization(arguments).await,
            None => Ok(None),
        }
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        self.execute_with_context(
            args,
            ToolCallOptions {
                prefer_markdown: true,
            },
            None,
        )
        .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        options: ToolCallOptions,
        context: Option<&dyn ToolRunContext>,
    ) -> anyhow::Result<ToolResult> {
        let Some(tool) = self.resolved() else {
            tracing::warn!(tool = %self.name, "[tool-set] shared tool not found");
            return Ok(ToolResult::error(format!("unknown tool '{}'", self.name)));
        };
        let execution: ToolExecution<'_> =
            Box::pin(tool.execute_with_context(args, options, context));
        let execution = match &self.scope {
            Some(scope) => scope.scope(context, execution),
            None => execution,
        };
        // An operational failure is input to the loop, not a harness failure:
        // keep it as an error result so the model can recover.
        Ok(match execution.await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(tool = %self.name, %error, "[tool-set] shared tool failed");
                ToolResult::error(format!("{} failed: {error}", self.name))
            }
        })
    }
}

#[cfg(test)]
#[path = "tool_set_tests.rs"]
mod tests;
