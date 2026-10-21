//! One timeout/retry/budget policy for every subagent path.
//!
//! [`SubAgentPolicy`], [`SubAgentBudget`], and [`SubAgentOutput`] live here so
//! orchestration owns the policy used by the async driver. The graph crate
//! temporarily re-exports these types while its deprecated subagent node is
//! removed. [`SubagentDriver`](super::SubagentDriver) applies the policy:
//!
//! - **timeout** cancels the child and ends the run `Incomplete(Timeout)`;
//!   a timed-out attempt is never retried (it may have run for the whole
//!   window and run tools).
//! - **retry** applies only to a failure the retry policy deems retryable
//!   and never after the attempt ran tools, unless
//!   [`SubAgentPolicy::retry_after_tool_calls`] is set.
//! - **budget** call caps tighten the child's `RunConfig` (enforced during the
//!   run); token caps are checked against the reported usage afterwards;
//!   `max_cost` is carried but not enforced (see [`SubAgentBudget`]).

use std::time::Duration;

use serde_json::Value;
use tinyagents_harness::context::RunConfig;
use tinyagents_harness::middleware::BudgetLimits;
use tinyagents_harness::retry::RetryPolicy;
use tinyinference_llm::usage::UsageTotals;

/// Structured output and accounting returned by an agent invocation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubAgentOutput {
    /// The final assistant text.
    pub text: String,
    /// Parsed structured output, when present.
    pub structured: Option<Value>,
    /// Cumulative token usage across model calls.
    pub usage: UsageTotals,
    /// Number of dispatched model calls.
    pub model_calls: usize,
    /// Number of executed tool invocations.
    pub tool_calls: usize,
}

/// Optional work caps for one subagent invocation.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SubAgentBudget {
    /// Maximum model calls (`None` means unbounded).
    pub max_model_calls: Option<usize>,
    /// Maximum tool calls (`None` means unbounded).
    pub max_tool_calls: Option<usize>,
    /// Maximum cumulative input tokens (`None` means unbounded).
    pub max_input_tokens: Option<u64>,
    /// Maximum cumulative output tokens (`None` means unbounded).
    pub max_output_tokens: Option<u64>,
    /// Maximum estimated cost (`None` means unbounded); carried, not enforced
    /// here because enforcement requires a pricing table.
    pub max_cost: Option<f64>,
}

impl SubAgentBudget {
    /// A budget that imposes no cap.
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Caps model calls, returning `self` for chaining.
    pub fn with_max_model_calls(mut self, max: usize) -> Self {
        self.max_model_calls = Some(max);
        self
    }

    /// Caps tool calls, returning `self` for chaining.
    pub fn with_max_tool_calls(mut self, max: usize) -> Self {
        self.max_tool_calls = Some(max);
        self
    }

    /// Caps cumulative input tokens, returning `self` for chaining.
    pub fn with_max_input_tokens(mut self, max: u64) -> Self {
        self.max_input_tokens = Some(max);
        self
    }

    /// Caps cumulative output tokens, returning `self` for chaining.
    pub fn with_max_output_tokens(mut self, max: u64) -> Self {
        self.max_output_tokens = Some(max);
        self
    }

    /// Caps cumulative estimated cost, returning `self` for chaining.
    pub fn with_max_cost(mut self, max: f64) -> Self {
        self.max_cost = Some(max);
        self
    }

    /// Tightens `config`'s call caps without widening existing restrictions.
    pub fn apply_call_caps(&self, config: &mut RunConfig) {
        if let Some(max) = self.max_model_calls {
            config.max_model_calls = Some(config.max_model_calls.map_or(max, |set| set.min(max)));
        }
        if let Some(max) = self.max_tool_calls {
            config.max_tool_calls = Some(config.max_tool_calls.map_or(max, |set| set.min(max)));
        }
    }

    /// Converts token and cost limits to harness middleware limits.
    pub fn to_budget_limits(&self) -> BudgetLimits {
        BudgetLimits {
            max_input_tokens: self.max_input_tokens,
            max_output_tokens: self.max_output_tokens,
            max_cost: self.max_cost,
            ..BudgetLimits::default()
        }
    }

    /// Returns an error when the measured output exceeds a configured cap.
    pub fn check(
        &self,
        output: &SubAgentOutput,
        agent: &str,
    ) -> Result<(), tinyagents_harness::error::TinyAgentsError> {
        if let Some(max) = self.max_model_calls
            && output.model_calls > max
        {
            return Err(tinyagents_harness::error::TinyAgentsError::LimitExceeded(format!(
                "sub-agent `{agent}` exceeded model-call budget: {} > {max}",
                output.model_calls
            )));
        }
        if let Some(max) = self.max_tool_calls
            && output.tool_calls > max
        {
            return Err(tinyagents_harness::error::TinyAgentsError::LimitExceeded(format!(
                "sub-agent `{agent}` exceeded tool-call budget: {} > {max}",
                output.tool_calls
            )));
        }
        if let Some(max) = self.max_input_tokens
            && output.usage.usage.input_tokens > max
        {
            return Err(tinyagents_harness::error::TinyAgentsError::LimitExceeded(format!(
                "sub-agent `{agent}` exceeded input-token budget: {} > {max}",
                output.usage.usage.input_tokens
            )));
        }
        if let Some(max) = self.max_output_tokens
            && output.usage.usage.output_tokens > max
        {
            return Err(tinyagents_harness::error::TinyAgentsError::LimitExceeded(format!(
                "sub-agent `{agent}` exceeded output-token budget: {} > {max}",
                output.usage.usage.output_tokens
            )));
        }
        Ok(())
    }
}

/// Timeout, retry, and budget policy for an async subagent invocation.
#[derive(Clone, Debug)]
pub struct SubAgentPolicy {
    /// Optional wall-clock timeout for each attempt.
    pub timeout: Option<Duration>,
    /// Retry and backoff policy.
    pub retry: RetryPolicy,
    /// Optional work caps.
    pub budget: SubAgentBudget,
    /// Whether a failed attempt may be retried after tools ran.
    pub retry_after_tool_calls: bool,
}

impl Default for SubAgentPolicy {
    fn default() -> Self {
        Self {
            timeout: None,
            retry: RetryPolicy::default().with_max_attempts(1),
            budget: SubAgentBudget::unlimited(),
            retry_after_tool_calls: false,
        }
    }
}

impl SubAgentPolicy {
    /// Sets the per-attempt timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the retry policy.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sets the work budget.
    pub fn with_budget(mut self, budget: SubAgentBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Allows retries after tools have run, for idempotent tool sets.
    pub fn with_retry_after_tool_calls(mut self, allow: bool) -> Self {
        self.retry_after_tool_calls = allow;
        self
    }
}

use tinyagents_harness::CancellationToken;
use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::TinyAgentsError;
use tinyagents_harness::tool::ToolSnapshot;
use tinyinference_llm::message::Message;

use super::{
    AttemptContextFactory, IncompleteKind, PreparedSubagent, ResultPolicy, SubagentError,
    SubagentIncomplete, SubagentOutcome, SubagentOutcomeKind, SubagentRole,
};

/// Whether a failed attempt `attempt` (0-based) may be retried.
pub(crate) fn may_retry(
    policy: &SubAgentPolicy,
    attempt: usize,
    error: &TinyAgentsError,
    tools_ran: bool,
) -> bool {
    !error.is_terminal_limit()
        && policy.retry.should_retry_error(attempt, error)
        && (!tools_ran || policy.retry_after_tool_calls)
}

/// Everything about a plan except its (consumed) run context, kept so a
/// retry can rebuild an identical plan around a freshly minted context.
pub(crate) struct AttemptSource<C> {
    template: Option<Template<C>>,
}

struct Template<C> {
    task_id: String,
    agent_key: String,
    input: Vec<Message>,
    tools: ToolSnapshot,
    role: SubagentRole,
    tool_ceiling: Option<ToolSnapshot>,
    delegation_tools: Vec<String>,
    tool_scope: Option<tinyagents_definition::ToolScope>,
    skill_filter: Option<String>,
    policy: SubAgentPolicy,
    result_policy: ResultPolicy,
    factory: AttemptContextFactory<C>,
    /// Host extensions of the first attempt's context, carried to retries
    /// whose factory-built context installs none of its own.
    host_ext: tinyagents_harness::context::HostExtensions,
}

impl<C> AttemptSource<C> {
    /// Captures the plan, but only when retrying is possible at all.
    pub(crate) fn from_prepared(prepared: &PreparedSubagent<C>) -> Self {
        let template = (prepared.policy.retry.max_attempts > 1)
            .then(|| prepared.retry_context.clone())
            .flatten()
            .map(|factory| Template {
                task_id: prepared.task_id.clone(),
                agent_key: prepared.agent_key.clone(),
                input: prepared.input.clone(),
                tools: prepared.tools.clone(),
                role: prepared.role,
                tool_ceiling: prepared.tool_ceiling.clone(),
                delegation_tools: prepared.delegation_tools.clone(),
                tool_scope: prepared.tool_scope.clone(),
                skill_filter: prepared.skill_filter.clone(),
                policy: prepared.policy.clone(),
                result_policy: prepared.result_policy.clone(),
                factory,
                host_ext: prepared.run_context.host_extensions().clone(),
            });
        Self { template }
    }

    /// Whether a retry can be built (a context factory was supplied).
    pub(crate) fn can_retry(&self) -> bool {
        self.template.is_some()
    }

    /// The plan for retry attempt `attempt` (1 is the first retry), around a
    /// fresh context bound to `cancellation`.
    pub(crate) fn next(
        &mut self,
        attempt: u32,
        cancellation: CancellationToken,
    ) -> Result<PreparedSubagent<C>, SubagentError> {
        let template = self
            .template
            .as_ref()
            .ok_or(SubagentError::MissingCapability("retry context factory"))?;
        let mut context: RunContext<C> = (template.factory)(attempt)?;
        if context.host_extensions().is_empty() {
            context = context.with_host_extensions(template.host_ext.clone());
        }
        template.policy.budget.apply_call_caps(&mut context.config);
        let mut prepared = PreparedSubagent::new(
            template.task_id.clone(),
            template.agent_key.clone(),
            template.input.clone(),
            template.tools.clone(),
            context.with_cancellation(cancellation),
        )
        .with_role(template.role)
        .with_delegation_tools(template.delegation_tools.clone())
        .with_policy(template.policy.clone())
        .with_result_policy(template.result_policy.clone())
        .with_retry_context(template.factory.clone());
        prepared.tool_ceiling = template.tool_ceiling.clone();
        prepared.tool_scope = template.tool_scope.clone();
        prepared.skill_filter = template.skill_filter.clone();
        Ok(prepared)
    }
}

/// Applies the budget and result policies to a finished outcome.
///
/// A completed run over its token budget becomes `Incomplete(BudgetExceeded)`
/// (its output and usage are kept); an over-budget or completed run then has
/// its output trimmed and schema-checked per `result_policy`.
pub(crate) async fn apply_outcome_policies(
    mut outcome: SubagentOutcome,
    policy: &SubAgentPolicy,
    result_policy: &ResultPolicy,
) -> (SubagentOutcome, usize) {
    let mut omitted_chars = 0;
    if !matches!(outcome.status, SubagentOutcomeKind::Completed) {
        return (outcome, omitted_chars);
    }
    let measured = tinyagents_graph::SubAgentOutput {
        usage: outcome.usage,
        ..Default::default()
    };
    if let Err(error) = policy.budget.check(&measured, &outcome.task_id) {
        outcome.status = SubagentOutcomeKind::Incomplete(
            SubagentIncomplete::new(error.to_string()).with_kind(IncompleteKind::BudgetExceeded),
        );
    }
    if result_policy.is_active() {
        let applied = result_policy
            .apply(&outcome.task_id, &outcome.output, None)
            .await;
        omitted_chars = applied.omitted_chars;
        outcome.output = applied.text;
        outcome.schema_error = applied.schema_error;
        outcome.artifact_error = applied.artifact_error;
        outcome.artifacts.extend(applied.artifact);
    }
    (outcome, omitted_chars)
}

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;
