//! One workflow child invocation, run as a [`SubagentDriver`](crate::subagent::SubagentDriver)
//! lifecycle so workflow agent phases share the spawn-admission, timeout /
//! retry / budget, result-policy and typed-outcome behaviour of every other
//! subagent path.

use std::sync::Arc;

use serde_json::Value;
use tinyagents_harness::CancellationToken;

use super::engine::{
    OrchestrationError, WorkflowChildRegistration, WorkflowChildRequest, WorkflowChildResult,
    WorkflowExecutor, render_compat_output,
};
use super::persistence::{
    AsyncWorkflowChildRegistration, AsyncWorkflowExecutor, WorkflowPersistenceError,
    WorkflowPersistenceResult,
};
use crate::subagent::{
    AgentStepConfig, AgentStepIdentity, IncompleteKind, ResultPolicy, StepSuccess, SubagentOutcome,
    SubagentOutcomeKind, run_agent_step,
};

/// Remembers the child ids one step registered so a timed-out child can be
/// cancelled by id (the engine only cancels on interruption or lease loss).
struct RecordingRegistration {
    inner: Arc<dyn WorkflowChildRegistration>,
    async_inner: Option<Arc<dyn AsyncWorkflowChildRegistration>>,
    ids: parking_lot::Mutex<Vec<String>>,
}

impl WorkflowChildRegistration for RecordingRegistration {
    fn register(&self, child_id: String) -> Result<(), OrchestrationError> {
        self.ids.lock().push(child_id.clone());
        self.inner.register(child_id)
    }
}

#[async_trait::async_trait]
impl AsyncWorkflowChildRegistration for RecordingRegistration {
    fn register(&self, child_id: String) -> WorkflowPersistenceResult<()> {
        self.ids.lock().push(child_id.clone());
        match &self.async_inner {
            Some(inner) => inner.register(child_id),
            None => Ok(self.inner.register(child_id)?),
        }
    }

    async fn register_async(&self, child_id: String) -> WorkflowPersistenceResult<()> {
        self.ids.lock().push(child_id.clone());
        match &self.async_inner {
            Some(inner) => inner.register_async(child_id).await,
            None => Ok(self.inner.register(child_id)?),
        }
    }
}

const LOG_PREFIX: &str = "[workflow-child-step]";

#[cfg(test)]
pub(super) async fn run_child_step<E: WorkflowExecutor + 'static>(
    config: &AgentStepConfig,
    executor: Arc<E>,
    request: WorkflowChildRequest,
    cancel: CancellationToken,
    registration: Arc<dyn WorkflowChildRegistration>,
) -> Result<(WorkflowChildResult, Option<Value>), OrchestrationError> {
    run_child_step_authoritative(config, executor, None, request, cancel, registration, None)
        .await
        .map_err(WorkflowPersistenceError::into_legacy)
}

pub(super) async fn run_child_step_authoritative<E: WorkflowExecutor + 'static>(
    config: &AgentStepConfig,
    executor: Arc<E>,
    async_executor: Option<Arc<dyn AsyncWorkflowExecutor>>,
    request: WorkflowChildRequest,
    cancel: CancellationToken,
    registration: Arc<dyn WorkflowChildRegistration>,
    async_registration: Option<Arc<dyn AsyncWorkflowChildRegistration>>,
) -> WorkflowPersistenceResult<(WorkflowChildResult, Option<Value>)> {
    let identity = AgentStepIdentity::new(
        request.run_id.clone(),
        format!("{}:{}", request.phase, request.index_in_phase),
    )
    .with_target(request.agent_id.clone());
    tracing::debug!(
        "{LOG_PREFIX} start run={} phase={} index={} agent={}",
        request.run_id,
        request.phase,
        request.index_in_phase,
        request.agent_id
    );
    let work_request = request.clone();
    // The driver validates a schema against the output *text* only, which
    // mis-reads a JSON string. Validate the structured value here instead and
    // keep the schema out of the driver's policy so truncation and artifact
    // storage keep seeing the display text.
    let schema = config.result_policy.schema.clone();
    let mut step_config = config.clone();
    step_config.result_policy.schema = None;
    let config = &step_config;
    let registration = Arc::new(RecordingRegistration {
        inner: registration,
        async_inner: async_registration,
        ids: parking_lot::Mutex::new(Vec::new()),
    });
    let recorded = registration.clone();
    let cancel_executor = executor.clone();
    let cancel_async_executor = async_executor.clone();
    // The generic step outcome carries textual reasons. Keep the exact
    // executor source across that projection for the authoritative caller.
    let typed_failure = Arc::new(parking_lot::Mutex::new(None));
    let worker_failure = typed_failure.clone();
    // `cancel` is the lifecycle token: cancelling the run reaches the child,
    // whose own token the driver derives from it.
    let result = run_agent_step(config, identity, cancel, move |ctx| {
        let executor = executor.clone();
        let async_executor = async_executor.clone();
        let typed_failure = worker_failure.clone();
        let request = work_request.clone();
        let registration = registration.clone();
        async move {
            let result = match async_executor {
                Some(executor) => {
                    executor
                        .execute_async(request, ctx.cancellation, registration)
                        .await
                }
                None => executor
                    .execute(request, ctx.cancellation, registration)
                    .await
                    .map_err(WorkflowPersistenceError::from),
            }
            .map_err(|error| {
                let message = error.to_string();
                *typed_failure.lock() = Some(error);
                anyhow::anyhow!(message)
            })?;
            let rendered = render_compat_output(&result.output);
            Ok(StepSuccess::new(rendered, result))
        }
    })
    .await;
    if let Some(error) = typed_failure.lock().take() {
        return Err(error);
    }
    match result {
        Ok(step) => match step.outcome.status {
            SubagentOutcomeKind::Completed => {
                let mut child = step.value.ok_or_else(|| {
                    OrchestrationError("workflow child completed without a result".to_owned())
                })?;
                let original = child.output.clone();
                // Keep the executor's structured output unless the result
                // policy actually changed the text.
                if step.outcome.output != render_compat_output(&child.output) {
                    child.output = Value::String(step.outcome.output.clone());
                }
                let schema_error = match &schema {
                    Some(schema) => {
                        // Validate the executor's structured value, not the
                        // possibly trimmed display text.
                        ResultPolicy::new()
                            .with_schema(schema.clone())
                            .apply(&step.outcome.task_id, &step.outcome.output, Some(&original))
                            .await
                            .schema_error
                    }
                    None => None,
                };
                Ok((
                    child,
                    result_policy_annotations(&step.outcome, schema_error),
                ))
            }
            SubagentOutcomeKind::Incomplete(incomplete) => {
                tracing::debug!(
                    "{LOG_PREFIX} incomplete run={} phase={} agent={} kind={:?}",
                    request.run_id,
                    request.phase,
                    request.agent_id,
                    incomplete.kind
                );
                if incomplete.kind == IncompleteKind::Timeout {
                    // The driver dropped the executor future; the host's real
                    // child may still be running, so cancel what it registered.
                    let ids = recorded.ids.lock().clone();
                    if !ids.is_empty() {
                        match &cancel_async_executor {
                            Some(executor) => executor.cancel_children(&ids).await,
                            None => cancel_executor.cancel_children(&ids).await,
                        }
                    }
                }
                Err(OrchestrationError(incomplete.reason).into())
            }
            // The child registered its id before it could be cancelled, so the
            // engine keeps it (via the registration) without a result.
            SubagentOutcomeKind::Cancelled => {
                Err(OrchestrationError("workflow child cancelled".to_owned()).into())
            }
            SubagentOutcomeKind::AwaitingInput(_) => {
                Err(OrchestrationError("workflow child paused for input".to_owned()).into())
            }
        },
        Err(error) => {
            tracing::debug!(
                "{LOG_PREFIX} failed run={} phase={} agent={} error={error}",
                request.run_id,
                request.phase,
                request.agent_id
            );
            Err(OrchestrationError(error.to_string()).into())
        }
    }
}

/// Result-policy findings (schema check, artifact overflow) the driver
/// recorded on the outcome, for the phase output metadata. `None` when the
/// policy had nothing to report.
fn result_policy_annotations(
    outcome: &SubagentOutcome,
    schema_error: Option<String>,
) -> Option<Value> {
    if schema_error.is_none() && outcome.artifact_error.is_none() && outcome.artifacts.is_empty() {
        return None;
    }
    Some(serde_json::json!({
        "schemaError": schema_error,
        "artifactError": outcome.artifact_error,
        "artifacts": outcome.artifacts,
    }))
}

#[cfg(test)]
#[path = "child_step_tests.rs"]
mod tests;
