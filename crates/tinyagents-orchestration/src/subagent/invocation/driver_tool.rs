//! `SubAgentTool` adapter for the host-planned [`SubagentDriver`] lifecycle.
//!
//! The request factory is host supplied: it owns authorization, child context
//! construction, durable task identity, and opaque planner options. The driver
//! then remains the only path that plans, executes, and persists the run.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyagents_harness::context::RunContext;
use tinyagents_harness::ids::CallId;
use tinyagents_harness::tool::ToolDispatch;
use tinytools::{Tool, ToolResult};

use super::jobs::InlineJobGuard;
use super::types::DriverJobRegistration;
use crate::subagent::{
    SubAgentJobRegistry, SubagentDriver, SubagentError, SubagentOutcomeKind, SubagentRequest,
};

fn status_tool_result(
    task_id: String,
    status: &str,
    output: Option<String>,
    error: Option<String>,
) -> ToolResult {
    let mut value = json!({ "task_id": task_id, "status": status });
    if let Some(output) = output {
        value["output"] = Value::String(output);
    }
    if let Some(error) = error {
        value["error"] = Value::String(error);
    }
    if matches!(status, "completed" | "running" | "queued") {
        ToolResult::json(value)
    } else {
        ToolResult::error(value.to_string())
    }
}

type RequestFactory<State, Ctx, H> = dyn Fn(&State, &RunContext<Ctx>, &Value, &CallId) -> Result<SubagentRequest<Ctx, H>, SubagentError>
    + Send
    + Sync;
type DeliveryTarget<Ctx, H> = dyn Fn(&SubagentRequest<Ctx, H>) -> Option<String> + Send + Sync;
type CompletionCallback =
    dyn Fn(&str, Result<crate::subagent::SubagentOutcome, String>) + Send + Sync;

/// A typed-parent dispatcher that runs through [`SubagentDriver`].
///
/// Hosts construct the validated request with `request_factory`; this keeps
/// authorization and execution policy in the host planner/executor. A call
/// requesting background mode is detached only when `delivery_target` returns
/// a target. Otherwise the child runs inline and the result is returned to the
/// caller.
pub struct DriverSubAgentTool<State, Ctx: Send + 'static, H: Send + 'static> {
    name: String,
    description: String,
    parameters: Value,
    driver: Arc<SubagentDriver<Ctx, H>>,
    request_factory: Arc<RequestFactory<State, Ctx, H>>,
    delivery_target: Arc<DeliveryTarget<Ctx, H>>,
    completion: Option<Arc<CompletionCallback>>,
    jobs: Option<SubAgentJobRegistry>,
    _state: PhantomData<fn(&State)>,
}

impl<State, Ctx: Send + 'static, H: Send + 'static> DriverSubAgentTool<State, Ctx, H> {
    /// Creates a driver-backed dispatcher with a host request factory and a
    /// delivery-target resolver.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        driver: Arc<SubagentDriver<Ctx, H>>,
        request_factory: impl Fn(
            &State,
            &RunContext<Ctx>,
            &Value,
            &CallId,
        ) -> std::result::Result<SubagentRequest<Ctx, H>, SubagentError>
        + Send
        + Sync
        + 'static,
        delivery_target: impl Fn(&SubagentRequest<Ctx, H>) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters: json!({
                "type": "object",
                "required": ["input"],
                "properties": {
                    "input": { "type": "string" },
                    "mode": {
                        "type": "string",
                        "enum": ["background", "inline"],
                        "description": "Background returns a task id when a delivery target exists; otherwise it runs inline."
                    }
                }
            }),
            driver,
            request_factory: Arc::new(request_factory),
            delivery_target: Arc::new(delivery_target),
            completion: None,
            jobs: None,
            _state: PhantomData,
        }
    }

    /// Creates the dispatcher from an object implementing the host callback
    /// surface. This is useful when request construction and delivery routing
    /// already live in one host adapter.
    pub fn from_host(
        name: impl Into<String>,
        description: impl Into<String>,
        driver: Arc<SubagentDriver<Ctx, H>>,
        host: Arc<dyn SubagentDriverToolHost<State, Ctx, H>>,
    ) -> Self
    where
        State: 'static,
        Ctx: 'static,
        H: 'static,
    {
        let delivery_host = Arc::clone(&host);
        Self::new(
            name,
            description,
            driver,
            move |state, parent, args, call_id| host.request(state, parent, args, call_id),
            move |request| delivery_host.delivery_target(request),
        )
    }

    /// Replaces the model-visible argument schema.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = parameters;
        self
    }

    /// Bridges this driver-backed alias into the shared Jobs/Message registry.
    /// The registry ID is scoped to the full durable task key, so a repeated
    /// coalesced request reuses its controls and another parent cannot replace
    /// its cancellation or queues.
    pub fn with_job_registry(mut self, jobs: SubAgentJobRegistry) -> Self {
        self.jobs = Some(jobs);
        self
    }

    /// Installs the host callback that publishes a detached result to its target.
    /// The callback is invoked for successful and failed runs; hosts can persist
    /// completion state or route a notification without moving policy into this crate.
    pub fn with_completion_handler(
        mut self,
        handler: impl Fn(&str, Result<crate::subagent::SubagentOutcome, String>) + Send + Sync + 'static,
    ) -> Self {
        self.completion = Some(Arc::new(handler));
        self
    }

    fn requested_mode(args: &Value) -> Result<crate::subagent::SubAgentMode, String> {
        match args.get("mode") {
            None | Some(Value::Null) => Ok(crate::subagent::SubAgentMode::Background),
            Some(Value::String(mode)) if mode == "background" => {
                Ok(crate::subagent::SubAgentMode::Background)
            }
            Some(Value::String(mode)) if mode == "inline" => {
                Ok(crate::subagent::SubAgentMode::Inline)
            }
            Some(_) => Err("`mode` must be \"background\" or \"inline\"".into()),
        }
    }

    async fn execute_request(
        &self,
        mut request: SubagentRequest<Ctx, H>,
        target: Option<String>,
        owner: u64,
    ) -> anyhow::Result<ToolResult>
    where
        Ctx: Send + 'static,
        H: Send + 'static,
    {
        let task_id = request.task_id().to_owned();
        let bridged_job = if let Some(jobs) = &self.jobs {
            let continuing = request.resume().is_some();
            let (id, steering, run_queue, cancellation, created) =
                jobs.register_driver_job(DriverJobRegistration {
                    key: request.task_key().clone(),
                    agent: request.target().unwrap_or(&self.name).to_owned(),
                    owner,
                    cancellation: request.run_context().cancellation.clone(),
                    steering: request.run_context().steering.clone(),
                    run_queue: request.run_context().run_queue.clone(),
                    continuing,
                })?;
            if created {
                jobs.mark_running(&id);
            }
            request = request.with_job_controls(steering, run_queue, cancellation);
            Some((jobs.clone(), id, created))
        } else {
            None
        };
        let public_task_id = bridged_job
            .as_ref()
            .map(|(_, id, _)| id.as_str().to_owned())
            .unwrap_or_else(|| task_id.clone());
        let job_guard = bridged_job
            .as_ref()
            .filter(|(_, _, created)| *created)
            .map(|(jobs, id, _)| InlineJobGuard::new(jobs.clone(), id.clone()));
        if let Some((jobs, id, false)) = &bridged_job {
            let job = if target.is_some() {
                jobs.get(id.as_str())
                    .ok_or_else(|| anyhow::anyhow!("registered subagent job disappeared"))?
            } else {
                jobs.wait_owned(id.as_str(), owner).await?
            };
            let status = match job.status {
                crate::subagent::SubAgentJobStatus::Queued => "queued",
                crate::subagent::SubAgentJobStatus::Running => "running",
                crate::subagent::SubAgentJobStatus::AwaitingInput => "awaiting_input",
                crate::subagent::SubAgentJobStatus::Completed => "completed",
                crate::subagent::SubAgentJobStatus::Failed => "failed",
                crate::subagent::SubAgentJobStatus::Incomplete => "incomplete",
                crate::subagent::SubAgentJobStatus::Cancelled => "cancelled",
            };
            return Ok(status_tool_result(
                public_task_id,
                status,
                job.output,
                job.error,
            ));
        }
        if let Some(target) = target {
            let cancellation = request.run_context().cancellation.clone();
            let driver = Arc::clone(&self.driver);
            let completion = self.completion.clone();
            let detached_task_id = task_id.clone();
            let completion_target = target.clone();
            let bridge = bridged_job.clone();
            let mut job_guard = job_guard;
            tokio::spawn(async move {
                match driver.run(request, cancellation).await {
                    Ok(result) => {
                        if let Some((jobs, id, _)) = &bridge {
                            jobs.mark_driver_result(id, Ok(result.outcome.clone()));
                        }
                        if let Some(guard) = &mut job_guard {
                            guard.disarm();
                        }
                        if result.should_emit_host_effects()
                            && let Some(completion) = completion
                        {
                            completion(&completion_target, Ok(result.outcome));
                        }
                    }
                    Err(error) => {
                        let error = error.to_string();
                        if let Some((jobs, id, _)) = &bridge {
                            jobs.mark_driver_result(id, Err(error.clone()));
                        }
                        if let Some(guard) = &mut job_guard {
                            guard.disarm();
                        }
                        if let Some(completion) = completion {
                            completion(&completion_target, Err(error.clone()));
                        }
                        tracing::error!(task_id = %detached_task_id, error, "driver-backed subagent failed");
                    }
                }
            });
            return Ok(ToolResult::json(json!({
                "task_id": public_task_id,
                "status": "running",
                "delivery_target": target,
            })));
        }

        let cancellation = request.run_context().cancellation.clone();
        let mut job_guard = job_guard;
        match self.driver.run(request, cancellation).await {
            Ok(result) => {
                let outcome = result.outcome;
                if let Some((jobs, id, _)) = &bridged_job {
                    jobs.mark_driver_result(id, Ok(outcome.clone()));
                }
                if let Some(guard) = &mut job_guard {
                    guard.disarm();
                }
                let (status, error) = match &outcome.status {
                    SubagentOutcomeKind::Completed => ("completed", None),
                    SubagentOutcomeKind::AwaitingInput(_) => ("awaiting_input", None),
                    SubagentOutcomeKind::Incomplete(incomplete) => {
                        ("incomplete", Some(incomplete.reason.clone()))
                    }
                    SubagentOutcomeKind::Cancelled => ("cancelled", Some("cancelled".into())),
                };
                Ok(status_tool_result(
                    public_task_id,
                    status,
                    Some(outcome.output),
                    error,
                ))
            }
            Err(error) => {
                if let Some((jobs, id, _)) = &bridged_job {
                    jobs.mark_driver_result(id, Err(error.to_string()));
                }
                if let Some(guard) = &mut job_guard {
                    guard.disarm();
                }
                Ok(ToolResult::error(format!("subagent driver: {error}")))
            }
        }
    }
}

#[async_trait]
impl<State, Ctx, H> ToolDispatch<State, Ctx> for DriverSubAgentTool<State, Ctx, H>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    H: Send + Sync + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(DriverSubAgentToolDeclaration {
            name: self.name.clone(),
            description: self.description.clone(),
            parameters: self.parameters.clone(),
        })
    }

    fn output_origin(&self) -> tinyagents_harness::host::ContentOrigin {
        tinyagents_harness::host::ContentOrigin::Agent
    }

    async fn execute(
        &self,
        state: &State,
        call_id: CallId,
        arguments: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let requested = match Self::requested_mode(&arguments) {
            Ok(mode) => mode,
            Err(error) => return Ok(ToolResult::error(error)),
        };
        let request = match (self.request_factory)(state, parent, &arguments, &call_id) {
            Ok(request) => request,
            Err(error) => return Ok(ToolResult::error(format!("subagent request: {error}"))),
        };
        let target = if requested == crate::subagent::SubAgentMode::Background {
            (self.delivery_target)(&request)
        } else {
            None
        };
        self.execute_request(request, target, parent.instance_id())
            .await
    }
}

struct DriverSubAgentToolDeclaration {
    name: String,
    description: String,
    parameters: Value,
}

#[async_trait]
impl Tool for DriverSubAgentToolDeclaration {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
    fn parameters_schema(&self) -> Value {
        self.parameters.clone()
    }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        anyhow::bail!("driver-backed subagent tools require typed-parent dispatch")
    }
}

/// The host callback surface for building a driver request and resolving its
/// delivery target. A closure-backed constructor is exposed directly on
/// [`DriverSubAgentTool::new`]; this trait name is retained for discoverability
/// in API documentation.
pub trait SubagentDriverToolHost<State, Ctx, H>: Send + Sync {
    /// Builds the validated lifecycle request for one tool call.
    fn request(
        &self,
        state: &State,
        parent: &RunContext<Ctx>,
        args: &Value,
        call_id: &CallId,
    ) -> std::result::Result<SubagentRequest<Ctx, H>, SubagentError>;

    /// Returns a delivery destination when background execution is supported.
    fn delivery_target(&self, request: &SubagentRequest<Ctx, H>) -> Option<String>;
}
