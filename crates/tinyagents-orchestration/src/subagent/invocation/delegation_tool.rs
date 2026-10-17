//! Model-facing multi-target delegation adapter.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinyagents_graph::delegation::{
    DelegationConfig, DelegationOutcome, DelegationStage, DelegationStageOutput, DelegationState,
    run_delegation, run_or_resume_delegation,
};
use tinyagents_harness::{context::RunContext, ids::CallId, tool::ToolDispatch};
use tinytools::{
    PermissionLevel, Tool, ToolCategory, ToolExposure, ToolResult, ToolScope, ToolTimeout,
};

use crate::subagent::SUBAGENT_MODE_FIELD;

/// A task envelope passed to the selected target agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationEnvelope {
    /// The requested work.
    pub prompt: String,
    /// Optional objective statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    /// Relevant evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    /// Constraints the target must follow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<String>,
    /// Claims the target must not assume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub must_not_assume: Option<String>,
    /// Expected response shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_output: Option<String>,
    /// Citation expectations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub citation_requirement: Option<String>,
    /// Optional model override, interpreted by the host callback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Whether the caller waits for completion; maps to [`SUBAGENT_MODE_FIELD`].
    #[serde(default)]
    pub blocking: bool,
}

/// Parsed target and task passed to a host request factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationRequest {
    /// The selected agent id.
    pub agent: String,
    /// Structured task instructions.
    pub envelope: DelegationEnvelope,
    /// Driver mode selected from `blocking`.
    pub mode: String,
}

/// Driver-backed multi-target tool whose schema and execution remain host owned.
pub struct MultiTargetDelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationRequest) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    name: String,
    description: String,
    agents: Vec<(String, String)>,
    worker: Arc<Worker>,
    _context: PhantomData<fn(&State, &Ctx)>,
    exposure: ToolExposure,
}

impl<State, Ctx, Worker, Fut> MultiTargetDelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationRequest) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    /// Creates a multi-target delegation tool. `agents` entries are `(id, description)`.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        agents: Vec<(String, String)>,
        worker: Worker,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            agents,
            worker: Arc::new(worker),
            _context: PhantomData,
            exposure: ToolExposure::Direct,
        }
    }

    /// Sets whether the tool is directly advertised, deferred, or host-only.
    pub fn with_exposure(mut self, exposure: ToolExposure) -> Self {
        self.exposure = exposure;
        self
    }
}

#[async_trait]
impl<State, Ctx, Worker, Fut> ToolDispatch<State, Ctx>
    for MultiTargetDelegationTool<State, Ctx, Worker, Fut>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationRequest) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(DelegationDeclaration {
            name: self.name.clone(),
            description: self.description.clone(),
            parameters: json!({"type":"object","required":["agent","prompt"],"properties":{
                "agent":{"type":"string","enum":self.agents.iter().map(|(id,_)|id).collect::<Vec<_>>()},
                "prompt":{"type":"string"},"objective":{"type":"string"},"evidence":{"type":"string"},
                "constraints":{"type":"string"},"must_not_assume":{"type":"string"},
                "expected_output":{"type":"string"},"citation_requirement":{"type":"string"},
                "model":{"type":"string"},"blocking":{"type":"boolean","description":"Wait for the target result when true."}
            }}),
            metadata: None,
            exposure: self.exposure,
        })
    }

    async fn execute(
        &self,
        _state: &State,
        _call_id: CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        _parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        let agent = args
            .get("agent")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("agent must be a string"))?;
        if !self.agents.iter().any(|(id, _)| id == agent) {
            anyhow::bail!("agent is not an available delegation target");
        }
        let envelope: DelegationEnvelope = serde_json::from_value(args.clone())?;
        let request = DelegationRequest {
            agent: agent.into(),
            mode: if envelope.blocking {
                "inline"
            } else {
                "background"
            }
            .into(),
            envelope,
        };
        let result = (self.worker)(request.clone())
            .await
            .map_err(|error| anyhow::anyhow!("delegation failed: {error}"))?;
        Ok(ToolResult::json(
            json!({"agent":agent,"mode":request.mode,"output":result.text}),
        ))
    }
}

struct DelegationDeclaration {
    name: String,
    description: String,
    parameters: Value,
    metadata: Option<DelegationToolMetadata>,
    exposure: ToolExposure,
}
#[async_trait]
impl Tool for DelegationDeclaration {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
    fn parameters_schema(&self) -> Value {
        self.parameters.clone()
    }
    fn exposure(&self) -> ToolExposure {
        self.exposure
    }
    fn permission_level(&self) -> PermissionLevel {
        self.metadata
            .map_or(PermissionLevel::ReadOnly, |metadata| metadata.permission)
    }
    fn scope(&self) -> ToolScope {
        self.metadata
            .map_or(ToolScope::All, |metadata| metadata.scope)
    }
    fn category(&self) -> ToolCategory {
        self.metadata
            .map_or(ToolCategory::System, |metadata| metadata.category)
    }
    fn timeout_policy(&self, _args: &Value) -> ToolTimeout {
        self.metadata
            .map_or(ToolTimeout::Inherit, |metadata| metadata.timeout)
    }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        anyhow::bail!("delegation requires typed-parent dispatch")
    }
}

/// Parses a reviewer approval exactly when the whole normalized response is `APPROVE`.
pub fn parse_approval(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("APPROVE")
}

/// Tool argument field used to map blocking mode into the subagent invocation mode.
pub const DELEGATION_MODE_FIELD: &str = SUBAGENT_MODE_FIELD;

/// Generic prompt guidance for a graph delegation planning worker.
pub const DELEGATION_PLAN_PROMPT: &str = "Create a concise, verifiable plan for the requested task. Preserve its constraints and expected output.";
/// Generic prompt guidance for a graph delegation execution worker.
pub const DELEGATION_EXECUTE_PROMPT: &str = "Execute the current plan. Report concrete changes and evidence, and do not claim work that was not performed.";
/// Generic prompt guidance for a graph delegation review worker.
pub const DELEGATION_REVIEW_PROMPT: &str = "Review the latest execution against the request and plan. Reply with exactly APPROVE only when it is complete; otherwise explain the needed revision.";

/// Creates a review-stage result using the canonical standalone APPROVE parser.
pub fn review_stage_output(text: impl Into<String>) -> DelegationStageOutput {
    let text = text.into();
    let approved = parse_approval(&text);
    DelegationStageOutput {
        text,
        approved,
        prompt: None,
    }
}

type ConfigFactory<State, Ctx> =
    dyn Fn(&State, &RunContext<Ctx>, &Value) -> DelegationConfig + Send + Sync;
type ResultProjector = dyn Fn(Result<DelegationOutcome, String>) -> ToolResult + Send + Sync;

#[derive(Debug, Clone, Copy)]
struct DelegationToolMetadata {
    permission: PermissionLevel,
    scope: ToolScope,
    category: ToolCategory,
    timeout: ToolTimeout,
}

/// Determines how review-stage approval is interpreted by [`DelegationTool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReviewApprovalPolicy {
    /// Require a standalone `APPROVE` response (the generic default).
    #[default]
    StandaloneApprove,
    /// Trust the approval boolean returned by the host worker.
    WorkerOutput,
}

/// Single-target graph delegation tool with an injected worker for each stage.
pub struct DelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(Value, DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    config: Arc<ConfigFactory<State, Ctx>>,
    worker: Arc<Worker>,
    name: String,
    description: String,
    parameters: Value,
    resumable: bool,
    review_approval_policy: ReviewApprovalPolicy,
    result_projector: Option<Arc<ResultProjector>>,
    metadata: Option<DelegationToolMetadata>,
}

impl<State, Ctx, Worker, Fut> DelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(Value, DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    /// Constructs the tool from a host config factory and stage worker.
    pub fn new(
        config: impl Fn(&State, &RunContext<Ctx>, &Value) -> DelegationConfig + Send + Sync + 'static,
        worker: Worker,
    ) -> Self {
        Self {
            config: Arc::new(config),
            worker: Arc::new(worker),
            name: "delegate_graph".into(),
            description: "Plan, execute, and review delegated work.".into(),
            parameters: json!({"type":"object","properties":{"task":{"type":"string"}},"required":["task"]}),
            resumable: false,
            review_approval_policy: ReviewApprovalPolicy::StandaloneApprove,
            result_projector: None,
            metadata: None,
        }
    }

    /// Overrides the model-facing name while keeping graph execution generic.
    pub fn with_tool_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Overrides the model-facing description for a host-owned alias.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Supplies a host-owned schema, including dynamic target enums.
    pub fn with_parameters(mut self, parameters: Value) -> Self {
        self.parameters = parameters;
        self
    }

    /// Enables durable classify-and-resume execution for checkpointed threads.
    pub fn with_resumable_execution(mut self) -> Self {
        self.resumable = true;
        self
    }

    /// Selects how the tool interprets the review worker's approval result.
    pub fn with_review_approval_policy(mut self, policy: ReviewApprovalPolicy) -> Self {
        self.review_approval_policy = policy;
        self
    }

    /// Projects the complete graph outcome or raw graph error into a tool result.
    pub fn with_result_projection(
        mut self,
        project: impl Fn(Result<DelegationOutcome, String>) -> ToolResult + Send + Sync + 'static,
    ) -> Self {
        self.result_projector = Some(Arc::new(project));
        self
    }

    /// Preserves host authorization and execution metadata on the declaration.
    pub fn with_metadata(
        mut self,
        permission: PermissionLevel,
        scope: ToolScope,
        category: ToolCategory,
        timeout: ToolTimeout,
    ) -> Self {
        self.metadata = Some(DelegationToolMetadata {
            permission,
            scope,
            category,
            timeout,
        });
        self
    }
}

#[async_trait]
impl<State, Ctx, Worker, Fut> ToolDispatch<State, Ctx> for DelegationTool<State, Ctx, Worker, Fut>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    Worker: Fn(Value, DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(DelegationDeclaration {
            name: self.name.clone(),
            description: self.description.clone(),
            parameters: self.parameters.clone(),
            metadata: self.metadata,
            exposure: ToolExposure::Direct,
        })
    }
    async fn execute(
        &self,
        state: &State,
        _call_id: CallId,
        args: Value,
        _options: tinytools::ToolCallOptions,
        parent: &RunContext<Ctx>,
    ) -> anyhow::Result<ToolResult> {
        if args.get("task").and_then(Value::as_str).is_none() {
            anyhow::bail!("task must be a string");
        }
        let config = (self.config)(state, parent, &args);
        let worker = Arc::clone(&self.worker);
        let review_approval_policy = self.review_approval_policy;
        let stage_runner = move |stage, graph| {
            let worker = Arc::clone(&worker);
            let mut stage_args = args.clone();
            stage_args["stage_prompt"] = Value::String(stage_prompt(stage).to_owned());
            async move {
                let mut output = worker(stage_args, stage, graph).await?;
                if stage == DelegationStage::Review
                    && review_approval_policy == ReviewApprovalPolicy::StandaloneApprove
                {
                    output.approved = parse_approval(&output.text);
                }
                Ok(output)
            }
        };
        let outcome = if self.resumable {
            run_or_resume_delegation(config, stage_runner).await
        } else {
            run_delegation(config, stage_runner)
                .await
                .map(|state| DelegationOutcome {
                    state,
                    pending: None,
                })
        };

        if let Some(project) = &self.result_projector {
            return Ok(project(outcome));
        }
        let outcome = outcome.map_err(|error| anyhow::anyhow!("delegation failed: {error}"))?;
        if self.resumable {
            return Ok(ToolResult::json(json!({
                "state": outcome.state,
                "pending": outcome.pending.map(|pending| json!({
                    "interrupt_id": pending.interrupt_id,
                    "node": pending.node,
                    "payload": pending.payload,
                    "thread_id": pending.thread_id,
                })),
            })));
        }
        Ok(ToolResult::json(json!({
            "output": outcome.state.final_output,
            "approved": outcome.state.approved,
            "cancelled": outcome.state.cancelled,
        })))
    }
}

fn stage_prompt(stage: DelegationStage) -> &'static str {
    match stage {
        DelegationStage::Plan => DELEGATION_PLAN_PROMPT,
        DelegationStage::Execute => DELEGATION_EXECUTE_PROMPT,
        DelegationStage::Review => DELEGATION_REVIEW_PROMPT,
    }
}
