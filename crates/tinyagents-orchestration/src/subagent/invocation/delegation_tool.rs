//! Model-facing multi-target delegation adapter.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinyagents_harness::{context::RunContext, ids::CallId, tool::ToolDispatch};
use tinyagents_graph::delegation::{
    DelegationConfig, DelegationStage, DelegationStageOutput, DelegationState, run_delegation,
};
use tinytools::{Tool, ToolResult};

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
    Worker: Fn(DelegationRequest, DelegationStage, tinyagents_graph::delegation::DelegationState) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    name: String,
    description: String,
    agents: Vec<(String, String)>,
    worker: Arc<Worker>,
}

impl<State, Ctx, Worker, Fut> MultiTargetDelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationRequest, DelegationStage, tinyagents_graph::delegation::DelegationState) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    /// Creates a multi-target delegation tool. `agents` entries are `(id, description)`.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        agents: Vec<(String, String)>,
        worker: Worker,
    ) -> Self {
        Self { name: name.into(), description: description.into(), agents, worker: Arc::new(worker) }
    }
}

#[async_trait]
impl<State, Ctx, Worker, Fut> ToolDispatch<State, Ctx> for MultiTargetDelegationTool<State, Ctx, Worker, Fut>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationRequest, DelegationStage, tinyagents_graph::delegation::DelegationState) -> Fut
        + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(DelegationDeclaration {
            name: self.name.clone(), description: self.description.clone(),
            parameters: json!({"type":"object","required":["agent","prompt"],"properties":{
                "agent":{"type":"string","enum":self.agents.iter().map(|(id,_)|id).collect::<Vec<_>>()},
                "prompt":{"type":"string"},"objective":{"type":"string"},"evidence":{"type":"string"},
                "constraints":{"type":"string"},"must_not_assume":{"type":"string"},
                "expected_output":{"type":"string"},"citation_requirement":{"type":"string"},
                "model":{"type":"string"},"blocking":{"type":"boolean","description":"Wait for the target result when true."}
            }}),
        })
    }

    async fn execute(&self, state: &State, _call_id: CallId, args: Value,
        _options: tinytools::ToolCallOptions, parent: &RunContext<Ctx>) -> anyhow::Result<ToolResult> {
        let agent = args.get("agent").and_then(Value::as_str).ok_or_else(|| anyhow::anyhow!("agent must be a string"))?;
        if !self.agents.iter().any(|(id, _)| id == agent) {
            anyhow::bail!("agent is not an available delegation target");
        }
        let envelope: DelegationEnvelope = serde_json::from_value(args.clone())?;
        let request = DelegationRequest { agent: agent.into(), mode: if envelope.blocking {"inline"} else {"background"}.into(), envelope };
        let result = (self.worker)(request.clone(), DelegationStage::Execute, DelegationState::default())
            .await.map_err(|error| anyhow::anyhow!("delegation failed: {error}"))?;
        Ok(ToolResult::json(json!({"agent":agent,"mode":request.mode,"output":result.text})))
    }
}

struct DelegationDeclaration { name: String, description: String, parameters: Value }
#[async_trait]
impl Tool for DelegationDeclaration {
    fn name(&self) -> &str { &self.name }
    fn description(&self) -> &str { &self.description }
    fn parameters_schema(&self) -> Value { self.parameters.clone() }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> { anyhow::bail!("delegation requires typed-parent dispatch") }
}

/// Parses a reviewer approval exactly when the whole normalized response is `APPROVE`.
pub fn parse_approval(text: &str) -> bool { text.trim().eq_ignore_ascii_case("APPROVE") }

/// Tool argument field used to map blocking mode into the subagent invocation mode.
pub const DELEGATION_MODE_FIELD: &str = SUBAGENT_MODE_FIELD;

/// Single-target graph delegation tool with an injected worker for each stage.
pub struct DelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    config: Arc<dyn Fn(&State, &RunContext<Ctx>, &Value) -> DelegationConfig + Send + Sync>,
    worker: Arc<Worker>,
}

impl<State, Ctx, Worker, Fut> DelegationTool<State, Ctx, Worker, Fut>
where
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    /// Constructs the tool from a host config factory and stage worker.
    pub fn new(
        config: impl Fn(&State, &RunContext<Ctx>, &Value) -> DelegationConfig + Send + Sync + 'static,
        worker: Worker,
    ) -> Self { Self { config: Arc::new(config), worker: Arc::new(worker) } }
}

#[async_trait]
impl<State, Ctx, Worker, Fut> ToolDispatch<State, Ctx> for DelegationTool<State, Ctx, Worker, Fut>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    Worker: Fn(DelegationStage, DelegationState) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<DelegationStageOutput, String>> + Send + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(DelegationDeclaration { name: "delegate_graph".into(), description: "Plan, execute, and review delegated work.".into(), parameters: json!({"type":"object","properties":{"task":{"type":"string"}},"required":["task"]}) })
    }
    async fn execute(&self, state: &State, _call_id: CallId, args: Value, _options: tinytools::ToolCallOptions, parent: &RunContext<Ctx>) -> anyhow::Result<ToolResult> {
        if args.get("task").and_then(Value::as_str).is_none() { anyhow::bail!("task must be a string"); }
        let worker = Arc::clone(&self.worker);
        let result = run_delegation((self.config)(state, parent, &args), move |stage, graph| worker(stage, graph))
            .await.map_err(|error| anyhow::anyhow!("delegation failed: {error}"))?;
        Ok(ToolResult::json(json!({"output":result.final_output,"approved":result.approved,"cancelled":result.cancelled})))
    }
}
