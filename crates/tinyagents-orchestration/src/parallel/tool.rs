//! Host-bound parallel fan-out tool.

use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Value, json};
use tinyagents_harness::{context::RunContext, ids::CallId, tool::ToolDispatch};
use tinytools::{Tool, ToolResult};

use super::{AdmissionPolicy, Plan, Preflight, RejectionKind, Task, Worker, run};

type Prepare<'a, P> = Pin<Box<dyn Future<Output = Result<Task<P>, String>> + Send + 'a>>;
type PreflightFn<P, Ctx> =
    dyn for<'a> Fn(usize, Task<P>, &'a RunContext<Ctx>) -> Prepare<'a, P> + Send + Sync;

/// Host-composed model tool that stages and runs independent branches.
pub struct ParallelFanoutTool<State, Ctx, P, BranchCtx, Output, MakeTasks, MakeBranch, Execute, Fut>
where
    Ctx: Send + Sync + 'static,
    P: Send + Sync + 'static,
    BranchCtx: Send + 'static,
    Output: Serialize + Send + 'static,
    MakeTasks: Fn(&State, &Value) -> anyhow::Result<Vec<Task<P>>> + Send + Sync + 'static,
    MakeBranch: Fn(usize, &RunContext<Ctx>) -> BranchCtx + Send + Sync + 'static,
    Execute: Fn(Worker<P>, BranchCtx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Output> + Send + 'static,
{
    name: String,
    description: String,
    schema: Value,
    tasks: Arc<MakeTasks>,
    policy: Arc<dyn AdmissionPolicy<P> + Send + Sync>,
    preflight: Arc<PreflightFn<P, Ctx>>,
    branch_context: Arc<MakeBranch>,
    execute: Arc<Execute>,
    max_concurrency: usize,
    state: PhantomData<fn(&State)>,
}

impl<State, Ctx, P, BranchCtx, Output, MakeTasks, MakeBranch, Execute, Fut>
    ParallelFanoutTool<State, Ctx, P, BranchCtx, Output, MakeTasks, MakeBranch, Execute, Fut>
where
    Ctx: Send + Sync + 'static,
    P: Send + Sync + 'static,
    BranchCtx: Send + 'static,
    Output: Serialize + Send + 'static,
    MakeTasks: Fn(&State, &Value) -> anyhow::Result<Vec<Task<P>>> + Send + Sync + 'static,
    MakeBranch: Fn(usize, &RunContext<Ctx>) -> BranchCtx + Send + Sync + 'static,
    Execute: Fn(Worker<P>, BranchCtx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Output> + Send + 'static,
{
    /// Creates a tool from host-supplied task parsing, policy, async preflight,
    /// per-branch context factory, and executor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        schema: Value,
        tasks: MakeTasks,
        policy: Arc<dyn AdmissionPolicy<P> + Send + Sync>,
        preflight: impl for<'a> Fn(usize, Task<P>, &'a RunContext<Ctx>) -> Prepare<'a, P>
        + Send
        + Sync
        + 'static,
        branch_context: MakeBranch,
        execute: Execute,
        max_concurrency: usize,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            schema,
            tasks: Arc::new(tasks),
            policy,
            preflight: Arc::new(preflight),
            branch_context: Arc::new(branch_context),
            execute: Arc::new(execute),
            max_concurrency,
            state: PhantomData,
        }
    }
}

#[async_trait]
impl<State, Ctx, P, BranchCtx, Output, MakeTasks, MakeBranch, Execute, Fut> ToolDispatch<State, Ctx>
    for ParallelFanoutTool<State, Ctx, P, BranchCtx, Output, MakeTasks, MakeBranch, Execute, Fut>
where
    State: Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
    P: Send + Sync + Clone + 'static,
    BranchCtx: Send + 'static,
    Output: Serialize + Send + 'static,
    MakeTasks: Fn(&State, &Value) -> anyhow::Result<Vec<Task<P>>> + Send + Sync + 'static,
    MakeBranch: Fn(usize, &RunContext<Ctx>) -> BranchCtx + Send + Sync + 'static,
    Execute: Fn(Worker<P>, BranchCtx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Output> + Send + 'static,
{
    fn tool(&self) -> Arc<dyn Tool> {
        Arc::new(FanoutDeclaration {
            name: self.name.clone(),
            description: self.description.clone(),
            schema: self.schema.clone(),
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
        let tasks = (self.tasks)(state, &args)?;
        let prepared = Plan::new(tasks)
            .stage_with_preflight(self.policy.as_ref(), |index, task| {
                (self.preflight)(index, task, parent)
            })
            .await;
        let mut workers = Vec::new();
        let mut rejected = Vec::new();
        for (index, item) in prepared.into_iter().enumerate() {
            match item {
                Preflight::Prepared(worker) => workers.push((index, worker)),
                Preflight::Rejected(rejection) => rejected.push(json!({
                    "index": index, "task_id": rejection.task.id,
                    "reason": match rejection.kind { RejectionKind::Policy(reason) => reason,
                        RejectionKind::Conflict(conflict) => format!("workspace conflict: {conflict:?}") }
                })),
            }
        }
        let indices: Vec<_> = workers.iter().map(|(index, _)| *index).collect();
        let workers: Vec<_> = workers.into_iter().map(|(_, worker)| worker).collect();
        let contexts = Arc::clone(&self.branch_context);
        let parent_ref = parent;
        let executor = Arc::clone(&self.execute);
        let results = run(
            workers,
            self.max_concurrency,
            parent.cancellation.clone(),
            move |branch| contexts(indices[branch], parent_ref),
            move |worker, context| executor(worker, context),
        )
        .await?;
        let outcomes: Vec<_> = results
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()?;
        Ok(ToolResult::json(
            json!({"results":outcomes,"rejected":rejected}),
        ))
    }
}

struct FanoutDeclaration {
    name: String,
    description: String,
    schema: Value,
}
#[async_trait]
impl Tool for FanoutDeclaration {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
    fn parameters_schema(&self) -> Value {
        self.schema.clone()
    }
    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        anyhow::bail!("parallel fan-out requires typed-parent dispatch")
    }
}
