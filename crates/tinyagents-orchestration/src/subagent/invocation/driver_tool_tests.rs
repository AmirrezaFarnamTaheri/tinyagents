use super::*;
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::json;
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::ids::CallId;
use tinyagents_harness::tool::ToolDispatch;
use tinyagents_runtime::ToolSnapshot;
use tinyinference_llm::message::Message;
use crate::subagent::*;

struct Planner;

#[async_trait]
impl SubagentPlanner<(), ()> for Planner {
    async fn prepare(
        &self,
        request: SubagentRequest<(), ()>,
    ) -> std::result::Result<PreparedSubagent<()>, SubagentError> {
        let parts = request.into_parts();
        Ok(PreparedSubagent::new(
            parts.task_key.task_id,
            "worker",
            vec![Message::user(parts.input)],
            ToolSnapshot::new(vec![]).unwrap(),
            parts.run_context,
        ))
    }
}

struct Executor;

#[async_trait]
impl SubagentExecutor<()> for Executor {
    async fn execute(
        &self,
        execution: SubagentExecution<()>,
    ) -> std::result::Result<SubagentOutcome, SubagentError> {
        Ok(SubagentOutcome::completed(
            execution.prepared.task_id,
            "driver result",
        ))
    }
}

struct Persistence;

#[async_trait]
impl SubagentPersistence for Persistence {
    async fn load_terminal(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(None)
    }
    async fn load(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentResume>, SubagentError> {
        Ok(None)
    }
    async fn load_pause(
        &self,
        _: &SubagentTaskKey,
    ) -> std::result::Result<Option<SubagentOutcome>, SubagentError> {
        Ok(None)
    }
    async fn save_pause(
        &self,
        _: PersistedSubagentPause,
    ) -> std::result::Result<SubagentPausePersistenceDisposition, SubagentError> {
        Ok(SubagentPausePersistenceDisposition::Inserted)
    }
    async fn record_terminal(
        &self,
        _: &SubagentTaskKey,
        _: &SubagentOutcome,
        _: Option<&SubagentResume>,
    ) -> std::result::Result<SubagentTerminalPersistenceDisposition, SubagentError> {
        Ok(SubagentTerminalPersistenceDisposition::Inserted)
    }
}

#[tokio::test]
async fn background_request_without_delivery_target_runs_inline() {
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value, _: &CallId| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
            .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                "worker-task",
                (),
                args["input"].as_str().unwrap_or_default(),
                None,
            )
        },
        |_request| None,
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());

    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("call-1"),
        json!({"input":"do work", "mode":"background"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();

    assert!(!result.is_error);
    let value: Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["output"], "driver result");
}

fn driver() -> Arc<SubagentDriver<(), ()>> {
    Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(Executor)),
            persistence: Some(Arc::new(Persistence)),
        })
        .unwrap(),
    )
}
