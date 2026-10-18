use std::sync::Arc;

use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::runtime::AgentHarness;
use tinyagents_orchestration::subagent::{SubAgentJob, SubAgentJobRegistry, SubAgentJobStatus};
use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};

pub struct BlockedModel {
    pub started: Arc<tokio::sync::Semaphore>,
    pub release: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl ChatModel<()> for BlockedModel {
    async fn invoke(
        &self,
        _state: &(),
        _request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.started.add_permits(1);
        let _permit = self
            .release
            .acquire()
            .await
            .expect("release semaphore open");
        Ok(ModelResponse::assistant("finished later"))
    }
}

pub fn spawned_job_id(result: &tinytools::ToolResult) -> String {
    serde_json::from_str::<serde_json::Value>(&result.output())
        .expect("spawn result is JSON")
        .get("job_id")
        .and_then(serde_json::Value::as_str)
        .expect("spawn result contains a job id")
        .to_owned()
}

pub async fn wait_for_terminal(
    jobs: &SubAgentJobRegistry,
    job_id: &str,
    owner: impl ToString,
) -> SubAgentJob {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let job = jobs
                .get_owned(job_id, owner.to_string())
                .expect("spawned job is registered");
            if job.status.is_terminal() {
                return job;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("job reaches a terminal state")
}

pub fn child_harness() -> AgentHarness<(), ()> {
    let mut harness = AgentHarness::new();
    harness.register_model(
        "worker",
        Arc::new(tinyinference_llm::providers::MockModel::constant(
            "finished later",
        )),
    );
    harness
}

pub fn terminal(job: &SubAgentJob) -> bool {
    job.status.is_terminal()
}

pub fn is_running(job: &SubAgentJob) -> bool {
    job.status == SubAgentJobStatus::Running
}

pub fn parent() -> RunContext<()> {
    RunContext::new(RunConfig::new("parent"), ())
}
