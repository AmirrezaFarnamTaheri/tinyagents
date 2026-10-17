use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

use serde_json::json;
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::ids::CallId;
use tinyagents_harness::tool::ToolDispatch;

fn poison_task_registry(jobs: &SubAgentJobRegistry) {
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        let _ = jobs.tasks.cancel_where(|_| panic!("test registry poison"));
    }));
    assert!(
        poisoned.is_err(),
        "fixture must poison the task registry lock"
    );
}

#[tokio::test]
async fn opted_in_control_tools_keep_task_registry_faults_fatal() {
    let jobs = SubAgentJobRegistry::new();
    let parent = RunContext::new(RunConfig::new("registry-fault"), ());
    let (job_id, _) = jobs.create("worker", parent.instance_id());
    poison_task_registry(&jobs);

    let query = SubAgentJobsTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &query,
        &(),
        CallId::new("query"),
        json!({"job_id": job_id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a poisoned task registry is a real query failure");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Registry(_))
    ));

    let list = SubAgentJobsTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &list,
        &(),
        CallId::new("list"),
        json!({}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a poisoned task registry is a real list failure");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Registry(_))
    ));

    let await_all = SubAgentJobsTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &await_all,
        &(),
        CallId::new("await-all"),
        json!({"action": "await"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a poisoned task registry is a real await-all failure");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Registry(_))
    ));

    let cancel = SubAgentJobsTool::new(jobs.clone()).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &cancel,
        &(),
        CallId::new("cancel"),
        json!({"action": "cancel", "job_id": job_id.as_str()}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a poisoned task registry is a real cancel failure");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Registry(_))
    ));

    let message = SubAgentMessageTool::new(jobs).recover_selector_errors();
    let error = ToolDispatch::<(), ()>::execute(
        &message,
        &(),
        CallId::new("message"),
        json!({"job_id": job_id.as_str(), "message": "continue"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect_err("a poisoned task registry is a real message failure");
    assert!(matches!(
        error.downcast_ref::<SubAgentJobError>(),
        Some(SubAgentJobError::Registry(_))
    ));
}
