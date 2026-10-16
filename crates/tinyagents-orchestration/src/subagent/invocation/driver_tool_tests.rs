use super::*;
use std::sync::Arc;
use serde_json::json;
use tinyagents_harness::context::{RunConfig, RunContext};
use tinyagents_harness::ids::CallId;
use tinyagents_harness::tool::ToolDispatch;

#[tokio::test]
async fn background_request_without_delivery_target_runs_inline() {
    let tool = DriverSubAgentTool::new(
        "worker",
        "worker description",
        driver(),
        |_: &(), parent: &RunContext<()>, args: &Value| {
            let child = parent
                .child(RunConfig::new("worker-child"), ())
                .expect("child context");
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
    todo!("test driver fixture")
}
