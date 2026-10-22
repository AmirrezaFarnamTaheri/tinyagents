use super::*;
use serde_json::json;
use std::sync::Arc;
use tinyagents_harness::{
    context::{RunConfig, RunContext},
    ids::CallId,
    tool::ToolDispatch,
};

struct ReadOnly;
impl AdmissionPolicy<String> for ReadOnly {
    fn admit(&self, _: usize, _: &Task<String>) -> AdmissionVerdict {
        AdmissionVerdict::ReadOnly
    }
}

#[tokio::test]
async fn fanout_runs_preflighted_branches_in_input_order_and_reports_rejections() {
    let tool = ParallelFanoutTool::<(), (), String, (), String, _, _, _, _>::new(
        "spawn_parallel",
        "Run independent branches",
        json!({"type":"object"}),
        |_, _| {
            Ok(vec![
                Task {
                    id: "a".into(),
                    prompt: "first".into(),
                    ownership: None,
                    payload: "one".into(),
                },
                Task {
                    id: "b".into(),
                    prompt: "second".into(),
                    ownership: None,
                    payload: "two".into(),
                },
            ])
        },
        Arc::new(ReadOnly),
        |_, mut task, _| {
            Box::pin(async move {
                task.prompt.push_str(" prepared");
                Ok(task)
            })
        },
        |_, _| (),
        |worker, ()| async move { format!("{}:{}", worker.id, worker.prompt) },
        2,
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        CallId::new("parallel"),
        json!({}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&result.output()).unwrap();
    assert_eq!(
        value["results"],
        json!(["a:first prepared", "b:second prepared"])
    );
    assert_eq!(value["rejected"], json!([]));
}
