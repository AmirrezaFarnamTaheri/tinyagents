use super::*;
use tinyagents_graph::delegation::{DelegationConfig, DelegationStage, DelegationStageOutput};
use tinytools::ToolExposure;

#[test]
fn approval_parser_requires_a_standalone_approve_token() {
    assert!(parse_approval(" APPROVE\n"));
    assert!(!parse_approval("APPROVE, because it looks good"));
    assert!(!parse_approval("not approved"));
}

#[test]
fn multi_target_schema_exposes_only_configured_agents_and_blocking_envelope() {
    let tool = MultiTargetDelegationTool::<(), (), _, _>::new(
        "delegate_to",
        "Delegate to a specialist",
        vec![
            ("coder".into(), "Writes code".into()),
            ("reviewer".into(), "Reviews".into()),
        ],
        |_, _, _| async { Ok(DelegationStageOutput::done("ok")) },
    );
    let declaration = tool.with_exposure(ToolExposure::Hidden).tool();
    let schema = declaration.parameters_schema();
    assert_eq!(
        schema["properties"]["agent"]["enum"],
        json!(["coder", "reviewer"])
    );
    assert_eq!(schema["properties"]["blocking"]["type"], "boolean");
    assert_eq!(DELEGATION_MODE_FIELD, SUBAGENT_MODE_FIELD);
    assert_eq!(declaration.exposure(), ToolExposure::Hidden);
}

#[test]
fn delegation_envelope_keeps_structured_instructions_and_mode() {
    let envelope: DelegationEnvelope = serde_json::from_value(json!({
        "prompt": "Inspect the patch", "objective": "Find regressions",
        "must_not_assume": "Tests passed", "blocking": true
    }))
    .unwrap();
    assert!(envelope.blocking);
    assert_eq!(envelope.objective.as_deref(), Some("Find regressions"));
    assert_eq!(envelope.must_not_assume.as_deref(), Some("Tests passed"));
}

#[tokio::test]
async fn graph_delegation_runs_stage_worker_with_original_task_and_approval_parser() {
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        ids::new_call_id,
        tool::ToolDispatch,
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let worker_seen = seen.clone();
    let tool = DelegationTool::<(), (), _, _>::new(
        |_, _, _| DelegationConfig::default(),
        move |args, stage, _| {
            let seen = worker_seen.clone();
            async move {
                seen.lock()
                    .unwrap()
                    .push((args["task"].as_str().unwrap().to_owned(), stage));
                Ok(match stage {
                    DelegationStage::Plan => DelegationStageOutput::done("plan"),
                    DelegationStage::Execute => DelegationStageOutput::done("result"),
                    DelegationStage::Review => review_stage_output("APPROVE"),
                })
            }
        },
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let result = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"task":"ship a safe fix"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    assert!(!result.is_error);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert!(seen.iter().all(|(task, _)| task == "ship a safe fix"));
    assert_eq!(seen[0].1, DelegationStage::Plan);
    assert_eq!(seen[2].1, DelegationStage::Review);
}
