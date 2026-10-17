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
        |_| async { Ok(DelegationStageOutput::done("ok")) },
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
async fn multi_target_blocking_maps_to_subagent_mode_and_validates_target() {
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        ids::new_call_id,
        tool::ToolDispatch,
    };
    let tool = MultiTargetDelegationTool::<(), (), _, _>::new(
        "delegate_to",
        "Delegate",
        vec![("coder".into(), "Writes code".into())],
        |request| async move { Ok(DelegationStageOutput::done(request.mode)) },
    );
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let inline = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"agent":"coder","prompt":"fix","blocking":true}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .unwrap();
    let value: serde_json::Value = serde_json::from_str(&inline.output()).unwrap();
    assert_eq!(value["mode"], "inline");
    let invalid = ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"agent":"unlisted","prompt":"fix"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await;
    assert!(invalid.is_err());
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
                seen.lock().unwrap().push((
                    args["task"].as_str().unwrap().to_owned(),
                    stage,
                    args["stage_prompt"].as_str().unwrap().to_owned(),
                ));
                Ok(match stage {
                    DelegationStage::Plan => DelegationStageOutput::done("plan"),
                    DelegationStage::Execute => DelegationStageOutput::done("result"),
                    DelegationStage::Review => DelegationStageOutput::done("APPROVE, looks good"),
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
    assert!(seen.len() >= 3);
    assert!(
        seen.len() > 3,
        "non-standalone APPROVE must trigger another review cycle"
    );
    assert!(seen.iter().all(|(task, _, _)| task == "ship a safe fix"));
    assert_eq!(seen[0].1, DelegationStage::Plan);
    assert_eq!(seen[0].2, DELEGATION_PLAN_PROMPT);
    assert_eq!(seen[1].2, DELEGATION_EXECUTE_PROMPT);
    assert_eq!(seen[2].1, DelegationStage::Review);
    assert_eq!(seen[2].2, DELEGATION_REVIEW_PROMPT);
    assert!(!parse_approval("APPROVE, looks good"));
}

#[tokio::test]
async fn graph_alias_keeps_host_declaration_and_routes_arguments_to_stage_worker() {
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        ids::new_call_id,
        tool::ToolDispatch,
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let worker_seen = seen.clone();
    let schema = json!({
        "type":"object",
        "required":["agent_id","task"],
        "properties":{
            "agent_id":{"type":"string","enum":["planner","reviewer"]},
            "task":{"type":"string"},
            "revisions":{"type":"integer","minimum":0,"maximum":5,"default":2}
        }
    });
    let tool = DelegationTool::<(), (), _, _>::new(
        |_, _, _| DelegationConfig::default(),
        move |args, stage, _| {
            let seen = worker_seen.clone();
            async move {
                seen.lock().unwrap().push((
                    args["agent_id"].as_str().unwrap().to_owned(),
                    args["revisions"].as_u64().unwrap(),
                    stage,
                ));
                Ok(DelegationStageOutput::done(
                    if stage == DelegationStage::Review {
                        "APPROVE"
                    } else {
                        "ok"
                    },
                ))
            }
        },
    )
    .with_tool_name("delegate_graph")
    .with_description("Plan and review delegated work.")
    .with_parameters(schema.clone());
    let declaration = tool.tool();
    assert_eq!(declaration.name(), "delegate_graph");
    assert_eq!(declaration.description(), "Plan and review delegated work.");
    assert_eq!(declaration.parameters_schema(), schema);

    let parent = RunContext::new(RunConfig::new("parent"), ());
    ToolDispatch::<(), ()>::execute(
        &tool,
        &(),
        new_call_id(),
        json!({"agent_id":"planner","task":"ship the fix","revisions":3}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .expect("custom alias executes the typed graph worker");
    let seen = seen.lock().unwrap();
    assert!(seen.len() >= 3);
    assert!(
        seen.iter()
            .all(|(agent, revisions, _)| agent == "planner" && *revisions == 3)
    );
}

#[tokio::test]
async fn resumable_delegation_restarts_at_the_pending_stage() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tinyagents_graph::{DelegationState, checkpoint::InMemoryCheckpointer};
    use tinyagents_harness::{
        context::{RunConfig, RunContext},
        ids::new_call_id,
        tool::ToolDispatch,
    };

    let checkpointer = std::sync::Arc::new(InMemoryCheckpointer::<DelegationState>::new());
    let fail_execute_once = std::sync::Arc::new(AtomicBool::new(true));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let make_tool = || {
        let cp = checkpointer.clone();
        let fail = fail_execute_once.clone();
        let calls = calls.clone();
        DelegationTool::<(), (), _, _>::new(
            move |_, _, _| DelegationConfig {
                checkpointer: Some(cp.clone()),
                thread_id: Some("durable-delegation".into()),
                ..DelegationConfig::default()
            },
            move |_, stage, _| {
                let fail = fail.clone();
                let calls = calls.clone();
                async move {
                    calls.lock().unwrap().push(stage);
                    if stage == DelegationStage::Execute
                        && fail.swap(false, Ordering::SeqCst)
                    {
                        return Err("simulated process interruption".into());
                    }
                    Ok(DelegationStageOutput::done(if stage == DelegationStage::Review {
                        "APPROVE"
                    } else {
                        "completed"
                    }))
                }
            },
        )
        .with_resumable_execution()
    };
    let parent = RunContext::new(RunConfig::new("parent"), ());
    let first = make_tool();
    assert!(ToolDispatch::<(), ()>::execute(
        &first,
        &(),
        new_call_id(),
        json!({"task":"resume me"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .is_err());

    // A new dispatcher models a host process starting from the persisted graph.
    let restarted = make_tool();
    assert!(ToolDispatch::<(), ()>::execute(
        &restarted,
        &(),
        new_call_id(),
        json!({"task":"resume me"}),
        tinytools::ToolCallOptions::default(),
        &parent,
    )
    .await
    .is_ok());

    let calls = calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|&&stage| stage == DelegationStage::Plan).count(), 1);
    assert_eq!(calls.iter().filter(|&&stage| stage == DelegationStage::Execute).count(), 2);
}
