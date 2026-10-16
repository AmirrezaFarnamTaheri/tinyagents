use super::*;
use tinyagents_graph::delegation::DelegationStageOutput;

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
    let schema = tool.tool().parameters_schema();
    assert_eq!(
        schema["properties"]["agent"]["enum"],
        json!(["coder", "reviewer"])
    );
    assert_eq!(schema["properties"]["blocking"]["type"], "boolean");
    assert_eq!(DELEGATION_MODE_FIELD, SUBAGENT_MODE_FIELD);
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
