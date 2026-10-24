use super::*;

#[test]
fn decisions_keep_the_original_tagged_json_wire_form() {
    let fixtures = [
        (PlanReviewResolution::Approve, r#"{"decision":"approve"}"#),
        (PlanReviewResolution::Reject, r#"{"decision":"reject"}"#),
        (
            PlanReviewResolution::Revise {
                feedback: "add tests".into(),
            },
            r#"{"decision":"revise","feedback":"add tests"}"#,
        ),
    ];
    for (resolution, expected) in fixtures {
        let value = serde_json::to_string(&resolution).unwrap();
        assert_eq!(value, expected);
        assert_eq!(
            serde_json::from_str::<PlanReviewResolution>(expected).unwrap(),
            resolution
        );
    }
}

#[test]
fn internal_tool_outcomes_are_not_client_wire_decisions() {
    assert!(serde_json::to_string(&PlanReviewResolution::NonInteractiveApprove).is_err());
    assert!(serde_json::to_string(&PlanReviewResolution::NotApplicable).is_err());
    assert!(
        serde_json::from_str::<PlanReviewResolution>(r#"{"decision":"non_interactive_approve"}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<PlanReviewResolution>(r#"{"decision":"not_applicable"}"#).is_err()
    );
    assert_eq!(PlanReviewResolution::Approve.as_str(), "approve");
    assert_eq!(PlanReviewResolution::Reject.as_str(), "reject");
    assert_eq!(
        PlanReviewResolution::Revise {
            feedback: "private feedback".into()
        }
        .as_str(),
        "revise"
    );
}
