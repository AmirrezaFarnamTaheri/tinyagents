use super::*;

#[test]
fn successful_output_summary_keeps_shape_and_discards_contents() {
    let summary = sanitize_tool_output("secret-token-value", "read_file", true);
    assert_eq!(summary, "read_file: ok (18 chars)");
    assert!(!summary.contains("secret-token-value"));
}

#[test]
fn failed_output_summary_keeps_only_a_classification() {
    let summary = sanitize_tool_output("permission denied for /private/path", "shell", false);
    assert_eq!(summary, "shell: failed (permission_denied)");
    assert!(!summary.contains("/private/path"));
}

#[test]
fn command_decision_vocabulary_keeps_input_rewrite_and_context_together() {
    let decision: ToolHookDecision = crate::command::HookOutput {
        updated_input: Some(serde_json::json!({"safe": true})),
        additional_context: Some("remember this".into()),
        ..Default::default()
    };
    assert_eq!(decision.updated_input, Some(serde_json::json!({"safe": true})));
    assert_eq!(decision.additional_context.as_deref(), Some("remember this"));
}
