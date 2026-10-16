use super::*;

#[test]
fn render_tool_results_spends_the_budget_on_newest_results() {
    let results = vec![
        CloseToolResult { name: "old".into(), success: true, content: "old output".into() },
        CloseToolResult { name: "new".into(), success: false, content: "new output".into() },
    ];
    let rendered = render_tool_results(&results, 30);
    assert!(rendered.contains("new"));
    assert!(!rendered.contains("old output"));
}

#[test]
fn verdict_parser_accepts_only_an_exact_verdict_line() {
    assert_eq!(parse_close_verdict("ACCEPT\nLooks good"), CloseVerdict::Accept);
    assert_eq!(parse_close_verdict("REJECT"), CloseVerdict::Reject);
    assert_eq!(parse_close_verdict("probably ACCEPT"), CloseVerdict::Unclear);
    assert_eq!(parse_close_verdict(""), CloseVerdict::Unclear);
}

#[test]
fn harness_quote_matching_normalizes_whitespace_but_ignores_short_phrases() {
    assert!(quotes_harness_instruction("Do not call another tool right now", &["Do not call another tool right now"]));
    assert!(!quotes_harness_instruction("The answer is yes", &["yes"]));
}

#[test]
fn repair_prompt_inserts_the_violation_into_host_template() {
    let prompt = close_repair_instruction("{repair_reason}\nTry again", CloseViolation::Unverified);
    assert!(prompt.starts_with("Your previous answer was not verified"));
}
