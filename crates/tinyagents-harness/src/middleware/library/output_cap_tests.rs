use super::*;

struct P;
impl OutputCapPolicy for P {
    fn default_budget_bytes(&self) -> usize {
        16_000
    }
    fn tool_cap(&self, tool: &str) -> Option<usize> {
        (tool == "web_fetch").then_some(50_000)
    }
    fn is_compaction_exempt(&self, tool: &str) -> bool {
        tool == "propose_workflow" || tool == "mcp_list_tools"
    }
    fn is_truncation_exempt(&self, tool: &str) -> bool {
        tool == "propose_workflow" || tool == "mcp_list_tools"
    }
    fn max_persistable_bytes(&self) -> u64 {
        1_000_000
    }
}

#[test]
fn shared_budget_applies_without_a_declared_cap() {
    let plan = plan_output_stages(&P, "shell", 20_000, true);
    assert_eq!(plan.budget_bytes, 16_000);
    assert!(!plan.tool_declared_cap);
    assert!(plan.keep_full_output);
}

#[test]
fn declared_cap_wins_over_the_shared_budget() {
    let plan = plan_output_stages(&P, "web_fetch", 20_000, true);
    assert_eq!(plan.budget_bytes, 50_000);
    assert!(plan.tool_declared_cap);
    assert!(!plan.keep_full_output, "within the declared cap");
}

#[test]
fn truncation_exempt_tools_are_never_capped_or_kept() {
    let plan = plan_output_stages(&P, "propose_workflow", 900_000, true);
    assert!(plan.truncation_exempt && plan.compaction_exempt);
    assert!(!plan.keep_full_output);
}

#[test]
fn oversized_bodies_are_not_kept_for_persistence() {
    assert!(!plan_output_stages(&P, "shell", 2_000_000, true).keep_full_output);
    assert!(!plan_output_stages(&P, "shell", 20_000, false).keep_full_output);
}

#[test]
fn capped_tools_skip_compaction_unless_focused() {
    let capped = plan_output_stages(&P, "web_fetch", 10, true);
    assert!(!should_compact(&capped, false, true));
    assert!(should_compact(&capped, true, true));
    let plain = plan_output_stages(&P, "shell", 10, true);
    assert!(should_compact(&plain, false, true));
    assert!(!should_compact(&plain, false, false));
    let exempt = plan_output_stages(&P, "mcp_list_tools", 10, true);
    assert!(!should_compact(&exempt, true, true));
}

#[test]
fn disclosure_orders_summary_label_before_notice() {
    let out = disclose("body".into(), Some("NOTICE"), Some(42), "host");
    assert_eq!(
        out,
        "[host: summary of 42 bytes of tool output, complete]\n\nNOTICE\n\nbody"
    );
    assert_eq!(disclose("body".into(), None, None, "host"), "body");
}
