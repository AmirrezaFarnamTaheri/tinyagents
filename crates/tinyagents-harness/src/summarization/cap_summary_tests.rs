use super::*;

#[test]
fn digest_keeps_order_status_and_utf8_safe_bounded_output() {
    let digest = build_cap_digest(
        &[
            CapToolResult {
                call_id: "1",
                name: "read",
                output: "ééé",
                success: true,
            },
            CapToolResult {
                call_id: "2",
                name: "write",
                output: "failed",
                success: false,
            },
        ],
        2,
    );
    assert_eq!(digest, "- read [ok]: éé…\n- write [failed]: fa…");
}

#[test]
fn fallback_always_marks_the_run_incomplete() {
    let text = cap_summary_fallback(4, "- read [ok]: file");
    assert!(text.contains("tool-call limit (4 steps)"));
    assert!(text.contains("task is incomplete"));
}
