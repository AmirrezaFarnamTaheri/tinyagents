use super::*;

#[test]
fn within_budget_class_with_instruction_nudges_once_then_halts() {
    let t = ClassifiedFailureTracker::default();
    let first = classified_step(
        &t,
        "invalid_arguments",
        "apply_patch",
        "apply_patch",
        3,
        "bad\nmore",
    );
    match first {
        ClassifiedStep::Nudge(text) => {
            assert!(text.contains("`apply_patch`") && text.contains("(bad)"))
        }
        other => panic!("expected nudge, got {other:?}"),
    }
    for _ in 0..2 {
        assert!(matches!(
            classified_step(
                &t,
                "invalid_arguments",
                "apply_patch",
                "apply_patch",
                3,
                "bad"
            ),
            ClassifiedStep::Nudge(_)
        ));
    }
    assert!(matches!(
        classified_step(
            &t,
            "invalid_arguments",
            "apply_patch",
            "apply_patch",
            3,
            "bad"
        ),
        ClassifiedStep::Halt(_)
    ));
}

#[test]
fn zero_budget_class_halts_on_first_failure() {
    let t = ClassifiedFailureTracker::default();
    assert!(matches!(
        classified_step(&t, "authentication", "gmail", "gmail", 0, "401"),
        ClassifiedStep::Halt(_)
    ));
}

#[test]
fn class_without_instruction_stays_within_budget() {
    let t = ClassifiedFailureTracker::default();
    assert_eq!(
        classified_step(&t, "transient", "web_fetch", "web_fetch", 2, "503"),
        ClassifiedStep::Within
    );
}

#[test]
fn uncertain_side_effect_halt_carries_the_reconcile_suffix() {
    let t = ClassifiedFailureTracker::default();
    match classified_step(
        &t,
        "uncertain_side_effect",
        "gmail_send",
        "gmail_send",
        0,
        "timeout",
    ) {
        ClassifiedStep::Halt(s) => {
            assert_eq!(s.class, "uncertain_side_effect");
            assert_eq!(s.operation, "gmail_send");
            assert!(s.summary.ends_with(UNCERTAIN_SIDE_EFFECT_HALT_SUFFIX));
        }
        other => panic!("expected halt, got {other:?}"),
    }
}

#[test]
fn success_clears_every_class_for_the_operation_only() {
    let t = ClassifiedFailureTracker::default();
    assert!(matches!(
        classified_step(&t, "validation", "a", "s", 1, "x"),
        ClassifiedStep::Nudge(_)
    ));
    clear_recovered(&t, "a", "s");
    // Counter restarted: the first failure is within budget again.
    assert!(matches!(
        classified_step(&t, "validation", "a", "s", 1, "x"),
        ClassifiedStep::Nudge(_)
    ));
    // A different scope is untouched by clearing.
    assert!(matches!(
        classified_step(&t, "validation", "a", "other", 1, "x"),
        ClassifiedStep::Nudge(_)
    ));
    clear_recovered(&t, "a", "s");
    assert!(matches!(
        classified_step(&t, "validation", "a", "other", 1, "x"),
        ClassifiedStep::Halt(_)
    ));
}

#[test]
fn every_nudged_class_is_a_known_class_with_an_instruction() {
    for class in RECOVERY_CLASSES {
        assert_eq!(
            class_has_nudge(class),
            recovery_nudge(class, "t", "l").is_some() && class_has_nudge(class)
        );
    }
    assert!(recovery_nudge("permission", "t", "l").is_none());
}

#[test]
fn json_error_contract_maps_status_and_code() {
    let d = |s: &str| json_error_recovery(s).map(|d| (d.class, d.budget));
    assert_eq!(d(r#"{"status_code":401}"#), Some(("authentication", 0)));
    assert_eq!(
        d(r#"{"error":{"status_code":503}}"#),
        Some(("transient", 2))
    );
    assert_eq!(d(r#"{"status":422}"#), Some(("validation", 1)));
    assert_eq!(d(r#"{"code":"APP_NOT_FOUND"}"#), Some(("missing_app", 1)));
    assert_eq!(d(r#"{"status":200,"data":[1]}"#), None);
    assert_eq!(d("plain text"), None);
}

#[test]
fn clear_recovered_clears_classes_outside_the_known_list() {
    let tracker = ClassifiedFailureTracker::default();
    assert!(matches!(
        classified_step(&tracker, "custom_class", "t", "s", 1, "x"),
        ClassifiedStep::Within
    ));
    clear_recovered(&tracker, "t", "s");
    assert!(matches!(
        classified_step(&tracker, "custom_class", "t", "s", 1, "x"),
        ClassifiedStep::Within
    ));
}

#[test]
fn json_error_recovery_falls_through_nonnumeric_status() {
    let d = json_error_recovery(r#"{"status":"error","error":{"status_code":401}}"#);
    assert!(d.is_some());
}

#[test]
fn json_error_recovery_falls_through_nonstring_code() {
    let d = json_error_recovery(r#"{"code":7,"error":{"code":"UNAUTHENTICATED"}}"#);
    assert!(d.is_some());
}
