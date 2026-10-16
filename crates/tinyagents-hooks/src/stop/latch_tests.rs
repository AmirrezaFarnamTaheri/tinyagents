use super::*;
use tinyagents_harness::steering::SteeringHandle;
use std::sync::Arc;

fn latch() -> (StopLatch, HaltSummarySlot) {
    let slot: HaltSummarySlot = Arc::default();
    (
        StopLatch::new(SteeringHandle::allow_all(), Arc::clone(&slot)),
        slot,
    )
}

fn note(slot: &HaltSummarySlot) -> Option<String> {
    slot.lock().unwrap().clone()
}

#[test]
fn the_first_breaker_to_stop_the_run_owns_the_note() {
    let (latch, slot) = latch();
    assert!(latch.halt("first".into()));
    assert!(!latch.halt("second".into()));
    assert_eq!(note(&slot).as_deref(), Some("first"));
}

#[test]
fn a_refusal_after_a_classified_repeat_stop_relabels_it() {
    let (latch, slot) = latch();
    latch.halt_classified("classified".into(), "not_found", "file_read", true, || {
        "No such file".into()
    });
    assert_eq!(note(&slot).as_deref(), Some("classified"));
    latch.record_refusal();
    let relabelled = note(&slot).unwrap();
    assert!(relabelled.contains("kept re-issuing"), "{relabelled}");
}

#[test]
fn a_success_in_the_batch_restores_the_classified_note() {
    let (latch, slot) = latch();
    latch.halt_classified("classified".into(), "not_found", "file_read", true, || {
        "No such file".into()
    });
    latch.record_refusal();
    latch.record_success();
    assert_eq!(note(&slot).as_deref(), Some("classified"));
}

#[test]
fn a_batch_that_made_progress_is_never_relabelled() {
    let (latch, slot) = latch();
    latch.start_batch();
    latch.record_success();
    latch.halt_classified("classified".into(), "not_found", "file_read", true, || {
        "No such file".into()
    });
    latch.record_refusal();
    assert_eq!(note(&slot).as_deref(), Some("classified"));
}

#[test]
fn a_security_class_keeps_its_wording() {
    let (latch, slot) = latch();
    latch.record_refusal();
    latch.halt_classified("denied".into(), "policy", "shell", true, || "x".into());
    assert_eq!(note(&slot).as_deref(), Some("denied"));
}
