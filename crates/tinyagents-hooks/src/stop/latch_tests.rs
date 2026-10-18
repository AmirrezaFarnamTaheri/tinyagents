use super::*;
use std::sync::Arc;
use tinyagents_harness::steering::SteeringHandle;

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
    assert_eq!(
        latch.current_classified_cause(),
        Some(ClassifiedStopCause {
            class: "not_found".into(),
            operation: "file_read".into(),
            summary: "classified".into(),
        })
    );
    latch.record_refusal();
    let relabelled = note(&slot).unwrap();
    assert!(relabelled.contains("kept re-issuing"), "{relabelled}");
    let cause = latch.current_classified_cause().expect("relabel cause");
    assert_eq!(cause.class, "repeating_step");
    assert_eq!(cause.operation, "file_read");
    assert_eq!(cause.summary, relabelled);
}

#[test]
fn a_success_in_the_batch_restores_the_classified_note() {
    let (latch, slot) = latch();
    latch.halt_classified("classified".into(), "not_found", "file_read", true, || {
        "No such file".into()
    });
    latch.record_refusal();
    assert_eq!(
        latch.current_classified_cause().unwrap().class,
        "repeating_step"
    );
    latch.record_success();
    assert_eq!(note(&slot).as_deref(), Some("classified"));
    assert_eq!(
        latch.current_classified_cause(),
        Some(ClassifiedStopCause {
            class: "not_found".into(),
            operation: "file_read".into(),
            summary: "classified".into(),
        })
    );
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
    let cause = latch.current_classified_cause().unwrap();
    assert_eq!(cause.class, "policy");
    assert_eq!(cause.operation, "shell");
}

#[test]
fn classified_bookkeeping_does_not_pause_before_the_driver_halts_once() {
    let slot: HaltSummarySlot = Arc::default();
    let handle = SteeringHandle::allow_all();
    let latch = StopLatch::new(handle.clone(), slot.clone());

    latch.record_classified_halt("classified".into(), "permission", "search", false, || {
        "denied".into()
    });
    assert!(handle.drain().is_empty(), "bookkeeping does not steer");
    assert_eq!(note(&slot).as_deref(), Some("classified"));

    assert!(!latch.halt("classified".into()));
    assert_eq!(
        handle.drain(),
        vec![SteeringCommand::Pause],
        "the containing driver owns exactly one pause"
    );
}
