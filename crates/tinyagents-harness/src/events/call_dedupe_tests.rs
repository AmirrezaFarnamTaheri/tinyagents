use super::*;

#[test]
fn only_the_first_claim_for_an_iteration_wins() {
    let dedupe = CallDedupe::default();
    assert!(dedupe.first_claim(1));
    assert!(!dedupe.first_claim(1));
    assert!(dedupe.first_claim(2));
    assert!(!dedupe.first_claim(2));
}

#[test]
fn cursor_is_one_based_and_monotonic() {
    let cursor = IterationCursor::default();
    assert_eq!(advance_iteration(&cursor), 1);
    assert_eq!(advance_iteration(&cursor), 2);
    assert_eq!(cursor.load(Ordering::SeqCst), 2);
}
