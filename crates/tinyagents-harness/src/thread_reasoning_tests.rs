use super::*;
use std::sync::Barrier;

#[test]
fn effort_updates_are_isolated_and_shared_by_cloned_handles() {
    let store = ThreadReasoningStore::default();
    let clone = store.clone();
    let separate = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");
    let other = ThreadId::new("conversation-b");

    assert_eq!(store.set_effort(&thread, ReasoningEffort::High), None);
    assert_eq!(clone.get(&thread), Some(ReasoningEffort::High));
    assert_eq!(separate.get(&thread), None);
    assert_eq!(store.get(&other), None);
    assert_eq!(clone.clear(&thread), Some(ReasoningEffort::High));
    assert_eq!(store.get(&thread), None);
}

#[test]
fn requested_effort_supports_noop_clear_and_invalid_without_mutation() {
    let store = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");

    assert_eq!(store.apply_requested(None, Some("high")).unwrap(), None);
    assert_eq!(store.apply_requested(Some(&thread), None).unwrap(), None);
    assert_eq!(
        store
            .apply_requested(Some(&thread), Some(" high "))
            .unwrap(),
        None
    );
    assert_eq!(store.get(&thread), Some(ReasoningEffort::High));

    assert_eq!(
        store.apply_requested(Some(&thread), None).unwrap(),
        Some(ReasoningEffort::High)
    );
    assert!(
        store
            .apply_requested(Some(&thread), Some("mystery"))
            .is_err()
    );
    assert_eq!(store.get(&thread), Some(ReasoningEffort::High));
    for clear in ["", "  ", "DEFAULT", "Auto"] {
        assert_eq!(
            store.apply_requested(Some(&thread), Some(clear)).unwrap(),
            Some(ReasoningEffort::High)
        );
        assert_eq!(store.get(&thread), None);
        store.set_effort(&thread, ReasoningEffort::High);
    }
}

#[test]
fn requested_effort_uses_shared_parser_aliases() {
    let store = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");

    for (input, expected) in [
        ("off", ReasoningEffort::None),
        ("disabled", ReasoningEffort::None),
        ("max", ReasoningEffort::XHigh),
        ("maximum", ReasoningEffort::XHigh),
        ("med", ReasoningEffort::Medium),
    ] {
        store.apply_requested(Some(&thread), Some(input)).unwrap();
        assert_eq!(store.get(&thread), Some(expected));
    }
}

#[test]
fn separate_threads_can_update_concurrently() {
    let store = ThreadReasoningStore::default();
    let barrier = Arc::new(Barrier::new(3));
    let workers = [
        ("conversation-a", ReasoningEffort::Low),
        ("conversation-b", ReasoningEffort::High),
    ]
    .into_iter()
    .map(|(thread, effort)| {
        let store = store.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let thread = ThreadId::new(thread);
            barrier.wait();
            store.set_effort(&thread, effort);
            (thread, effort)
        })
    })
    .collect::<Vec<_>>();
    barrier.wait();

    for worker in workers {
        let (thread, effort) = worker.join().unwrap();
        assert_eq!(store.get(&thread), Some(effort));
    }
}

#[test]
fn turn_snapshot_is_stable_across_store_updates_and_uses_output_room() {
    let store = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");
    store.set_effort(&thread, ReasoningEffort::High);

    let turn_snapshot = store.reasoning_for_turn(Some(&thread), Some(4_000));
    store.set_effort(&thread, ReasoningEffort::Low);

    assert_eq!(
        turn_snapshot,
        Some(ReasoningConfig {
            effort: Some(ReasoningEffort::High),
            budget_tokens: Some(2_200),
            summary: None,
        })
    );
    assert_eq!(
        store
            .reasoning_for_turn(Some(&thread), Some(4_000))
            .unwrap()
            .effort,
        Some(ReasoningEffort::Low)
    );
    assert_eq!(store.reasoning_for_turn(None, Some(4_000)), None);
}

#[test]
fn turn_snapshot_respects_output_room_minimum_and_unbounded_requests() {
    let store = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");
    store.set_effort(&thread, ReasoningEffort::Medium);

    assert_eq!(
        store
            .reasoning_for_turn(Some(&thread), Some(1_000))
            .unwrap()
            .budget_tokens,
        None
    );
    assert_eq!(
        store
            .reasoning_for_turn(Some(&thread), None)
            .unwrap()
            .budget_tokens,
        None
    );
}
