use tinyagents_harness::ids::ThreadId;
use tinyagents_harness::thread_reasoning::ThreadReasoningStore;

#[test]
fn thread_reasoning_store_is_scoped_by_thread_and_can_be_cleared() {
    let store = ThreadReasoningStore::default();
    let thread = ThreadId::new("conversation-a");
    let other = ThreadId::new("conversation-b");

    store.apply_requested(Some(&thread), Some("high")).unwrap();
    assert_eq!(
        store.get(&thread),
        Some(tinyagents_harness::tinyinference_llm::model::ReasoningEffort::High)
    );
    assert!(store.get(&other).is_none());

    store.apply_requested(Some(&thread), Some("auto")).unwrap();
    assert!(store.get(&thread).is_none());
}
