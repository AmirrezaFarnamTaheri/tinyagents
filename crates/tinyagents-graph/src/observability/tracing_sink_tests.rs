use super::*;
use crate::ids::{NodeId, RunId};
use std::sync::atomic::Ordering;

#[test]
fn every_emitted_event_is_counted_including_unmatched_variants() {
    let sink = GraphTracingSink::new("test:graph");
    let count = sink.counter();
    sink.emit(GraphEventEnvelope::for_test(GraphEvent::RunStarted {
        run_id: RunId::new("r"),
    }));
    sink.emit(GraphEventEnvelope::for_test(GraphEvent::NodeFailed {
        node: NodeId::new("n"),
        step: 1,
        error: "boom".into(),
    }));
    sink.emit(GraphEventEnvelope::for_test(GraphEvent::StepCompleted {
        step: 1,
    }));
    assert_eq!(count.load(Ordering::Relaxed), 3);
}

#[test]
fn runtime_built_labels_are_accepted() {
    let _ = GraphTracingSink::new(format!("team:{}", 7));
}
