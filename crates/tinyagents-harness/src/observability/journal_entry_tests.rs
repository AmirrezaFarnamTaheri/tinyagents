use super::*;
use crate::events::AgentEvent;
use serde_json::json;

fn observation(run: &str, root: &str, offset: u64, event_id: String) -> AgentObservation {
    AgentObservation {
        event_id: EventId::new(event_id),
        run_id: RunId::new(run),
        parent_run_id: (run != root).then(|| RunId::new(root)),
        root_run_id: RunId::new(root),
        offset,
        ts_ms: 1_700_000_000_000 + offset,
        event: AgentEvent::StreamClosed,
    }
}

#[test]
fn a_top_level_entry_repeats_neither_run_ids_nor_the_event_id() {
    let obs = observation("run.a", "run.a", 7, canonical_event_id("run.a", 7).as_str().to_owned());
    let stored = serde_json::to_value(JournalEntry::from_observation(&obs)).unwrap();
    assert_eq!(
        stored,
        json!({"offset": 7, "ts_ms": 1_700_000_000_007_u64, "event": {"kind": "stream_closed"}})
    );
}

#[test]
fn an_entry_rebuilds_the_wire_observation_from_the_stream_name() {
    let obs = observation("run.a", "run.a", 7, canonical_event_id("run.a", 7).as_str().to_owned());
    let stored = serde_json::to_value(JournalEntry::from_observation(&obs)).unwrap();
    let rebuilt = serde_json::from_value::<JournalEntry>(stored)
        .unwrap()
        .into_observation("run.a");
    assert_eq!(rebuilt, obs);
    assert_eq!(rebuilt.event_id.as_str(), "run.a-evt-7");
}

#[test]
fn a_child_run_keeps_the_root_it_cannot_derive_and_a_foreign_event_id() {
    // A child run's events may be numbered by its root's sink.
    let obs = observation("run.child", "run.root", 3, "run.root-evt-3".to_owned());
    let stored = serde_json::to_value(JournalEntry::from_observation(&obs)).unwrap();
    assert_eq!(stored["root_run_id"], "run.root");
    assert_eq!(stored["parent_run_id"], "run.root");
    assert_eq!(stored["event_id"], "run.root-evt-3");
    assert!(stored.get("run_id").is_none());
    let rebuilt = serde_json::from_value::<JournalEntry>(stored)
        .unwrap()
        .into_observation("run.child");
    assert_eq!(rebuilt, obs);
}

#[test]
fn a_v1_entry_that_was_never_migrated_still_reads() {
    let obs = observation("run.a", "run.a", 2, "evt-2".to_owned());
    let v1 = serde_json::to_value(&obs).unwrap();
    let rebuilt = serde_json::from_value::<JournalEntry>(v1)
        .unwrap()
        .into_observation("run.a");
    assert_eq!(rebuilt, obs);
}
