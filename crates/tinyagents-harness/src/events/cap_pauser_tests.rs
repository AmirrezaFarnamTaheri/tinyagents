use super::*;

use std::sync::Mutex;

use crate::events::EventSink;
use crate::steering::SteeringPolicy;

const CAP: usize = 3;

fn complete(sink: &EventSink, call_id: &str) {
    sink.emit(AgentEvent::ModelCompleted {
        call_id: call_id.into(),
        started_at_ms: None,
        usage: None,
        input: None,
        output: None,
    });
}

fn pauser_on(sink: &EventSink) -> (SteeringHandle, Arc<CapPauser>) {
    let handle = SteeringHandle::new(SteeringPolicy::allow_all());
    let pauser = CapPauser::new(handle.clone(), CAP, "agent_turn").unwrap();
    sink.subscribe(pauser.clone());
    (handle, pauser)
}

/// Parent completions interleaved with nested runs' completions pause only on
/// the run's own cap, with the crate's id formats.
#[test]
fn parent_completions_interleaved_with_active_children_pause_only_on_the_runs_own_cap() {
    let sink = EventSink::new();
    let (handle, pauser) = pauser_on(&sink);

    for call_id in [
        "agent_turn-model-1",
        "agent_turn-d1-7-model-1",
        "summarizer-d1-8-model-1",
        "agent_turn-model-2",
        "agent_turn-d1-7-model-2",
        "summarizer-d2-9-model-1",
        "agent_turn-d1-7-model-3",
    ] {
        complete(&sink, call_id);
    }
    assert_eq!(
        handle.pending(),
        0,
        "nested completions must not spend the cap"
    );
    assert_eq!(pauser.completed(), 2);

    complete(&sink, "agent_turn-model-3");
    assert_eq!(handle.pending(), 1, "pause exactly at the run's own cap");
}

#[test]
fn malformed_model_call_ids_are_not_the_runs_own() {
    let sink = EventSink::new();
    let (handle, pauser) = pauser_on(&sink);
    for call_id in [
        "agent_turn-model-",
        "agent_turn-model-x",
        "agent_turn-model-1a",
        "agent_turn2-model-1",
        "other-agent_turn-model-1",
    ] {
        complete(&sink, call_id);
    }
    assert_eq!(pauser.completed(), 0);
    assert_eq!(handle.pending(), 0);
}

#[test]
fn recorder_runs_with_counts_before_the_pause_is_sent() {
    let sink = EventSink::new();
    let handle = SteeringHandle::new(SteeringPolicy::allow_all());
    let seen: Arc<Mutex<Vec<(u64, u64, usize)>>> = Arc::default();
    let recorder: PauseRecorder = {
        let seen = Arc::clone(&seen);
        let handle = handle.clone();
        Arc::new(move |n, cap| seen.lock().unwrap().push((n, cap, handle.pending())))
    };
    sink.subscribe(CapPauser::with_recorder(
        handle.clone(),
        2,
        "agent_turn",
        Some(recorder),
    )
    .unwrap());

    complete(&sink, "agent_turn-model-1");
    assert!(seen.lock().unwrap().is_empty());
    complete(&sink, "agent_turn-model-2");

    // Recorded with nothing pending yet: it happens-before the pause command.
    assert_eq!(*seen.lock().unwrap(), vec![(2, 2, 0)]);
    assert_eq!(handle.pending(), 1);
}

#[test]
fn completions_past_the_cap_keep_requesting_pause() {
    let sink = EventSink::new();
    let (handle, _pauser) = pauser_on(&sink);
    for n in 1..=CAP + 1 {
        complete(&sink, &format!("agent_turn-model-{n}"));
    }
    assert_eq!(handle.pending(), 2);
}

#[test]
fn a_zero_cap_or_a_handle_that_refuses_pause_is_rejected() {
    let allow = SteeringHandle::new(SteeringPolicy::allow_all());
    assert!(CapPauser::new(allow, 0, "agent_turn").is_err());
    let refuses = SteeringHandle::new(SteeringPolicy::new());
    assert!(CapPauser::new(refuses, 2, "agent_turn").is_err());
}
