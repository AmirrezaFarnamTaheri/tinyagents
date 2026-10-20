//! Persisted snapshot shape is a wire/storage contract: pin it as literal JSON.

use serde_json::json;

use super::types::{TurnLifecycle, TurnState};

#[test]
fn started_snapshot_serializes_to_the_documented_camel_case_shape() {
    let mut state = TurnState::started("t1", "r1", 25, "2026-05-04T10:00:00Z");
    state.lifecycle = TurnLifecycle::Streaming;
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        json!({
            "threadId": "t1",
            "requestId": "r1",
            "lifecycle": "streaming",
            "iteration": 0,
            "maxIterations": 25,
            "streamingText": "",
            "thinking": "",
            "toolTimeline": [],
            "startedAt": "2026-05-04T10:00:00Z",
            "updatedAt": "2026-05-04T10:00:00Z"
        })
    );
}

#[test]
fn snapshot_written_before_optional_fields_still_loads() {
    let raw = json!({
        "threadId": "t1", "requestId": "r1", "lifecycle": "interrupted",
        "iteration": 2, "maxIterations": 10, "streamingText": "x", "thinking": "",
        "toolTimeline": [], "startedAt": "a", "updatedAt": "b"
    });
    let state: TurnState = serde_json::from_value(raw).unwrap();
    assert_eq!(state.lifecycle, TurnLifecycle::Interrupted);
    assert!(state.transcript.is_empty());
}

#[test]
fn turn_state_json_schema_tracks_the_serialized_wire_names() {
    let schema = serde_json::to_value(schemars::schema_for!(TurnState)).unwrap();
    let properties = schema["properties"].as_object().unwrap();
    for field in [
        "threadId",
        "requestId",
        "maxIterations",
        "toolTimeline",
        "transcript",
        "startedAt",
        "updatedAt",
    ] {
        assert!(properties.contains_key(field), "schema is missing {field}");
    }
    assert!(
        schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "threadId")
    );
}
