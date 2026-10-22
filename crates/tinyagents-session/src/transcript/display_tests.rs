use super::*;

fn origin() -> BackgroundOrigin {
    BackgroundOrigin {
        idempotency_key: "run-1".into(),
        provenance: serde_json::json!({"kind": "cron"}),
    }
}

#[test]
fn background_provenance_round_trips_through_row_metadata() {
    let row = with_background(TranscriptMessage::assistant("hi"), &origin());
    assert_eq!(background_of(&row), Some(origin()));
    let display = display_message_of(&row);
    assert_eq!(display.background, Some(origin()));
    assert!(display.message.extra_metadata.is_none());
}

#[test]
fn other_metadata_survives_the_lift() {
    let mut row = TranscriptMessage::assistant("hi");
    row.extra_metadata = Some(serde_json::json!({"keep": 1}));
    let display = display_message_of(&with_background(row, &origin()));
    assert_eq!(
        display.message.extra_metadata,
        Some(serde_json::json!({"keep": 1}))
    );
    assert_eq!(display.background, Some(origin()));
}

#[test]
fn non_object_metadata_is_kept_under_value() {
    let mut row = TranscriptMessage::assistant("hi");
    row.extra_metadata = Some(serde_json::json!("raw"));
    let display = display_message_of(&with_background(row, &origin()));
    assert_eq!(
        display.message.extra_metadata,
        Some(serde_json::json!({"value": "raw"}))
    );
}

#[test]
fn a_plain_row_has_no_background() {
    let display = display_message_of(&TranscriptMessage::user("hello"));
    assert!(display.background.is_none());
    assert_eq!(display.message.content, "hello");
    assert!(!display.interrupted);
}

#[test]
fn a_partial_is_an_interrupted_assistant_record() {
    let mut partial = TranscriptPartial::new("half");
    partial.iteration = Some(2);
    partial.reasoning_content = Some("  thinking ".into());
    let display = partial_display_message(&partial, Some("r1"));
    assert!(display.interrupted);
    assert_eq!(display.message.role, "assistant");
    assert_eq!(display.request_id.as_deref(), Some("r1"));
    assert_eq!(display.iteration, Some(2));
    assert_eq!(display.reasoning_content.as_deref(), Some("thinking"));
}

#[test]
fn a_session_projects_one_record_per_message() {
    let session = SessionTranscript {
        meta: crate::transcript::history::seed_meta_for_discovered("a"),
        messages: vec![
            TranscriptMessage::user("q"),
            TranscriptMessage::assistant("a"),
        ],
        tools: None,
    };
    let display = display_from_session(session);
    assert_eq!(display.records.len(), 2);
    assert_eq!(display.meta.agent_name, "a");
}
