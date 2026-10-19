use super::*;
use crate::transcript::{TranscriptMessage, repair_orphaned_tool_head};
use std::borrow::Cow;
use tinyagents_harness::tinyinference_llm::message::Message;

#[test]
fn repair_removes_only_the_leading_tool_run_after_system_prefix() {
    let rows = vec![
        TranscriptMessage::system("system"),
        TranscriptMessage::tool("orphan one").with_id("one"),
        TranscriptMessage::tool("orphan two").with_id("two"),
        TranscriptMessage::user("keep"),
        TranscriptMessage::tool("later orphan").with_id("later"),
    ];
    let repaired = repair_orphaned_tool_head(&rows);
    assert!(matches!(repaired, Cow::Owned(_)));
    assert_eq!(repaired.len(), 3);
    assert_eq!(repaired[0].role, "system");
    assert_eq!(repaired[1].content, "keep");
    assert_eq!(repaired[2].tool_call_id.as_deref(), Some("later"));
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[1].tool_call_id.as_deref(), Some("one"));
}

#[test]
fn repair_preserves_nonleading_tool_rows_and_borrows_without_orphans() {
    let rows = vec![
        TranscriptMessage::system("system"),
        TranscriptMessage::user("user"),
        TranscriptMessage::tool("later").with_id("later"),
    ];
    assert!(matches!(repair_orphaned_tool_head(&rows), Cow::Borrowed(_)));
}

#[test]
fn message_repair_matches_transcript_repair_and_preserves_later_orphans() {
    let history = vec![
        Message::system("system"),
        Message::tool("call-one", "orphan one"),
        Message::tool("call-two", "orphan two"),
        Message::user("keep"),
        Message::tool("later", "later orphan"),
    ];
    let repaired = repair_orphaned_tool_head_messages(&history);
    assert!(matches!(repaired, Cow::Owned(_)));
    assert_eq!(repaired.len(), 3);
    assert!(matches!(repaired[0], Message::User(_)));
    assert!(matches!(repaired[1], Message::Tool(_)));
    assert_eq!(history.len(), 5);
}

#[test]
fn message_repair_borrows_history_without_a_leading_orphan() {
    let history = vec![Message::system("system"), Message::user("user")];
    assert!(matches!(repair_orphaned_tool_head_messages(&history), Cow::Borrowed(_)));
}
