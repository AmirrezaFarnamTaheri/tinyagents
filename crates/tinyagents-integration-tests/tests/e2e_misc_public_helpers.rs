use std::thread;
use std::time::Duration;

use serde_json::json;
use tinyagents_harness::limits::{LimitTracker, RunLimits};
use tinyinference_llm::tool::{ToolCall, ToolFormat, ToolSchema};
use tinytools::ToolResult;

#[test]
fn tool_schema_limits_and_ids_cover_public_helpers() {
    let schema = ToolSchema::new(
        "make",
        "make a value",
        json!({
            "type": "object",
            "properties": {
                "name": { "type": "string", "enum": ["ok", "fine"] },
                "count": { "type": ["integer", "null"] },
                "items": { "type": "array", "items": { "type": "number" } }
            },
            "required": ["name"],
            "additionalProperties": false
        }),
    )
    .with_format(ToolFormat::Xml);
    assert!(!schema.format.is_json());
    schema
        .validate_call(&ToolCall::new(
            "call-1",
            "make",
            json!({ "name": "ok", "count": 1, "items": [1, 2.5] }),
        ))
        .unwrap();
    assert!(
        schema
            .validate_call(&ToolCall::new("call-2", "other", json!({ "name": "ok" })))
            .unwrap_err()
            .to_string()
            .contains("does not match")
    );
    assert!(
        schema
            .validate_call(&ToolCall::new("call-3", "make", json!({ "count": 1 })))
            .unwrap_err()
            .to_string()
            .contains("required")
    );
    assert!(
        schema
            .validate_call(&ToolCall::new("call-4", "make", json!({ "name": "bad" })))
            .unwrap_err()
            .to_string()
            .contains("enum")
    );
    assert!(
        schema
            .validate_call(&ToolCall::new(
                "call-5",
                "make",
                json!({ "name": "ok", "extra": true })
            ))
            .unwrap_err()
            .to_string()
            .contains("not allowed")
    );
    assert!(
        schema
            .validate_call(&ToolCall::new(
                "call-6",
                "make",
                json!({ "name": "ok", "items": ["x"] })
            ))
            .unwrap_err()
            .to_string()
            .contains("must be number")
    );

    let ok = ToolResult::success("done");
    assert!(!ok.is_error);
    let err = ToolResult::error("bad");
    assert!(err.is_error);

    let limits = RunLimits::default()
        .with_max_model_calls(1)
        .with_max_tool_calls(1)
        .with_max_wall_clock_ms(Some(1))
        .with_max_retries_per_call(3)
        .with_max_depth(9);
    assert_eq!(limits.max_retries_per_call, 3);
    assert_eq!(limits.max_depth, 9);
    let mut tracker = LimitTracker::new(limits);
    assert_eq!(tracker.model_calls(), 0);
    assert_eq!(tracker.tool_calls(), 0);
    assert_eq!(tracker.remaining_model_calls(), 1);
    tracker.record_model_call().unwrap();
    tracker.record_tool_call().unwrap();
    assert!(tracker.record_model_call().is_err());
    assert!(tracker.record_tool_call().is_err());
    assert!(tracker.elapsed() < Duration::from_secs(10));
    assert!(tracker.remaining_wall_clock().is_some());
    thread::sleep(Duration::from_millis(2));
    assert!(tracker.check_wall_clock().is_err());
    assert_eq!(tracker.limits().max_model_calls, 1);

    let first_seq = next_seq();
    assert!(next_seq() > first_seq);
    assert!(new_session_id().as_str().starts_with("session-"));
    assert!(new_cell_id().as_str().starts_with("cell-"));
    assert!(new_call_id().as_str().starts_with("call-"));
}
