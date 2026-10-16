use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Holder {
    #[serde(with = "history_wire")]
    rows: Vec<TranscriptMessage>,
    #[serde(default, with = "history_wire::option")]
    optional_rows: Option<Vec<TranscriptMessage>>,
}

#[test]
fn reads_known_legacy_rows_and_serializes_them_back_with_the_same_shape() {
    let wire = serde_json::json!({
        "rows": [
            {"role": "system", "content": "rules"},
            {"role": "user", "content": "hello"},
            {
                "role": "assistant",
                "content": "{\"content\":\"checking\",\"tool_calls\":[{\"id\":\"c1\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"x\\\"}\"}]}"
            },
            {
                "role": "tool",
                "content": "{\"tool_call_id\":\"c1\",\"content\":\"found\"}"
            },
            {"role": "assistant", "content": "done"}
        ],
        "optional_rows": null
    });

    let decoded: Holder = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(decoded.rows[0], TranscriptMessage::system("rules"));
    assert_eq!(decoded.rows[1], TranscriptMessage::user("hello"));
    assert_eq!(decoded.rows[2].content, "checking");
    assert_eq!(decoded.rows[2].tool_calls.len(), 1);
    assert_eq!(decoded.rows[2].tool_calls[0].id, "c1");
    assert_eq!(decoded.rows[2].tool_calls[0].name, "lookup");
    assert_eq!(decoded.rows[2].tool_calls[0].arguments, r#"{"q":"x"}"#);
    assert_eq!(decoded.rows[3].tool_call_id.as_deref(), Some("c1"));
    assert_eq!(decoded.rows[3].content, "found");
    assert!(decoded.optional_rows.is_none());

    let encoded = serde_json::to_value(decoded).unwrap();
    assert_eq!(encoded, wire);
}

#[test]
fn writes_only_legacy_role_and_content_fields() {
    let row = TranscriptMessage::tool_result("call-7", "finished")
        .with_id("call-7");
    let holder = Holder {
        rows: vec![row],
        optional_rows: None,
    };

    let wire = serde_json::to_value(holder).unwrap();
    assert_eq!(
        wire["rows"][0],
        serde_json::json!({
            "role": "tool",
            "content": "{\"tool_call_id\":\"call-7\",\"content\":\"finished\"}"
        })
    );
    assert_eq!(wire["rows"][0].as_object().unwrap().len(), 2);
    assert!(wire["optional_rows"].is_null());
}

#[test]
fn preserves_host_image_markers_and_surrounding_text() {
    let wire = serde_json::json!({
        "rows": [{
            "role": "user",
            "content": "before [IMAGE:data:image/png;base64,AA==] between [IMAGE:/tmp/b.png] after"
        }],
        "optional_rows": []
    });
    let decoded: Holder = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(decoded.rows[0].content, wire["rows"][0]["content"]);
    assert!(decoded.rows[0].parts.is_none());
    assert_eq!(decoded.optional_rows, Some(Vec::new()));
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
}

#[test]
fn leaves_malformed_and_unknown_content_as_plain_text() {
    let wire = serde_json::json!({
        "rows": [
            {"role": "assistant", "content": "{not-json"},
            {"role": "user", "content": "look [OH_IMAGE:unterminated"},
            {"role": "user", "content": "[IMAGE:]"}
        ],
        "optional_rows": null
    });
    let decoded: Holder = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(decoded.rows[0].content, "{not-json");
    assert_eq!(decoded.rows[1].content, "look [OH_IMAGE:unterminated");
    assert_eq!(decoded.rows[2].content, "[IMAGE:]");
    assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
}

#[test]
fn rejects_invalid_legacy_rows_instead_of_dropping_them() {
    let wire = serde_json::json!({
        "rows": [
            {"role": "user", "content": "valid"},
            {"role": "assistant", "content": 3}
        ],
        "optional_rows": null
    });
    assert!(serde_json::from_value::<Holder>(wire).is_err());
}
