use super::*;
use tinyagents_session::transcript::{
    TranscriptMediaRef, TranscriptMessage, TranscriptPart, TranscriptToolCall,
};
use tinyinference_llm::message::{ContentBlock, MediaRef, Message, MessageOrigin, UserMessage};

/// Native projection retains the assistant/tool pair and correlation fallback.
#[test]
fn native_tool_round_preserves_calls_and_tool_result_identity() {
    let row = TranscriptMessage::assistant_with_calls(
        "running",
        vec![TranscriptToolCall {
            id: "call-1".into(),
            name: "read".into(),
            arguments: "{\"path\":\"a\"}".into(),
            extra_content: None,
        }],
    );
    let message = transcript_to_message(&row);
    let native = message_to_native_transcript(&message).unwrap();
    assert_eq!(native.tool_calls, row.tool_calls);
    assert!(
        message_to_display_transcript(&message)
            .unwrap()
            .tool_calls
            .is_empty()
    );
    let mut tool = TranscriptMessage::tool("result");
    tool.id = Some("call-1".into());
    let message = transcript_to_message(&tool);
    let native = message_to_native_transcript(&message).unwrap();
    assert_eq!(native.tool_call_id.as_deref(), Some("call-1"));
    assert_eq!(native.id.as_deref(), Some("call-1"));
}

/// Execution parsing marks malformed arguments without mutating the raw row.
#[test]
fn malformed_arguments_are_invalid_without_changing_the_source_row() {
    let row = TranscriptMessage::assistant_with_calls(
        "",
        vec![TranscriptToolCall {
            id: "call".into(),
            name: "read".into(),
            arguments: "{unfinished".into(),
            extra_content: Some(serde_json::json!({"provider": "raw"})),
        }],
    );
    let Message::Assistant(message) = transcript_to_message(&row) else {
        panic!("assistant");
    };
    assert!(message.tool_calls[0].is_invalid());
    assert_eq!(
        message.tool_calls[0].arguments,
        serde_json::Value::String("{unfinished".into())
    );
    assert_eq!(row.tool_calls[0].arguments, "{unfinished");
    assert_eq!(
        row.tool_calls[0].extra_content,
        Some(serde_json::json!({"provider": "raw"}))
    );
}

#[test]
fn blank_arguments_project_to_an_empty_object() {
    let row = TranscriptMessage::assistant_with_calls(
        "",
        vec![TranscriptToolCall {
            id: "call".into(),
            name: "read".into(),
            arguments: "  \n".into(),
            extra_content: None,
        }],
    );
    let Message::Assistant(message) = transcript_to_message(&row) else {
        panic!("assistant");
    };
    assert_eq!(message.tool_calls[0].arguments, serde_json::json!({}));
    assert!(!message.tool_calls[0].is_invalid());
}

/// Typed media retain their original order and references without reading files.
#[test]
fn ordered_user_parts_round_trip_as_references() {
    let row = TranscriptMessage::user_with_parts(vec![
        TranscriptPart::Text {
            text: "first".into(),
        },
        TranscriptPart::Image {
            url: "data:image/png;base64,AA==".into(),
        },
        TranscriptPart::Audio {
            source: TranscriptMediaRef::Path {
                path: "clip.wav".into(),
            },
            mime_type: "audio/wav".into(),
        },
        TranscriptPart::Video {
            source: TranscriptMediaRef::Url {
                url: "https://example.invalid/movie".into(),
            },
            mime_type: "video/mp4".into(),
        },
        TranscriptPart::Document {
            source: TranscriptMediaRef::Path {
                path: "guide.pdf".into(),
            },
            mime_type: "application/pdf".into(),
        },
        TranscriptPart::Text {
            text: "last".into(),
        },
    ]);
    let message = transcript_to_message(&row);
    let native = message_to_native_transcript(&message).unwrap();
    assert_eq!(native.parts, row.parts);
    let Message::User(user) = message else {
        panic!("user");
    };
    let ContentBlock::Image(image) = &user.content[1] else {
        panic!("image");
    };
    assert_eq!(image.mime_type.as_deref(), Some("image/png"));
}

/// Provider-prepared bytes are ephemeral; only paths and URLs enter durable parts.
#[test]
fn native_media_projection_omits_inline_provider_bytes() {
    let message = Message::User(UserMessage {
        content: vec![
            ContentBlock::Text("read".into()),
            ContentBlock::Document(MediaRef::Base64 {
                data: "private-inline-data".into(),
                media_type: "application/pdf".into(),
            }),
            ContentBlock::Audio(MediaRef::Path {
                path: "sound".into(),
                media_type: None,
            }),
        ],
    });
    let native = message_to_native_transcript(&message).unwrap();
    assert_eq!(
        native.parts.unwrap(),
        vec![
            TranscriptPart::Text {
                text: "read".into()
            },
            TranscriptPart::Audio {
                source: TranscriptMediaRef::Path {
                    path: "sound".into()
                },
                mime_type: "application/octet-stream".into()
            },
        ]
    );
}

/// Thinking replay preserves byte boundaries and all distinct reasoning spans.
#[test]
fn reasoning_replays_verbatim_and_retains_multiple_spans() {
    let mut row = TranscriptMessage::assistant("answer");
    row.extra_metadata = Some(serde_json::json!({"reasoning_content": "  thought\n"}));
    let message = transcript_to_message(&row);
    let native = message_to_native_transcript(&message).unwrap();
    assert_eq!(native.extra_metadata, row.extra_metadata);
    let content = vec![
        ContentBlock::Thinking {
            text: " first ".into(),
            signature: None,
        },
        ContentBlock::ProviderExtension(serde_json::json!({"reasoning_content": " first "})),
        ContentBlock::Thinking {
            text: "second".into(),
            signature: None,
        },
    ];
    assert_eq!(
        reasoning_from_content(&content).as_deref(),
        Some(" first \n\nsecond")
    );
    let repeated = vec![
        ContentBlock::Thinking {
            text: "repeat".into(),
            signature: None,
        },
        ContentBlock::Thinking {
            text: "repeat".into(),
            signature: None,
        },
    ];
    assert_eq!(
        reasoning_from_content(&repeated).as_deref(),
        Some("repeat\n\nrepeat")
    );
    assert!(reasoning_content_block(Some(" \n ")).is_none());
}

#[test]
fn legacy_reasoning_replays_after_assistant_text() {
    let mut row = TranscriptMessage::assistant("checking");
    row.extra_metadata = Some(serde_json::json!({"reasoning_content": "plan the calls"}));

    let Message::Assistant(assistant) = transcript_to_message(&row) else {
        panic!("assistant");
    };
    assert_eq!(
        assistant.content,
        vec![
            ContentBlock::Text("checking".into()),
            ContentBlock::Thinking {
                text: "plan the calls".into(),
                signature: None,
            },
        ],
        "legacy rows must preserve the pre-migration text-then-reasoning provider view"
    );
}

#[test]
fn legacy_empty_assistant_rows_keep_the_empty_text_block() {
    let row = TranscriptMessage::assistant("");

    let Message::Assistant(assistant) = transcript_to_message(&row) else {
        panic!("assistant");
    };
    assert_eq!(assistant.content, vec![ContentBlock::Text(String::new())]);
}

#[test]
fn signed_thinking_blocks_survive_native_replay() {
    let origin = MessageOrigin {
        provider: "anthropic".into(),
        api: "messages".into(),
        model: "claude-sonnet".into(),
    };
    let message = Message::Assistant(tinyinference_llm::message::AssistantMessage {
        id: None,
        content: vec![
            ContentBlock::Thinking {
                text: "plan".into(),
                signature: Some("signed-by-provider".into()),
            },
            ContentBlock::Text("answer".into()),
        ],
        tool_calls: Vec::new(),
        usage: None,
        origin: Some(origin),
    });
    let row = message_to_native_transcript(&message).unwrap();
    let replay = transcript_to_message(&row);
    assert_eq!(replay, message);
}

#[test]
fn reasoning_only_replay_does_not_add_an_empty_text_block() {
    let mut row = TranscriptMessage::assistant("");
    row.extra_metadata = Some(serde_json::json!({
        "reasoning_content": "plan",
        "reasoning_blocks": [{"text": "plan", "signature": "signed"}],
    }));
    let Message::Assistant(assistant) = transcript_to_message(&row) else {
        panic!("assistant");
    };
    assert_eq!(assistant.content.len(), 1);
    assert!(matches!(
        assistant.content[0],
        ContentBlock::Thinking { .. }
    ));
}

/// Marker lifting is a host compatibility policy, not this typed conversion core.
#[test]
fn untyped_user_text_is_preserved_without_interpreting_legacy_markers() {
    let row = TranscriptMessage::user("literal [IMAGE:old-path] and [FILE:file]");
    assert_eq!(transcript_to_message(&row).text(), row.content);
    let row = TranscriptMessage::user_with_parts(Vec::new());
    let Message::User(user) = transcript_to_message(&row) else {
        panic!("user");
    };
    assert_eq!(user.content.len(), 1);
    assert!(matches!(&user.content[0], ContentBlock::Text(text) if text.is_empty()));
}

/// Out-of-band host records must never leak into a later provider request.
#[test]
fn custom_records_are_omitted_from_both_execution_projections() {
    let custom = Message::Custom(tinyinference_llm::message::CustomMessage {
        kind: "audit".into(),
        payload: serde_json::json!({"private": true}),
        display: Some("note".into()),
    });
    assert!(message_to_display_transcript(&custom).is_none());
    assert!(message_to_native_transcript(&custom).is_none());
    let mut row = TranscriptMessage::user("inbound");
    row.role = "unknown".into();
    assert!(matches!(transcript_to_message(&row), Message::User(_)));
}

/// Native replay keeps a complete declaration/result pair correlated after projection.
#[test]
fn complete_native_tool_round_remains_valid_after_projection_and_replay() {
    let Message::Assistant(mut assistant) = Message::assistant("") else {
        unreachable!()
    };
    assistant.tool_calls.push(TaToolCall::new(
        "call-1",
        "read",
        serde_json::json!({"path":"a"}),
    ));
    let history = vec![
        Message::Assistant(assistant),
        Message::tool("call-1", "result"),
    ];
    let rows: Vec<_> = history
        .iter()
        .filter_map(message_to_native_transcript)
        .collect();
    let replay: Vec<_> = rows.iter().map(transcript_to_message).collect();
    let Message::Assistant(assistant) = &replay[0] else {
        panic!("assistant declaration");
    };
    let Message::Tool(tool) = &replay[1] else {
        panic!("tool result");
    };
    assert_eq!(assistant.tool_calls.len(), 1);
    assert_eq!(assistant.tool_calls[0].id, tool.tool_call_id);
    assert_eq!(
        assistant.tool_calls[0].arguments,
        serde_json::json!({"path":"a"})
    );
    assert_eq!(replay, history);
}

#[test]
fn native_history_decode_rejects_display_rows_and_unanswered_or_duplicate_results() {
    let Message::Assistant(mut assistant) = Message::assistant("") else {
        unreachable!()
    };
    assistant.tool_calls = vec![TaToolCall::new("call-1", "read", serde_json::json!({}))];
    let messages = vec![
        Message::Assistant(assistant),
        Message::tool("call-1", "result"),
    ];
    let native: Vec<_> = messages
        .iter()
        .filter_map(message_to_native_transcript)
        .collect();
    assert_eq!(native_transcript_to_messages(&native).unwrap(), messages);
    let display: Vec<_> = messages
        .iter()
        .filter_map(message_to_display_transcript)
        .collect();
    assert!(
        native_transcript_to_messages(&display)
            .unwrap_err()
            .to_string()
            .contains("orphan")
    );
    assert!(
        native_transcript_to_messages(&native[..1])
            .unwrap_err()
            .to_string()
            .contains("unanswered")
    );
    let mut duplicate = native.clone();
    duplicate.push(native[1].clone());
    assert!(
        native_transcript_to_messages(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("orphan")
    );
    let mut interrupted = native.clone();
    interrupted.insert(1, TranscriptMessage::user("interrupt"));
    assert!(native_transcript_to_messages(&interrupted).is_err());
}

#[test]
fn legacy_image_markers_lift_in_order_and_keep_malformed_markers_as_text() {
    let png = "data:image/png;base64,AA==";
    let jpeg = "https://example.test/image.jpg";
    let original = format!(
        "before [IMAGE:{png}] between [IMAGE:{jpeg}] after [IMAGE:] broken [IMAGE:unterminated"
    );
    let row = TranscriptMessage::user(original.clone());
    let message = transcript_to_message(&row);

    let Message::User(user) = message else {
        panic!("user row");
    };
    assert_eq!(
        user.content,
        vec![
            ContentBlock::Text("before".into()),
            ContentBlock::Image(ImageRef {
                url: png.into(),
                mime_type: Some("image/png".into()),
            }),
            ContentBlock::Text("between".into()),
            ContentBlock::Image(ImageRef {
                url: jpeg.into(),
                mime_type: None,
            }),
            ContentBlock::Text("after [IMAGE:] broken [IMAGE:unterminated".into()),
        ]
    );
    assert_eq!(row.content, original);
}

#[test]
fn legacy_user_text_parser_preserves_prose_whitespace_around_images() {
    let source = " before  [IMAGE:data:image/png;base64,AA==]  between [IMAGE:] after ";
    let message = user_message_from_text(source);
    let Message::User(user) = message else {
        panic!("user message");
    };
    assert_eq!(
        user.content,
        vec![
            ContentBlock::Text(" before  ".into()),
            ContentBlock::Image(ImageRef {
                url: "data:image/png;base64,AA==".into(),
                mime_type: Some("image/png".into()),
            }),
            ContentBlock::Text("  between [IMAGE:] after ".into()),
        ]
    );
}

#[test]
fn history_helpers_preserve_last_user_and_explicit_request_boundaries() {
    let messages = vec![
        Message::assistant("prior"),
        Message::user("current"),
        Message::assistant("tool prelude"),
        Message::tool("call-1", "result"),
        Message::user("steer"),
        Message::assistant("answer"),
    ];

    assert_eq!(messages_since_last_user(&messages), &[messages[5].clone()]);
    assert_eq!(
        messages_since_last_user(&messages[..1]),
        &[messages[0].clone()]
    );
    assert!(messages_since_last_user(&[]).is_empty());
    assert_eq!(
        messages_since_request(&messages, 2),
        &messages[2..],
        "the boundary retains assistant/tool/steer messages in order"
    );
    assert!(messages_since_request(&messages, usize::MAX).is_empty());
    assert!(messages_since_request(&[], 0).is_empty());
}

#[test]
fn conversation_projection_groups_tool_results_and_keeps_typed_calls() {
    let Message::Assistant(mut assistant) = Message::assistant("calling") else {
        unreachable!()
    };
    assistant.tool_calls = vec![TaToolCall::new(
        "call-1",
        "lookup",
        serde_json::json!({"q": "x"}),
    )];
    let messages = vec![
        Message::user("go"),
        Message::Assistant(assistant),
        Message::tool("call-1", "first"),
        Message::tool("call-2", "second"),
        Message::Custom(tinyinference_llm::message::CustomMessage::new("note", "hidden")),
        Message::assistant("done"),
    ];

    let entries = messages_to_conversation(&messages);
    assert_eq!(entries.len(), 4);
    assert!(matches!(
        &entries[0],
        TranscriptEntry::Chat(chat) if chat.role == DialectRole::User && chat.content == "go"
    ));
    match &entries[1] {
        TranscriptEntry::AssistantToolCalls { text, tool_calls, .. } => {
            assert_eq!(text.as_deref(), Some("calling"));
            assert_eq!(tool_calls.len(), 1);
            assert_eq!(tool_calls[0].name, "lookup");
            assert_eq!(tool_calls[0].arguments, r#"{"q":"x"}"#);
        }
        other => panic!("expected typed tool call, got {other:?}"),
    }
    match &entries[2] {
        TranscriptEntry::ToolResults(results) => {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].tool_call_id, "call-1");
            assert_eq!(results[0].content, "first");
            assert_eq!(results[1].tool_call_id, "call-2");
            assert_eq!(results[1].content, "second");
            assert!(!results[0].trusted_verbatim);
        }
        other => panic!("expected grouped tool results, got {other:?}"),
    }
    assert!(matches!(
        &entries[3],
        TranscriptEntry::Chat(chat) if chat.role == DialectRole::Assistant && chat.content == "done"
    ));
}
