use super::*;
use crate::context::{RunConfig, RunContext};
use crate::middleware::Middleware;
use tinyinference_llm::message::{AssistantMessage, ContentBlock};
use tinyinference_llm::model::{ModelRequest, ModelResponse};
use tinyinference_llm::tool::ToolCall;
use tinyinference_llm::usage::Usage;

#[test]
fn accepted_prefix_is_clamped_after_the_seeded_input() {
    let snapshot = TranscriptSnapshot {
        messages: vec![Message::user("seed"), Message::assistant("reply")],
        accepted_len: 0,
        request_base_len: 1,
        ..TranscriptSnapshot::default()
    };
    assert_eq!(snapshot.accepted_end(), 1);

    let past_end = TranscriptSnapshot {
        accepted_len: usize::MAX,
        ..snapshot
    };
    assert_eq!(past_end.accepted_end(), 2);
}

#[test]
fn unanswered_steps_are_plain_text_bounded_and_skip_custom_records() {
    let mut assistant = AssistantMessage {
        id: None,
        content: vec![ContentBlock::Text("say hi".into())],
        tool_calls: vec![ToolCall::new(
            "call-1",
            "search",
            serde_json::json!({"q":"abcdef"}),
        )],
        usage: None,
        origin: None,
    };
    assistant
        .content
        .push(ContentBlock::Text("trailing".into()));
    let messages = vec![
        Message::Assistant(assistant),
        Message::Custom(tinyinference_llm::message::CustomMessage {
            kind: "opaque".into(),
            payload: serde_json::json!({"opaque": true}),
            display: None,
        }),
    ];
    let rendered = render_unanswered_steps(&messages, 4).expect("rendered steps");
    assert!(rendered.contains("called `search` with"));
    assert!(rendered.contains("{\"q\"…"));
    assert!(!rendered.contains("opaque"));
    assert_eq!(render_unanswered_steps(&[], 4), None);
}

#[tokio::test]
async fn snapshot_captures_arguments_success_and_final_tool_text() {
    let sink = new_transcript_snapshot_sink();
    let middleware = TranscriptSnapshotMiddleware::new(Arc::clone(&sink));
    let mut ctx = RunContext::new(RunConfig::new("snapshot-test"), ());
    let args = serde_json::json!({"query": "weather"});
    let mut call = ToolCall::new("call-1", "search", args.clone());
    middleware
        .before_tool(&mut ctx, &(), &mut call)
        .await
        .unwrap();
    let invocation = crate::middleware::ToolInvocationIdentity::new("call-1", "search");
    let mut result = ToolResult::success("final answer");
    middleware
        .after_tool(&mut ctx, &(), &invocation, &mut result)
        .await
        .unwrap();

    let snapshot = sink.lock().unwrap();
    assert_eq!(snapshot.tool_outcomes.len(), 1);
    let outcome = &snapshot.tool_outcomes[0];
    assert_eq!(outcome.call_id, "call-1");
    assert_eq!(outcome.name, "search");
    assert_eq!(outcome.arguments, args);
    assert!(outcome.success);
    assert_eq!(outcome.content, "final answer");
    assert!(
        snapshot
            .messages
            .last()
            .unwrap()
            .text()
            .contains("final answer")
    );
}

#[tokio::test]
async fn snapshot_marks_dispatched_requests_and_excludes_cache_replay_from_spend() {
    let sink = new_transcript_snapshot_sink();
    let outcomes = crate::turn_outcome::new_tool_outcome_sink();
    let middleware = TranscriptSnapshotMiddleware::new(Arc::clone(&sink))
        .with_tool_outcome_sink(Arc::clone(&outcomes));
    let mut ctx = RunContext::new(RunConfig::new("snapshot-rounds"), ());

    let mut request = ModelRequest {
        messages: vec![Message::user("seed"), Message::assistant("first turn")],
        ..ModelRequest::default()
    };
    middleware
        .before_model_dispatch(&mut ctx, &(), &mut request)
        .await
        .unwrap();
    let mut first = ModelResponse::assistant("round one");
    first.usage = Some(Usage {
        input_tokens: 10,
        output_tokens: 3,
        cache_read_tokens: 2,
        ..Usage::default()
    });
    middleware
        .after_model(&mut ctx, &(), &mut first)
        .await
        .unwrap();

    // The next provider request includes the completed response and is the
    // boundary later associated with the tool result.
    let mut second_request = ModelRequest {
        messages: vec![
            Message::user("seed"),
            Message::assistant("first turn"),
            Message::assistant("round one"),
        ],
        ..ModelRequest::default()
    };
    middleware
        .before_model_dispatch(&mut ctx, &(), &mut second_request)
        .await
        .unwrap();
    let mut replay = ModelResponse::assistant("round two");
    replay.served_from_cache = true;
    replay.usage = Some(Usage {
        input_tokens: 10,
        output_tokens: 4,
        cache_read_tokens: 10,
        ..Usage::default()
    });
    middleware
        .after_model(&mut ctx, &(), &mut replay)
        .await
        .unwrap();

    let snapshot = sink.lock().unwrap();
    assert_eq!(snapshot.accepted_len, 3);
    assert_eq!(snapshot.request_base_len, 0);
    assert_eq!(snapshot.accepted_end(), 3);
    assert_eq!(snapshot.model_calls, 2);
    assert_eq!(snapshot.input_tokens, 10);
    assert_eq!(snapshot.output_tokens, 3);
    assert_eq!(snapshot.cached_input_tokens, 2);
    assert_eq!(snapshot.last_call_input_tokens, 10);
    assert_eq!(snapshot.last_call_output_tokens, 4);
    assert_eq!(snapshot.messages.len(), 4);
    drop(snapshot);
    assert!(outcomes.lock().unwrap().is_empty());
}
