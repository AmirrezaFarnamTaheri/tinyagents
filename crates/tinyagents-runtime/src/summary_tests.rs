use super::*;
use crate::types::{CommitReceipt, SessionTurnOutcome, TranscriptTurnOptions};
use tinyagents_session::transcript::{MessageUsage, TurnUsage};
use tinyinference_llm::message::Message;

fn observation(thread_id: Option<&str>, agent_id: Option<&str>) -> PostCommitObservation {
    PostCommitObservation {
        receipt: CommitReceipt {
            outcome: SessionTurnOutcome {
                history: vec![Message::user("private input")],
                output: Some("private output".into()),
                interrupted: false,
            },
            options: TranscriptTurnOptions {
                request_id: None,
                thread_id: thread_id.map(str::to_owned),
                stream: false,
                resume: crate::ResumeMode::Never,
                context: (),
            },
            transcript: None,
        },
        input: Message::user("private input"),
        usage: Some(TurnUsage {
            provider: "test-provider".into(),
            model: "test-model".into(),
            usage: MessageUsage {
                input: 12,
                output: 5,
                cached_input: 3,
                context_window: 128,
                ..MessageUsage::default()
            },
            ts: String::new(),
            reasoning_content: None,
            tool_calls: Vec::new(),
            iteration: 1,
        }),
        tool_outcomes: vec![tinyagents_harness::turn_outcome::ToolCallOutcome {
            call_id: "call-private".into(),
            name: "private_tool".into(),
            arguments: serde_json::json!({"secret": "do-not-persist"}),
            success: false,
            content: "timeout: private details".into(),
            duration_ms: 23,
        }],
        thread_id: thread_id.map(str::to_owned),
        agent_id: agent_id.map(str::to_owned),
    }
}

#[test]
fn summary_requires_resolved_thread_and_agent_identity() {
    assert!(turn_summary(&observation(None, Some("agent"))).is_none());
    assert!(turn_summary(&observation(Some("  "), Some("agent"))).is_none());
    assert!(turn_summary(&observation(Some("thread"), None)).is_none());
}

#[test]
fn summary_keeps_only_tool_digest_classification_and_captured_usage() {
    use sha2::{Digest, Sha256};
    let summary = turn_summary(&observation(Some("thread-1"), Some("agent-1"))).unwrap();
    let expected = Sha256::digest(
        serde_json::to_vec(&serde_json::json!({
            "secret": "do-not-persist"
        }))
        .unwrap(),
    );
    let expected: String = expected.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(summary.input, "private input");
    assert_eq!(summary.output, "private output");
    assert_eq!(summary.tool_outcomes.len(), 1);
    assert_eq!(summary.tool_outcomes[0].args_digest, expected);
    assert_eq!(
        summary.tool_outcomes[0].failure_class.as_deref(),
        Some("timeout")
    );
    assert_eq!(summary.tool_outcomes[0].duration_ms, 23);
    assert_eq!(summary.usage.input_tokens, 12);
    assert_eq!(summary.usage.output_tokens, 5);
    assert_eq!(summary.usage.total_tokens, 17);
    assert_eq!(summary.usage.cache_read_tokens, 3);
    assert_eq!(summary.usage.context_window_tokens, Some(128));
    let encoded = serde_json::to_string(&summary).unwrap();
    assert!(!encoded.contains("do-not-persist"));
    assert!(!encoded.contains("timeout: private details"));
}

#[test]
fn summary_usage_total_saturates_on_overflow() {
    let mut observation = observation(Some("thread-1"), Some("agent-1"));
    let usage = observation.usage.as_mut().unwrap();
    usage.usage.input = u64::MAX;
    usage.usage.output = 1;
    let summary = turn_summary(&observation).unwrap();
    assert_eq!(summary.usage.total_tokens, u64::MAX);
}

#[test]
fn successful_tool_summary_has_no_failure_class() {
    let mut observation = observation(Some("thread-1"), Some("agent-1"));
    observation.tool_outcomes[0].success = true;
    let summary = turn_summary(&observation).unwrap();
    assert!(summary.tool_outcomes[0].failure_class.is_none());
}
