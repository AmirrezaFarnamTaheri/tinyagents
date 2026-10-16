use super::*;
use crate::ids::{CallId, EventId, RunId};
use tinyinference_llm::message::MessageDelta;
use tinyinference_llm::usage::{ChargedAmount, Usage};

fn observation(offset: u64, ts_ms: u64, event: AgentEvent) -> AgentObservation {
    AgentObservation {
        event_id: EventId::new(format!("event-{offset}")),
        run_id: RunId::new("run-1"),
        parent_run_id: None,
        root_run_id: RunId::new("run-1"),
        offset,
        ts_ms,
        event,
    }
}

#[test]
fn observations_fold_generation_usage_ttft_and_run_lifecycle() {
    let context = TraceContext::new("trace-1", Some("user-1".into()))
        .with_agent_id("assistant")
        .with_capture_content(true);
    let mut collector = SpanCollector::new(context);
    for obs in [
        observation(0, 1_000, AgentEvent::RunStarted { run_id: RunId::new("run-1"), thread_id: None }),
        observation(1, 1_010, AgentEvent::ModelStarted { call_id: CallId::new("model-1"), model: "vendor/model".into() }),
        observation(2, 1_050, AgentEvent::ModelDelta { run_id: RunId::new("run-1"), call_id: CallId::new("model-1"), delta: MessageDelta::text("hello") }),
        observation(3, 1_200, AgentEvent::ModelCompleted {
            call_id: CallId::new("model-1"), started_at_ms: Some(1_010),
            usage: Some(Usage { input_tokens: 10, output_tokens: 2, cache_read_tokens: 3, cache_creation_tokens: 1, reasoning_tokens: 1, charged_amount: Some(ChargedAmount::usd_micros(250)), ..Default::default() }),
            input: Some(json!([{"role":"user","content":"question"}])),
            output: Some(json!({"role":"assistant","content":"answer"})),
        }),
        observation(4, 1_300, AgentEvent::RunCompleted { run_id: RunId::new("run-1"), outcome: None }),
    ] { collector.record(&obs); }

    let spans = collector.spans();
    assert_eq!(spans.len(), 2);
    let generation = &spans[1];
    assert_eq!(generation.kind, SpanKind::Generation);
    assert_eq!(generation.start_unix_ms, 1_010);
    assert_eq!(generation.end_unix_ms, Some(1_200));
    assert_eq!(generation.attributes["gen_ai.response.time_to_first_token_ms"], 40);
    assert_eq!(generation.attributes["gen_ai.usage.input_tokens"], 10);
    assert_eq!(generation.attributes["gen_ai.usage.cache_creation_tokens"], 1);
    assert_eq!(generation.attributes["gen_ai.cost.source"], "provider_reported");
    assert_eq!(generation.attributes["gen_ai.usage.cost"], 0.00025);
    assert_eq!(generation.input.as_ref().unwrap()[0]["content"], "question");
    assert_eq!(spans[0].status, SpanStatus::Ok);
}

#[test]
fn unstreamed_call_does_not_report_completion_as_ttft_and_open_tool_is_force_closed() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-2", None));
    collector.record_event(&AgentEvent::RunStarted { run_id: RunId::new("run-2"), thread_id: None }, 1_000);
    collector.record_event(&AgentEvent::ModelStarted { call_id: CallId::new("m"), model: "model".into() }, 1_000);
    collector.record_event(&AgentEvent::ModelDelta { run_id: RunId::new("run-2"), call_id: CallId::new("m"), delta: MessageDelta::text("one chunk") }, 1_500);
    collector.record_event(&AgentEvent::ModelCompleted { call_id: CallId::new("m"), started_at_ms: Some(1_000), usage: None, input: None, output: None }, 1_510);
    collector.record_event(&AgentEvent::ToolStarted { call_id: CallId::new("t"), tool_name: "search".into(), input: None, parent_call_id: None }, 1_520);
    collector.finish(1_600);

    let generation = collector.spans().iter().find(|span| span.kind == SpanKind::Generation).unwrap();
    assert_eq!(generation.attributes["gen_ai.response.streamed"], false);
    assert!(!generation.attributes.contains_key("gen_ai.response.time_to_first_token_ms"));
    let tool = collector.spans().iter().find(|span| span.kind == SpanKind::Tool).unwrap();
    assert_eq!(tool.attributes["force_closed"], true);
    assert_eq!(tool.attributes["observation.level"], "WARNING");
}

#[test]
fn failed_calls_keep_error_status_and_payloads_stay_gated() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-3", None));
    collector.record_event(&AgentEvent::RunStarted { run_id: RunId::new("run-3"), thread_id: None }, 1);
    collector.record_event(&AgentEvent::ToolStarted { call_id: CallId::new("t"), tool_name: "secret_tool".into(), input: Some(json!({"secret":"value"})), parent_call_id: None }, 2);
    collector.record_event(&AgentEvent::ToolFailed { call_id: CallId::new("t"), tool_name: "secret_tool".into(), started_at_ms: Some(2), duration_ms: Some(5), error: "sensitive error".into(), parent_call_id: None }, 7);
    let tool = collector.spans().iter().find(|span| span.kind == SpanKind::Tool).unwrap();
    assert_eq!(tool.status, SpanStatus::Error);
    assert_eq!(tool.input, None);
    assert_eq!(tool.attributes["error.message"], "Operation failed");
}
