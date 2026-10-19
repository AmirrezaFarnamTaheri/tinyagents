use super::*;
use crate::observability::trace_export::ingestion_otlp::ingestion_batch_otlp_requests;

const BASE: u64 = 1_704_067_200_000;

fn span(id: &str, name: &str, start: u64, end: u64) -> Span {
    Span::new(id, "run-1", name, BASE + start).finish(BASE + end, SpanState::Ok)
}

#[test]
fn builds_nested_three_step_trace_with_payloads_and_usage() {
    let spans = vec![
        Span::new("trace:run:run-1", "run-1", "demo", BASE)
            .finish(BASE + 50, SpanState::Ok)
            .with_io(
                Some(json!({"input":"question"})),
                Some(json!({"output":"answer"})),
            ),
        span("trace:step:1", "step 1", 1, 10).with_parent("trace:run:run-1"),
        Span::new("trace:model:1", "run-1", "model call", BASE + 2)
            .with_parent("trace:step:1")
            .with_step_id("1")
            .with_io(
                Some(json!([{"role":"user","content":"question"}])),
                Some(json!({"content":"thinking"})),
            )
            .with_usage(json!({"input":12,"output":4,"total":16}))
            .finish(BASE + 8, SpanState::Ok),
        span("trace:step:2", "step 2", 11, 25).with_parent("trace:run:run-1"),
        Span::new("trace:tool:2", "run-1", "search", BASE + 12)
            .with_parent("trace:step:2")
            .with_step_id("2")
            .with_io(
                Some(json!({"query":"question"})),
                Some(json!({"result":"found"})),
            )
            .finish(BASE + 20, SpanState::Ok),
        span("trace:step:3", "step 3", 26, 50).with_parent("trace:run:run-1"),
        Span::new("trace:model:3", "run-1", "final answer", BASE + 27)
            .with_parent("trace:step:3")
            .with_step_id("3")
            .with_usage(json!({"input":20,"output":5,"total":25}))
            .finish(BASE + 49, SpanState::Ok),
    ];
    let batch = build_ingestion_batch(
        LangfuseTraceConfig {
            trace_id: Some("trace".to_string()),
            name: Some("demo".to_string()),
            session_id: Some("thread-1".to_string()),
            ..Default::default()
        },
        &spans,
    )
    .unwrap();
    let events = batch["batch"].as_array().unwrap();
    assert_eq!(events.len(), 8);
    assert_eq!(events[0]["type"], "trace-create");
    assert_eq!(events[1]["body"]["parentObservationId"], Value::Null);
    assert_eq!(events[2]["body"]["parentObservationId"], "trace:run:run-1");
    assert_eq!(events[3]["body"]["parentObservationId"], "trace:step:1");
    assert_eq!(events[3]["body"]["input"][0]["content"], "question");
    assert_eq!(events[3]["body"]["usageDetails"]["total"], 16);
    assert_eq!(events[7]["body"]["parentObservationId"], "trace:step:3");
    assert_eq!(events[7]["body"]["startTime"], "2024-01-01T00:00:00.027Z");
}

#[test]
fn preserves_graph_exporter_shape_for_deterministic_three_step_span_batch() {
    let spans = vec![
        Span::new("root-1:run:root-1", "run-1", "demo-graph", BASE + 1_000)
            .with_event_id("evt-0")
            .finish(BASE + 1_090, SpanState::Ok)
            .with_io(None, None),
        span("root-1:step:1", "step 1", 1_010, 1_030)
            .with_parent("root-1:run:root-1")
            .with_event_id("evt-3"),
        span("root-1:node:a:1", "a", 1_015, 1_025)
            .with_parent("root-1:step:1")
            .with_event_id("evt-2"),
        span("root-1:step:2", "step 2", 1_040, 1_060)
            .with_parent("root-1:run:root-1")
            .with_event_id("evt-6"),
        span("root-1:node:b:2", "b", 1_045, 1_055)
            .with_parent("root-1:step:2")
            .with_event_id("evt-5"),
        span("root-1:step:3", "step 3", 1_070, 1_090)
            .with_parent("root-1:run:root-1")
            .with_event_id("evt-9"),
        span("root-1:node:c:3", "c", 1_075, 1_085)
            .with_parent("root-1:step:3")
            .with_event_id("evt-8"),
    ];
    let trace = LangfuseTraceConfig {
        trace_id: Some("root-1".to_string()),
        name: Some("demo-graph".to_string()),
        session_id: Some("thread-1".to_string()),
        metadata: json!({"graph_id":"demo-graph","root_run_id":"root-1","health":{}}),
        ..Default::default()
    };
    let batch = build_ingestion_batch(trace, &spans).unwrap();
    let events = batch["batch"].as_array().unwrap();
    assert_eq!(events[0]["body"]["id"], "root-1");
    assert_eq!(events[0]["body"]["sessionId"], "thread-1");
    assert_eq!(events[1]["id"], "evt-0");
    assert_eq!(events[1]["body"]["id"], "root-1:run:root-1");
    assert_eq!(
        events[2]["body"]["parentObservationId"],
        "root-1:run:root-1"
    );
    assert_eq!(events[3]["body"]["parentObservationId"], "root-1:step:1");
    assert_eq!(events[6]["body"]["id"], "root-1:step:3");
}

#[test]
fn failed_and_open_spans_keep_status_and_timing_shape() {
    let spans = vec![
        Span::new("failed", "run-1", "operation", BASE)
            .finish(BASE + 4, SpanState::Error)
            .with_status_message("failed operation"),
        Span::new("open", "run-1", "running", BASE + 5),
    ];
    let batch = build_ingestion_batch(LangfuseTraceConfig::default(), &spans).unwrap();
    let events = batch["batch"].as_array().unwrap();
    assert_eq!(events[1]["body"]["level"], "ERROR");
    assert_eq!(events[1]["body"]["statusMessage"], "failed operation");
    assert!(events[2]["body"].get("endTime").is_none());
}

#[test]
fn otlp_conversion_keeps_parentage_payload_and_usage() {
    let spans = vec![
        Span::new("root", "run-1", "root", BASE).finish(BASE + 10, SpanState::Ok),
        Span::new("model", "run-1", "model", BASE + 1)
            .with_parent("root")
            .with_io(
                Some(json!({"prompt":"hi"})),
                Some(json!({"answer":"hello"})),
            )
            .with_usage(json!({"input":2,"output":1}))
            .finish(BASE + 9, SpanState::Ok),
    ];
    let batch = build_ingestion_batch(LangfuseTraceConfig::default(), &spans).unwrap();
    let requests =
        ingestion_batch_otlp_requests(&batch, "test-service", "test-service.spans").unwrap();
    let encoded = requests[0].to_string();
    assert!(encoded.contains("langfuse.observation.usage_details"));
    assert!(encoded.contains("langfuse.observation.input"));
    assert!(encoded.contains("langfuse.observation.output"));
    assert!(encoded.contains("parentSpanId"));
}

#[test]
fn rejects_empty_span_batch() {
    assert!(build_ingestion_batch(LangfuseTraceConfig::default(), &[]).is_err());
}
