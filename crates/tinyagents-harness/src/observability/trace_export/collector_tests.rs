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
        observation(
            0,
            1_000,
            AgentEvent::RunStarted {
                run_id: RunId::new("run-1"),
                thread_id: None,
            },
        ),
        observation(
            1,
            1_010,
            AgentEvent::ModelStarted {
                call_id: CallId::new("model-1"),
                model: "vendor/model".into(),
            },
        ),
        observation(
            2,
            1_050,
            AgentEvent::ModelDelta {
                run_id: RunId::new("run-1"),
                call_id: CallId::new("model-1"),
                delta: MessageDelta::text("hello"),
            },
        ),
        observation(
            3,
            1_200,
            AgentEvent::ModelCompleted {
                call_id: CallId::new("model-1"),
                started_at_ms: Some(1_010),
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_tokens: 3,
                    cache_creation_tokens: 1,
                    reasoning_tokens: 1,
                    charged_amount: Some(ChargedAmount::usd_micros(250)),
                    ..Default::default()
                }),
                input: Some(json!([{"role":"user","content":"question"}])),
                output: Some(json!({"role":"assistant","content":"answer"})),
            },
        ),
        observation(
            4,
            1_300,
            AgentEvent::RunCompleted {
                run_id: RunId::new("run-1"),
                outcome: None,
            },
        ),
    ] {
        collector.record(&obs);
    }

    let spans = collector.spans();
    assert_eq!(spans.len(), 2);
    let generation = &spans[1];
    assert_eq!(generation.kind, SpanKind::Generation);
    assert_eq!(generation.start_unix_ms, 1_010);
    assert_eq!(generation.end_unix_ms, Some(1_200));
    assert_eq!(
        generation.attributes["gen_ai.response.time_to_first_token_ms"],
        40
    );
    assert_eq!(generation.attributes["gen_ai.usage.input_tokens"], 10);
    assert_eq!(
        generation.attributes["gen_ai.usage.cache_creation_tokens"],
        1
    );
    assert_eq!(
        generation.attributes["gen_ai.cost.source"],
        "provider_reported"
    );
    assert_eq!(generation.attributes["gen_ai.usage.cost"], 0.00025);
    assert_eq!(generation.input.as_ref().unwrap()[0]["content"], "question");
    assert_eq!(spans[0].status, SpanStatus::Ok);
}

#[test]
fn unstreamed_call_does_not_report_completion_as_ttft_and_open_tool_is_force_closed() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-2", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-2"),
            thread_id: None,
        },
        1_000,
    );
    collector.record_event(
        &AgentEvent::ModelStarted {
            call_id: CallId::new("m"),
            model: "model".into(),
        },
        1_000,
    );
    collector.record_event(
        &AgentEvent::ModelDelta {
            run_id: RunId::new("run-2"),
            call_id: CallId::new("m"),
            delta: MessageDelta::text("one chunk"),
        },
        1_500,
    );
    collector.record_event(
        &AgentEvent::ModelCompleted {
            call_id: CallId::new("m"),
            started_at_ms: Some(1_000),
            usage: None,
            input: None,
            output: None,
        },
        1_510,
    );
    collector.record_event(
        &AgentEvent::ToolStarted {
            call_id: CallId::new("t"),
            tool_name: "search".into(),
            input: None,
            parent_call_id: None,
        },
        1_520,
    );
    collector.finish(1_600);

    let generation = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Generation)
        .unwrap();
    assert_eq!(generation.attributes["gen_ai.response.streamed"], false);
    assert!(
        !generation
            .attributes
            .contains_key("gen_ai.response.time_to_first_token_ms")
    );
    let tool = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Tool)
        .unwrap();
    assert_eq!(tool.attributes["force_closed"], true);
    assert_eq!(tool.attributes["observation.level"], "WARNING");
}

#[test]
fn failed_calls_keep_error_status_and_payloads_stay_gated() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-3", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-3"),
            thread_id: None,
        },
        1,
    );
    collector.record_event(
        &AgentEvent::ToolStarted {
            call_id: CallId::new("t"),
            tool_name: "secret_tool".into(),
            input: Some(json!({"secret":"value"})),
            parent_call_id: None,
        },
        2,
    );
    collector.record_event(
        &AgentEvent::ToolFailed {
            call_id: CallId::new("t"),
            tool_name: "secret_tool".into(),
            started_at_ms: Some(2),
            duration_ms: Some(5),
            error: "sensitive error".into(),
            parent_call_id: None,
        },
        7,
    );
    let tool = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Tool)
        .unwrap();
    assert_eq!(tool.status, SpanStatus::Error);
    assert_eq!(tool.input, None);
    assert_eq!(tool.attributes["error.message"], "Operation failed");
}

#[test]
fn estimated_zero_and_reported_zero_costs_keep_distinct_provenance_without_double_counting() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-cost", None));
    let mut offset = 0;
    let mut emit = |collector: &mut SpanCollector, event| {
        collector.record(&observation(offset, 1_000 + offset, event));
        offset += 1;
    };
    emit(
        &mut collector,
        AgentEvent::RunStarted {
            run_id: RunId::new("run-cost"),
            thread_id: None,
        },
    );
    emit(
        &mut collector,
        AgentEvent::ModelStarted {
            call_id: CallId::new("reported"),
            model: "model-a".into(),
        },
    );
    emit(
        &mut collector,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("reported"),
            started_at_ms: None,
            usage: Some(Usage {
                charged_amount: Some(ChargedAmount::usd_micros(0)),
                ..Default::default()
            }),
            input: None,
            output: None,
        },
    );
    emit(
        &mut collector,
        AgentEvent::CostRecorded {
            cost: crate::cost::CostTotals {
                total_cost: 9.0,
                ..Default::default()
            },
        },
    );
    emit(
        &mut collector,
        AgentEvent::ModelStarted {
            call_id: CallId::new("estimated"),
            model: "model-b".into(),
        },
    );
    emit(
        &mut collector,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("estimated"),
            started_at_ms: None,
            usage: Some(Usage::default()),
            input: None,
            output: None,
        },
    );
    emit(
        &mut collector,
        AgentEvent::CostRecorded {
            cost: crate::cost::CostTotals::default(),
        },
    );

    let generations: Vec<_> = collector
        .spans()
        .iter()
        .filter(|span| span.kind == SpanKind::Generation)
        .collect();
    assert_eq!(generations[0].attributes["gen_ai.usage.cost"], 0.0);
    assert_eq!(
        generations[0].attributes["gen_ai.cost.source"],
        "provider_reported"
    );
    assert_eq!(generations[1].attributes["gen_ai.usage.cost"], 0.0);
    assert_eq!(generations[1].attributes["gen_ai.cost.source"], "estimated");
    assert_eq!(collector.spans()[0].attributes["gen_ai.usage.cost"], 0.0);
}

#[test]
fn observation_replay_ignores_duplicate_offsets_and_other_run_streams() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-offset", None));
    collector.record(&observation(
        4,
        10,
        AgentEvent::RunStarted {
            run_id: RunId::new("run-1"),
            thread_id: None,
        },
    ));
    collector.record(&observation(
        4,
        20,
        AgentEvent::RunFailed {
            run_id: RunId::new("run-1"),
            error: "duplicate".into(),
            outcome: None,
        },
    ));
    let mut other_run = observation(
        5,
        30,
        AgentEvent::RunFailed {
            run_id: RunId::new("run-2"),
            error: "other".into(),
            outcome: None,
        },
    );
    other_run.run_id = RunId::new("run-2");
    collector.record(&other_run);
    assert_eq!(collector.spans().len(), 1);
    assert_eq!(collector.spans()[0].end_unix_ms, None);
    assert_eq!(collector.spans()[0].start_unix_ms, 10);
}

#[test]
fn terminal_outcomes_distinguish_cancellation_stops_and_failures() {
    use crate::terminal::{TerminalOutcome, TerminalReason};

    let outcomes = [
        (
            TerminalOutcome::new(TerminalReason::Cancelled, "user data"),
            SpanStatus::Unset,
            "cancelled",
        ),
        (
            TerminalOutcome::new(TerminalReason::LimitReached(None), "limit"),
            SpanStatus::Unset,
            "stopped",
        ),
        (
            TerminalOutcome::new(TerminalReason::Timeout, "deadline"),
            SpanStatus::Error,
            "failed",
        ),
    ];
    for (outcome, status, label) in outcomes {
        let mut collector = SpanCollector::new(TraceContext::new("trace-terminal", None));
        collector.record_event(
            &AgentEvent::RunStarted {
                run_id: RunId::new("run-terminal"),
                thread_id: None,
            },
            0,
        );
        collector.record_event(
            &AgentEvent::RunCompleted {
                run_id: RunId::new("run-terminal"),
                outcome: Some(outcome),
            },
            5,
        );
        assert_eq!(collector.spans()[0].status, status);
        assert_eq!(collector.spans()[0].attributes["turn.outcome"], label);
        if label == "cancelled" || label == "stopped" {
            assert_eq!(
                collector.spans()[0].attributes["observation.level"],
                "WARNING",
                "{label}"
            );
        }
    }
}

#[test]
fn retry_scheduling_does_not_split_a_call_and_empty_deltas_do_not_set_ttft() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-retry", None));
    collector.record_event(&AgentEvent::RunStarted { run_id: RunId::new("run-retry"), thread_id: None }, 0);
    collector.record_event(&AgentEvent::ModelStarted { call_id: CallId::new("m"), model: "model".into() }, 1_000);
    collector.record_event(&AgentEvent::ModelDelta { run_id: RunId::new("run-retry"), call_id: CallId::new("m"), delta: MessageDelta::default() }, 1_100);
    collector.record_event(&AgentEvent::RetryScheduled { call_id: CallId::new("m"), attempt: 2 }, 1_200);
    collector.record_event(&AgentEvent::ModelDelta { run_id: RunId::new("run-retry"), call_id: CallId::new("m"), delta: MessageDelta::text("answer") }, 1_300);
    collector.record_event(&AgentEvent::ModelCompleted { call_id: CallId::new("m"), started_at_ms: Some(1_000), usage: None, input: None, output: None }, 1_600);
    let generations: Vec<_> = collector.spans().iter().filter(|span| span.kind == SpanKind::Generation).collect();
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0].attributes["gen_ai.response.time_to_first_token_ms"], 300);
}

#[test]
fn late_tool_completion_closes_under_its_original_subagent_after_child_end() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-late", None));
    collector.record_event(&AgentEvent::RunStarted { run_id: RunId::new("run-late"), thread_id: None }, 0);
    collector.record_event(&AgentEvent::SubAgentStarted { name: "research".into(), depth: 1 }, 10);
    collector.record_event(&AgentEvent::ToolStarted { call_id: CallId::new("tool"), tool_name: "lookup".into(), input: None, parent_call_id: None }, 20);
    collector.record_event(&AgentEvent::SubAgentCompleted { name: "research".into(), depth: 1 }, 30);
    collector.record_event(&AgentEvent::ToolCompleted { call_id: CallId::new("tool"), tool_name: "lookup".into(), started_at_ms: Some(20), input: None, output: None, duration_ms: Some(15), output_bytes: None, error: None, metadata: None, parent_call_id: None }, 40);
    let child = collector.spans().iter().find(|span| span.kind == SpanKind::Subagent).unwrap();
    let tool = collector.spans().iter().find(|span| span.kind == SpanKind::Tool).unwrap();
    assert_eq!(child.end_unix_ms, Some(35));
    assert_eq!(tool.end_unix_ms, Some(35));
    assert_eq!(tool.parent_span_id.as_deref(), Some(child.span_id.as_str()));
}

#[test]
fn terminal_events_without_start_events_still_produce_spans() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-partial", None));
    collector.record_event(&AgentEvent::RunStarted { run_id: RunId::new("run-partial"), thread_id: None }, 0);
    collector.record_event(&AgentEvent::ModelCompleted { call_id: CallId::new("missing-start"), started_at_ms: Some(4), usage: None, input: None, output: None }, 9);
    collector.record_event(&AgentEvent::ToolFailed { call_id: CallId::new("missing-tool"), tool_name: "unsafe".into(), started_at_ms: Some(10), duration_ms: Some(2), error: "failed".into(), parent_call_id: None }, 12);
    assert!(collector.spans().iter().any(|span| span.name == "llm.unknown" && span.end_unix_ms == Some(9)));
    assert!(collector.spans().iter().any(|span| span.name == "tool.unsafe" && span.end_unix_ms == Some(12)));
}
