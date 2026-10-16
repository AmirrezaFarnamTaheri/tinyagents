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

fn observation_in_run(
    run: &str,
    parent: Option<&str>,
    root: &str,
    offset: u64,
    ts_ms: u64,
    event: AgentEvent,
) -> AgentObservation {
    AgentObservation {
        event_id: EventId::new(format!("{run}-{offset}")),
        run_id: RunId::new(run),
        parent_run_id: parent.map(RunId::new),
        root_run_id: RunId::new(root),
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
    assert_eq!(generation.attributes["gen_ai.usage.cost_usd"], 0.00025);
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
    assert_eq!(generations[0].attributes["gen_ai.usage.cost_usd"], 0.0);
    assert_eq!(
        generations[0].attributes["gen_ai.cost.source"],
        "provider_reported"
    );
    assert_eq!(generations[1].attributes["gen_ai.usage.cost_usd"], 0.0);
    assert_eq!(generations[1].attributes["gen_ai.cost.source"], "estimated");
    assert_eq!(
        collector.spans()[0].attributes["gen_ai.usage.cost_usd"],
        0.0
    );
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
    other_run.root_run_id = RunId::new("run-2");
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
fn linked_child_terminal_outcome_survives_parent_lifecycle_completion() {
    use crate::terminal::{TerminalOutcome, TerminalReason};

    let mut collector = SpanCollector::new(TraceContext::new("trace-child-stop", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("parent"),
            thread_id: None,
        },
        0,
    );
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "worker".into(),
            depth: 1,
        },
        1,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "worker".into(),
            depth: 1,
            run_id: RunId::new("child"),
        },
        1,
    );
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        0,
        2,
        AgentEvent::RunCompleted {
            run_id: RunId::new("child"),
            outcome: Some(TerminalOutcome::new(TerminalReason::Cancelled, "cancelled")),
        },
    ));
    collector.record_event(
        &AgentEvent::SubAgentCompleted {
            name: "worker".into(),
            depth: 1,
        },
        3,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunCompleted {
            run_id: RunId::new("child"),
        },
        3,
    );

    let child = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Subagent)
        .unwrap();
    assert_eq!(child.status, SpanStatus::Unset);
    assert_eq!(child.attributes["turn.outcome"], "cancelled");
}

#[test]
fn retry_scheduling_does_not_split_a_call_and_empty_deltas_do_not_set_ttft() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-retry", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-retry"),
            thread_id: None,
        },
        0,
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
            run_id: RunId::new("run-retry"),
            call_id: CallId::new("m"),
            delta: MessageDelta::default(),
        },
        1_100,
    );
    collector.record_event(
        &AgentEvent::RetryScheduled {
            call_id: CallId::new("m"),
            attempt: 2,
        },
        1_200,
    );
    collector.record_event(
        &AgentEvent::ModelDelta {
            run_id: RunId::new("run-retry"),
            call_id: CallId::new("m"),
            delta: MessageDelta::text("answer"),
        },
        1_300,
    );
    collector.record_event(
        &AgentEvent::ModelCompleted {
            call_id: CallId::new("m"),
            started_at_ms: Some(1_000),
            usage: None,
            input: None,
            output: None,
        },
        1_600,
    );
    let generations: Vec<_> = collector
        .spans()
        .iter()
        .filter(|span| span.kind == SpanKind::Generation)
        .collect();
    assert_eq!(generations.len(), 1);
    assert_eq!(
        generations[0].attributes["gen_ai.response.time_to_first_token_ms"],
        300
    );
}

#[test]
fn late_tool_completion_closes_under_its_original_subagent_after_child_end() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-late", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-late"),
            thread_id: None,
        },
        0,
    );
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "research".into(),
            depth: 1,
        },
        10,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "research".into(),
            depth: 1,
            run_id: RunId::new("research-run"),
        },
        10,
    );
    collector.record(&observation_in_run(
        "research-run",
        Some("run-late"),
        "run-late",
        0,
        20,
        AgentEvent::ToolStarted {
            call_id: CallId::new("tool"),
            tool_name: "lookup".into(),
            input: None,
            parent_call_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "research-run",
        Some("run-late"),
        "run-late",
        1,
        30,
        AgentEvent::RunCompleted {
            run_id: RunId::new("research-run"),
            outcome: None,
        },
    ));
    collector.record(&observation_in_run(
        "research-run",
        Some("run-late"),
        "run-late",
        2,
        40,
        AgentEvent::ToolCompleted {
            call_id: CallId::new("tool"),
            tool_name: "lookup".into(),
            started_at_ms: Some(20),
            input: None,
            output: None,
            duration_ms: Some(15),
            output_bytes: None,
            error: None,
            metadata: None,
            parent_call_id: None,
        },
    ));
    let child = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Subagent)
        .unwrap();
    let tool = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Tool)
        .unwrap();
    assert_eq!(child.end_unix_ms, Some(35));
    assert_eq!(tool.end_unix_ms, Some(35));
    assert_eq!(tool.parent_span_id.as_deref(), Some(child.span_id.as_str()));
}

#[test]
fn terminal_events_without_start_events_still_produce_spans() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-partial", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-partial"),
            thread_id: None,
        },
        0,
    );
    collector.record_event(
        &AgentEvent::ModelCompleted {
            call_id: CallId::new("missing-start"),
            started_at_ms: Some(4),
            usage: None,
            input: None,
            output: None,
        },
        9,
    );
    collector.record_event(
        &AgentEvent::ToolFailed {
            call_id: CallId::new("missing-tool"),
            tool_name: "unsafe".into(),
            started_at_ms: Some(10),
            duration_ms: Some(2),
            error: "failed".into(),
            parent_call_id: None,
        },
        12,
    );
    assert!(
        collector
            .spans()
            .iter()
            .any(|span| span.name == "llm.unknown" && span.end_unix_ms == Some(9))
    );
    assert!(
        collector
            .spans()
            .iter()
            .any(|span| span.name == "tool.unsafe" && span.end_unix_ms == Some(12))
    );
}

#[test]
fn own_usage_is_cumulative_while_child_generations_roll_up_only_to_the_child_span() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-rollup", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("run-rollup"),
            thread_id: None,
        },
        0,
    );
    collector.record_event(
        &AgentEvent::ModelStarted {
            call_id: CallId::new("parent-call"),
            model: "parent-model".into(),
        },
        1,
    );
    let own = Usage {
        input_tokens: 3,
        output_tokens: 4,
        charged_amount: Some(ChargedAmount::usd_micros(100)),
        ..Default::default()
    };
    collector.record_event(
        &AgentEvent::ModelCompleted {
            call_id: CallId::new("parent-call"),
            started_at_ms: Some(1),
            usage: Some(own),
            input: None,
            output: None,
        },
        2,
    );
    collector.record_event(&AgentEvent::UsageRecorded { usage: own }, 3);
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "child".into(),
            depth: 1,
        },
        4,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "child".into(),
            depth: 1,
            run_id: RunId::new("child-run"),
        },
        4,
    );
    collector.record(&observation_in_run(
        "child-run",
        Some("run-rollup"),
        "run-rollup",
        0,
        5,
        AgentEvent::ModelStarted {
            call_id: CallId::new("child-call"),
            model: "child-model".into(),
        },
    ));
    collector.record(&observation_in_run(
        "child-run",
        Some("run-rollup"),
        "run-rollup",
        1,
        6,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("child-call"),
            started_at_ms: Some(5),
            usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 2,
                ..Default::default()
            }),
            input: None,
            output: None,
        },
    ));
    collector.record(&observation_in_run(
        "child-run",
        Some("run-rollup"),
        "run-rollup",
        2,
        7,
        AgentEvent::UsageRecorded {
            usage: Usage {
                input_tokens: 10,
                output_tokens: 2,
                ..Default::default()
            },
        },
    ));
    collector.record(&observation_in_run(
        "child-run",
        Some("run-rollup"),
        "run-rollup",
        3,
        8,
        AgentEvent::CostRecorded {
            cost: crate::cost::CostTotals {
                total_cost: 0.25,
                ..Default::default()
            },
        },
    ));

    let root = &collector.spans()[0];
    let child = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Subagent)
        .unwrap();
    assert_eq!(root.attributes["gen_ai.usage.input_tokens"], 3);
    assert_eq!(root.attributes["gen_ai.usage.output_tokens"], 4);
    assert_eq!(root.attributes["gen_ai.usage.cost_usd"], 0.0001);
    assert_eq!(child.attributes["gen_ai.usage.input_tokens"], 10);
    assert_eq!(child.attributes["gen_ai.usage.output_tokens"], 2);
    assert_eq!(child.attributes["gen_ai.usage.cost_usd"], 0.25);
    assert_eq!(child.attributes["gen_ai.cost.source"], "estimated");
}

#[test]
fn concurrent_child_runs_scope_call_ids_and_cost_to_their_exact_lineage() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-parallel", None));
    collector.record_event(
        &AgentEvent::RunStarted {
            run_id: RunId::new("parent"),
            thread_id: None,
        },
        0,
    );
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "child-a".into(),
            depth: 1,
        },
        1,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "child-a".into(),
            depth: 1,
            run_id: RunId::new("a"),
        },
        1,
    );
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "child-b".into(),
            depth: 1,
        },
        2,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "child-b".into(),
            depth: 1,
            run_id: RunId::new("b"),
        },
        2,
    );

    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        0,
        3,
        AgentEvent::ModelStarted {
            call_id: CallId::new("same"),
            model: "parent-model".into(),
        },
    ));
    collector.record(&observation_in_run(
        "a",
        Some("parent"),
        "parent",
        0,
        4,
        AgentEvent::ModelStarted {
            call_id: CallId::new("same"),
            model: "child-a-model".into(),
        },
    ));
    collector.record(&observation_in_run(
        "b",
        Some("parent"),
        "parent",
        0,
        5,
        AgentEvent::ModelStarted {
            call_id: CallId::new("same"),
            model: "child-b-model".into(),
        },
    ));
    for (run, model, cost, at) in [
        ("a", "child-a-model", 0.25, 6),
        ("b", "child-b-model", 0.75, 7),
    ] {
        collector.record(&observation_in_run(
            run,
            Some("parent"),
            "parent",
            1,
            at,
            AgentEvent::ModelCompleted {
                call_id: CallId::new("same"),
                started_at_ms: Some(at - 2),
                usage: Some(Usage {
                    input_tokens: 10,
                    ..Default::default()
                }),
                input: None,
                output: None,
            },
        ));
        collector.record(&observation_in_run(
            run,
            Some("parent"),
            "parent",
            2,
            at + 1,
            AgentEvent::UsageRecorded {
                usage: Usage {
                    input_tokens: 10,
                    ..Default::default()
                },
            },
        ));
        collector.record(&observation_in_run(
            run,
            Some("parent"),
            "parent",
            3,
            at + 2,
            AgentEvent::CostRecorded {
                cost: crate::cost::CostTotals {
                    total_cost: cost,
                    ..Default::default()
                },
            },
        ));
        assert!(
            collector
                .spans()
                .iter()
                .any(|span| span.name == format!("llm.{model}"))
        );
    }

    let parent = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Turn)
        .unwrap();
    assert!(!parent.attributes.contains_key("gen_ai.usage.input_tokens"));
    let child_spans: Vec<_> = collector
        .spans()
        .iter()
        .filter(|span| span.kind == SpanKind::Subagent)
        .collect();
    assert_eq!(child_spans.len(), 2);
    for (name, expected_cost) in [("subagent.child-a", 0.25), ("subagent.child-b", 0.75)] {
        let child = child_spans.iter().find(|span| span.name == name).unwrap();
        assert_eq!(child.attributes["gen_ai.usage.input_tokens"], 10);
        assert_eq!(child.attributes["gen_ai.usage.cost_usd"], expected_cost);
        let generation = collector
            .spans()
            .iter()
            .find(|span| {
                span.kind == SpanKind::Generation
                    && span.name.contains(name.trim_start_matches("subagent."))
            })
            .unwrap();
        assert_eq!(
            generation.parent_span_id.as_deref(),
            Some(child.span_id.as_str())
        );
        assert_eq!(
            generation.attributes["gen_ai.usage.cost_usd"],
            expected_cost
        );
    }
}

#[test]
fn host_enrichment_uses_exact_generation_identity_and_preserves_native_costs() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-enrichment", None));
    collector.record(&observation_in_run(
        "run",
        None,
        "run",
        0,
        1,
        AgentEvent::ModelStarted {
            call_id: CallId::new("priced"),
            model: "model-x".into(),
        },
    ));
    collector.record(&observation_in_run(
        "run",
        None,
        "run",
        1,
        2,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("priced"),
            started_at_ms: Some(1),
            usage: Some(Usage::default()),
            input: None,
            output: None,
        },
    ));
    assert_eq!(
        collector.enrich_generation(
            &RunId::new("missing"),
            &CallId::new("priced"),
            GenerationEnrichment::default(),
        ),
        Err(GenerationEnrichmentError::GenerationNotFound),
    );
    collector
        .enrich_generation(
            &RunId::new("run"),
            &CallId::new("priced"),
            GenerationEnrichment {
                provider_id: Some("host-provider".into()),
                pricing: Some(GenerationPricing {
                    input_per_mtok_usd: Some(0.25),
                    cached_input_per_mtok_usd: None,
                    output_per_mtok_usd: Some(1.0),
                }),
                estimated_cost_usd: Some(0.4),
            },
        )
        .unwrap();
    collector.record(&observation_in_run(
        "run",
        None,
        "run",
        2,
        3,
        AgentEvent::CostRecorded {
            cost: crate::cost::CostTotals {
                total_cost: 0.7,
                ..Default::default()
            },
        },
    ));
    collector
        .enrich_generation(
            &RunId::new("run"),
            &CallId::new("priced"),
            GenerationEnrichment {
                estimated_cost_usd: Some(0.4),
                ..Default::default()
            },
        )
        .unwrap();
    let generation = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Generation)
        .unwrap();
    assert_eq!(generation.attributes["gen_ai.provider"], "host-provider");
    assert_eq!(
        generation.attributes["gen_ai.pricing.input_per_mtok_usd"],
        0.25
    );
    assert_eq!(generation.attributes["gen_ai.usage.cost_usd"], 0.7);
    assert_eq!(
        collector.spans()[0].attributes["gen_ai.usage.cost_usd"],
        0.7
    );

    let mut charged = SpanCollector::new(TraceContext::new("trace-charged", None));
    charged.record(&observation_in_run(
        "charged-run",
        None,
        "charged-run",
        0,
        1,
        AgentEvent::ModelStarted {
            call_id: CallId::new("charged"),
            model: "model-x".into(),
        },
    ));
    charged.record(&observation_in_run(
        "charged-run",
        None,
        "charged-run",
        1,
        2,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("charged"),
            started_at_ms: Some(1),
            usage: Some(Usage {
                charged_amount: Some(ChargedAmount::usd_micros(0)),
                ..Default::default()
            }),
            input: None,
            output: None,
        },
    ));
    charged
        .enrich_generation(
            &RunId::new("charged-run"),
            &CallId::new("charged"),
            GenerationEnrichment {
                estimated_cost_usd: Some(8.0),
                ..Default::default()
            },
        )
        .unwrap();
    let charged_generation = charged
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Generation)
        .unwrap();
    assert_eq!(charged_generation.attributes["gen_ai.usage.cost_usd"], 0.0);
    assert_eq!(
        charged_generation.attributes["gen_ai.cost.source"],
        "provider_reported"
    );

    let mut later_charge = SpanCollector::new(TraceContext::new("trace-later-charge", None));
    later_charge.record(&observation_in_run(
        "later-run",
        None,
        "later-run",
        0,
        1,
        AgentEvent::ModelStarted {
            call_id: CallId::new("later"),
            model: "model-x".into(),
        },
    ));
    later_charge
        .enrich_generation(
            &RunId::new("later-run"),
            &CallId::new("later"),
            GenerationEnrichment {
                estimated_cost_usd: Some(0.4),
                ..Default::default()
            },
        )
        .unwrap();
    later_charge.record(&observation_in_run(
        "later-run",
        None,
        "later-run",
        1,
        2,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("later"),
            started_at_ms: Some(1),
            usage: Some(Usage {
                charged_amount: Some(ChargedAmount::usd_micros(600_000)),
                ..Default::default()
            }),
            input: None,
            output: None,
        },
    ));
    later_charge.record(&observation_in_run(
        "later-run",
        None,
        "later-run",
        2,
        3,
        AgentEvent::UsageRecorded {
            usage: Usage {
                charged_amount: Some(ChargedAmount::usd_micros(600_000)),
                ..Default::default()
            },
        },
    ));
    assert_eq!(
        later_charge.spans()[0].attributes["gen_ai.usage.cost_usd"],
        0.6
    );
}
