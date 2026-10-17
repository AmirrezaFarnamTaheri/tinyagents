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
            "timed_out",
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
fn failed_run_preserves_typed_cancellation_and_timeout_outcomes() {
    use crate::terminal::{TerminalOutcome, TerminalReason};

    for (reason, expected_status, expected_outcome) in [
        (TerminalReason::Cancelled, SpanStatus::Unset, "cancelled"),
        (TerminalReason::Timeout, SpanStatus::Error, "timed_out"),
        (TerminalReason::Completed, SpanStatus::Error, "failed"),
    ] {
        let context = TraceContext::new("trace-failed-terminal", None).with_capture_content(false);
        let mut collector = SpanCollector::new(context);
        collector.record_event(
            &AgentEvent::RunStarted {
                run_id: RunId::new("run-failed-terminal"),
                thread_id: None,
            },
            10,
        );
        collector.record_event(
            &AgentEvent::RunFailed {
                run_id: RunId::new("run-failed-terminal"),
                error: "private error payload".into(),
                outcome: Some(TerminalOutcome::new(reason, "private typed message")),
            },
            20,
        );

        let root = &collector.spans()[0];
        assert_eq!(root.status, expected_status, "{reason:?}");
        assert_eq!(
            root.attributes["turn.outcome"], expected_outcome,
            "{reason:?}"
        );
        assert_ne!(root.attributes["turn.outcome"], "completed", "{reason:?}");
        assert_eq!(
            root.attributes["error.message"], "Operation failed",
            "{reason:?}"
        );
        assert_eq!(root.end_unix_ms, Some(20), "{reason:?}");
        if reason == TerminalReason::Cancelled {
            assert_eq!(root.attributes["observation.level"], "WARNING");
        }
    }
}

#[test]
fn failed_timeout_falls_back_to_legacy_error_without_leaking_when_capture_is_off() {
    use crate::terminal::{TerminalOutcome, TerminalReason};

    for (capture_content, expected_error) in [
        (true, "legacy timeout description"),
        (false, "Operation failed"),
    ] {
        let context =
            TraceContext::new("trace-timeout-fallback", None).with_capture_content(capture_content);
        let mut collector = SpanCollector::new(context);
        collector.record_event(
            &AgentEvent::RunStarted {
                run_id: RunId::new("run-timeout-fallback"),
                thread_id: None,
            },
            10,
        );
        collector.record_event(
            &AgentEvent::RunFailed {
                run_id: RunId::new("run-timeout-fallback"),
                error: "legacy timeout description".into(),
                outcome: Some(TerminalOutcome::new(TerminalReason::Timeout, "  \n ")),
            },
            20,
        );

        let root = &collector.spans()[0];
        assert_eq!(root.status, SpanStatus::Error);
        assert_eq!(root.attributes["turn.outcome"], "timed_out");
        assert_eq!(root.attributes["error.message"], expected_error);
        assert_eq!(root.end_unix_ms, Some(20));
    }
}

#[test]
fn failed_child_run_keeps_its_typed_error_on_the_child_span() {
    use crate::terminal::{TerminalOutcome, TerminalReason};

    let mut collector = SpanCollector::new(
        TraceContext::new("trace-child-failure", None).with_capture_content(true),
    );
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        0,
        10,
        AgentEvent::RunStarted {
            run_id: RunId::new("parent"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        0,
        11,
        AgentEvent::RunStarted {
            run_id: RunId::new("child"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        1,
        20,
        AgentEvent::RunFailed {
            run_id: RunId::new("child"),
            error: "legacy child failure".into(),
            outcome: Some(TerminalOutcome::new(
                TerminalReason::ToolFailed,
                "typed child failure",
            )),
        },
    ));

    let spans = collector.spans();
    let parent = spans
        .iter()
        .find(|span| span.kind == SpanKind::Turn)
        .unwrap();
    let child = spans
        .iter()
        .find(|span| span.kind == SpanKind::Subagent)
        .unwrap();
    assert_eq!(parent.status, SpanStatus::Unset);
    assert!(!parent.attributes.contains_key("turn.outcome"));
    assert!(!parent.attributes.contains_key("error.message"));
    assert_eq!(child.status, SpanStatus::Error);
    assert_eq!(child.attributes["turn.outcome"], "failed");
    assert_eq!(child.attributes["error.message"], "typed child failure");
}

#[test]
fn typed_turn_stop_enriches_a_completed_run_without_extending_its_timestamp() {
    use crate::terminal::{TerminalOutcome, TerminalReason};
    use crate::turn_stop::{TurnFailureClass, TurnStop};

    let mut collector =
        SpanCollector::new(TraceContext::new("trace-turn-stop", None).with_capture_content(false));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        0,
        10,
        AgentEvent::RunStarted {
            run_id: RunId::new("run-stop"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        1,
        20,
        AgentEvent::RunCompleted {
            run_id: RunId::new("run-stop"),
            outcome: Some(TerminalOutcome::new(TerminalReason::Completed, "done")),
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        2,
        30,
        AgentEvent::TurnStopped {
            turn: 4,
            stop: TurnStop::breaker_with_cause(
                "secret note must not leak",
                TurnFailureClass::new("uncertain_side_effect"),
                Some("web_answer_tool".into()),
            ),
        },
    ));

    let root = &collector.spans()[0];
    assert_eq!(root.status, SpanStatus::Unset);
    assert_eq!(root.end_unix_ms, Some(20));
    assert_eq!(root.attributes["turn.outcome"], "stopped");
    assert_eq!(root.attributes["turn.stop_kind"], "breaker");
    assert_eq!(root.attributes["turn.stop_turn"], 4);
    assert_eq!(root.attributes["turn.stop_class"], "uncertain_side_effect");
    assert_eq!(root.attributes["turn.stop_operation"], "web_answer_tool");
    assert_eq!(
        root.attributes["observation.status_message"],
        "stopped: breaker uncertain_side_effect on web_answer_tool"
    );
    assert_eq!(
        root.attributes["error.message"],
        "stopped: breaker uncertain_side_effect on web_answer_tool"
    );
    assert_eq!(root.attributes["observation.level"], "WARNING");
    assert!(
        root.attributes
            .values()
            .all(|value| !value.to_string().contains("secret note"))
    );
}

#[test]
fn turn_stop_is_run_scoped_and_replay_ignores_duplicate_offsets() {
    use crate::terminal::{TerminalOutcome, TerminalReason};
    use crate::turn_stop::TurnStop;

    let mut collector = SpanCollector::new(TraceContext::new("trace-run-scoped-stop", None));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        0,
        10,
        AgentEvent::RunStarted {
            run_id: RunId::new("parent"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        1,
        11,
        AgentEvent::ModelStarted {
            call_id: CallId::new("same-call"),
            model: "model".into(),
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        2,
        12,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("same-call"),
            started_at_ms: Some(11),
            usage: None,
            input: None,
            output: None,
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        0,
        13,
        AgentEvent::RunStarted {
            run_id: RunId::new("child"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        1,
        14,
        AgentEvent::ModelStarted {
            call_id: CallId::new("same-call"),
            model: "model".into(),
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        2,
        15,
        AgentEvent::ModelCompleted {
            call_id: CallId::new("same-call"),
            started_at_ms: Some(14),
            usage: None,
            input: None,
            output: None,
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        3,
        20,
        AgentEvent::RunCompleted {
            run_id: RunId::new("parent"),
            outcome: Some(TerminalOutcome::new(TerminalReason::Completed, "done")),
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        4,
        30,
        AgentEvent::TurnStopped {
            turn: 1,
            stop: TurnStop::breaker("private note", |_| "uncertain_side_effect"),
        },
    ));
    collector.record(&observation_in_run(
        "parent",
        None,
        "parent",
        4,
        31,
        AgentEvent::TurnStopped {
            turn: 2,
            stop: TurnStop::wind_down(),
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        3,
        19,
        AgentEvent::TurnStopped {
            turn: 1,
            stop: TurnStop::iteration_cap(),
        },
    ));
    collector.record(&observation_in_run(
        "child",
        Some("parent"),
        "parent",
        4,
        25,
        AgentEvent::RunCompleted {
            run_id: RunId::new("child"),
            outcome: Some(TerminalOutcome::new(TerminalReason::Completed, "done")),
        },
    ));

    let spans = collector.spans();
    let parent = spans
        .iter()
        .find(|span| span.kind == SpanKind::Turn)
        .unwrap();
    let child = spans
        .iter()
        .find(|span| span.kind == SpanKind::Subagent)
        .unwrap();
    assert_eq!(parent.attributes["turn.stop_kind"], "breaker");
    assert_eq!(
        parent.attributes["turn.stop_class"],
        "uncertain_side_effect"
    );
    assert_eq!(parent.attributes["turn.stop_turn"], 1);
    assert_eq!(parent.end_unix_ms, Some(25));
    assert_eq!(child.attributes["turn.stop_kind"], "iteration_cap");
    assert_eq!(child.attributes["turn.stop_turn"], 1);
    assert_eq!(child.end_unix_ms, Some(25));
}

#[test]
fn turn_stop_never_overrides_native_failure_cancellation_or_timeout() {
    use crate::terminal::{TerminalOutcome, TerminalReason};
    use crate::turn_stop::TurnStop;

    for stop_first in [false, true] {
        for (reason, expected_status, expected_outcome) in [
            (TerminalReason::Cancelled, SpanStatus::Unset, "cancelled"),
            (TerminalReason::Timeout, SpanStatus::Error, "timed_out"),
            (TerminalReason::ToolFailed, SpanStatus::Error, "failed"),
        ] {
            let mut collector = SpanCollector::new(TraceContext::new("trace-stop-authority", None));
            collector.record(&observation(
                0,
                10,
                AgentEvent::RunStarted {
                    run_id: RunId::new("run-authority"),
                    thread_id: None,
                },
            ));
            let stop_offset = if stop_first { 1 } else { 2 };
            let terminal_offset = if stop_first { 2 } else { 1 };
            let stop = observation(
                stop_offset,
                if stop_first { 11 } else { 30 },
                AgentEvent::TurnStopped {
                    turn: 2,
                    stop: TurnStop::wind_down(),
                },
            );
            let terminal = if reason == TerminalReason::ToolFailed {
                observation(
                    terminal_offset,
                    20,
                    AgentEvent::RunFailed {
                        run_id: RunId::new("run-authority"),
                        error: "failed".into(),
                        outcome: Some(TerminalOutcome::new(reason, "failed")),
                    },
                )
            } else {
                observation(
                    terminal_offset,
                    20,
                    AgentEvent::RunCompleted {
                        run_id: RunId::new("run-authority"),
                        outcome: Some(TerminalOutcome::new(reason, "terminal")),
                    },
                )
            };
            if stop_first {
                collector.record(&stop);
                collector.record(&terminal);
            } else {
                collector.record(&terminal);
                collector.record(&stop);
            }

            let root = &collector.spans()[0];
            assert_eq!(
                root.status, expected_status,
                "{reason:?}, stop_first={stop_first}"
            );
            assert_eq!(
                root.attributes["turn.outcome"], expected_outcome,
                "{reason:?}, stop_first={stop_first}"
            );
            assert!(!root.attributes.contains_key("turn.stop_kind"));
            assert_eq!(root.end_unix_ms, Some(20));
        }
    }
}

#[test]
fn turn_stop_projection_sanitizes_public_operation_fields() {
    use crate::turn_stop::{TurnFailureClass, TurnStop, TurnStopKind};

    let secret = "private@example.test/secret";
    let stop = TurnStop {
        kind: TurnStopKind::Breaker,
        failure_class: Some(TurnFailureClass::new(secret)),
        operation: Some(secret.into()),
    };
    let event = AgentEvent::TurnStopped { turn: 1, stop };
    let encoded = serde_json::to_string(&event).expect("event serialization");
    assert!(!encoded.contains(secret));

    let mut collector =
        SpanCollector::new(TraceContext::new("trace-stop-scrub", None).with_capture_content(false));
    collector.record(&observation(
        0,
        10,
        AgentEvent::RunCompleted {
            run_id: RunId::new("run-stop-scrub"),
            outcome: None,
        },
    ));
    collector.record(&observation(
        1,
        20,
        match event {
            AgentEvent::TurnStopped { stop, .. } => AgentEvent::TurnStopped { turn: 1, stop },
            _ => unreachable!(),
        },
    ));
    let root = &collector.spans()[0];
    assert_eq!(root.attributes["turn.stop_class"], "unknown");
    assert!(!root.attributes.contains_key("turn.stop_operation"));
    assert!(
        !root
            .attributes
            .values()
            .any(|value| value.to_string().contains(secret))
    );
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
            name: "worker".into(),
            depth: 1,
        },
        1,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "worker".into(),
            depth: 1,
            run_id: RunId::new("a"),
        },
        1,
    );
    collector.record_event(
        &AgentEvent::SubAgentStarted {
            name: "worker".into(),
            depth: 1,
        },
        2,
    );
    collector.record_event(
        &AgentEvent::SubAgentRunStarted {
            name: "worker".into(),
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
    for (run_id, model, expected_cost) in
        [("a", "child-a-model", 0.25), ("b", "child-b-model", 0.75)]
    {
        let child = child_spans
            .iter()
            .find(|span| span.attributes.get("run.id").and_then(Value::as_str) == Some(run_id))
            .unwrap();
        assert_eq!(child.name, "subagent.worker");
        assert_eq!(child.attributes["gen_ai.usage.input_tokens"], 10);
        assert_eq!(child.attributes["gen_ai.usage.cost_usd"], expected_cost);
        let generation = collector
            .spans()
            .iter()
            .find(|span| span.kind == SpanKind::Generation && span.name == format!("llm.{model}"))
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

#[test]
fn cache_hit_completion_keeps_usage_visible_without_recording_provider_spend() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-cache", None));
    let call_id = CallId::new("cached-call");
    collector.record(&observation_in_run(
        "cache-run",
        None,
        "cache-run",
        0,
        1,
        AgentEvent::ModelStarted {
            call_id: call_id.clone(),
            model: "model-x".into(),
        },
    ));
    collector.record(&observation_in_run(
        "cache-run",
        None,
        "cache-run",
        1,
        2,
        AgentEvent::CacheHit {
            call_id: call_id.clone(),
            key: "opaque-cache-key".into(),
        },
    ));
    collector.record(&observation_in_run(
        "cache-run",
        None,
        "cache-run",
        2,
        3,
        AgentEvent::ModelCompleted {
            call_id,
            started_at_ms: Some(1),
            usage: Some(Usage {
                input_tokens: 50,
                output_tokens: 10,
                charged_amount: Some(ChargedAmount::usd_micros(500_000)),
                ..Default::default()
            }),
            input: None,
            output: None,
        },
    ));

    let generation = collector
        .spans()
        .iter()
        .find(|span| span.kind == SpanKind::Generation)
        .unwrap();
    assert_eq!(generation.attributes["gen_ai.cache.hit"], true);
    assert_eq!(generation.attributes["gen_ai.cost.source"], "cached");
    assert_eq!(generation.attributes["gen_ai.usage.input_tokens"], 50);
    assert!(!generation.attributes.contains_key("gen_ai.usage.cost_usd"));
    assert!(
        !collector.spans()[0]
            .attributes
            .contains_key("gen_ai.usage.cost_usd")
    );
}

#[test]
fn usage_from_a_discarded_call_without_model_completion_still_reaches_run_totals() {
    let mut collector = SpanCollector::new(TraceContext::new("trace-discarded", None));
    collector.record(&observation_in_run(
        "discarded-run",
        None,
        "discarded-run",
        0,
        1,
        AgentEvent::RunStarted {
            run_id: RunId::new("discarded-run"),
            thread_id: None,
        },
    ));
    collector.record(&observation_in_run(
        "discarded-run",
        None,
        "discarded-run",
        1,
        2,
        AgentEvent::UsageRecorded {
            usage: Usage {
                input_tokens: 4,
                output_tokens: 3,
                charged_amount: Some(ChargedAmount::usd_micros(25)),
                ..Default::default()
            },
        },
    ));
    let root = &collector.spans()[0];
    assert_eq!(root.attributes["gen_ai.usage.input_tokens"], 4);
    assert_eq!(root.attributes["gen_ai.usage.output_tokens"], 3);
    assert_eq!(root.attributes["gen_ai.usage.cost_usd"], 0.000025);
}
