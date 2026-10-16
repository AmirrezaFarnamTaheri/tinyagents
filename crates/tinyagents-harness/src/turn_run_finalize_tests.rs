use super::*;
use tinyinference_llm::model::ModelResponse;

fn inputs() -> TurnEndInputs<'static> {
    TurnEndInputs {
        max_iterations: 3,
        pause_at_cap: true,
        early_exit: false,
        breaker_halt: None,
        failure_cause: None,
        wrap_up_injected: false,
    }
}

fn capped_run() -> AgentRun {
    AgentRun {
        model_calls: 3,
        final_response: None,
        ..Default::default()
    }
}

#[test]
fn a_run_at_the_cap_without_a_final_response_hit_the_cap() {
    assert!(detect_hit_cap(&capped_run(), &inputs()));
}

#[test]
fn a_run_that_finished_naturally_did_not_hit_the_cap() {
    let run = AgentRun {
        model_calls: 3,
        final_response: Some(ModelResponse::assistant("done")),
        ..Default::default()
    };
    assert!(!detect_hit_cap(&run, &inputs()));
}

#[test]
fn a_run_under_the_cap_did_not_hit_it() {
    let run = AgentRun {
        model_calls: 2,
        ..Default::default()
    };
    assert!(!detect_hit_cap(&run, &inputs()));
}

#[test]
fn the_cap_only_counts_when_the_host_pauses_at_it() {
    let mut i = inputs();
    i.pause_at_cap = false;
    assert!(!detect_hit_cap(&capped_run(), &i));
}

#[test]
fn an_early_exit_or_breaker_halt_wins_over_the_cap() {
    let mut i = inputs();
    i.early_exit = true;
    assert!(!detect_hit_cap(&capped_run(), &i));
    let mut i = inputs();
    i.breaker_halt = Some("halted");
    assert!(!detect_hit_cap(&capped_run(), &i));
}

#[test]
fn an_in_loop_wrap_up_is_a_cap_even_with_a_final_response() {
    let run = AgentRun {
        model_calls: 3,
        final_response: Some(ModelResponse::assistant("concluded")),
        ..Default::default()
    };
    let mut i = inputs();
    i.wrap_up_injected = true;
    assert!(detect_hit_cap(&run, &i));
    assert!(!detect_hit_cap(&run, &inputs()));
}

#[test]
fn classify_reports_truncation_alongside_the_cap() {
    let end = classify_turn_end(&capped_run(), &inputs());
    assert_eq!(
        end,
        TurnEnd {
            hit_cap: true,
            truncated: false
        }
    );
}

#[test]
fn the_breaker_summary_overrides_the_early_exit_question_and_the_reply() {
    let run = AgentRun {
        final_response: Some(ModelResponse::assistant("reply")),
        messages: vec![],
        ..Default::default()
    };
    assert_eq!(
        select_turn_text(&run, None, None),
        run.text().unwrap_or_default()
    );
    assert_eq!(
        select_turn_text(&run, Some("which one?".into()), None),
        "which one?"
    );
    assert_eq!(
        select_turn_text(&run, Some("which one?".into()), Some("stuck on X")),
        "stuck on X"
    );
}

#[test]
fn a_stop_is_reported_only_for_an_early_end() {
    assert!(turn_stop_for(None, false, false, |_| "x").is_none());
    assert!(turn_stop_for(None, false, true, |_| "x").is_some());
    assert!(turn_stop_for(None, true, true, |_| "x").is_some());
}

#[test]
fn typed_breaker_cause_prevents_a_cap_classification() {
    let mut inputs = inputs();
    inputs.pause_at_cap = true;
    inputs.max_iterations = 1;
    inputs.failure_cause = Some(crate::no_progress::ClassifiedStopCause {
        class: "permission".into(),
        operation: "write_file".into(),
        summary: "human-readable note may change".into(),
    });
    let mut run = AgentRun::default();
    run.model_calls = 1;
    assert!(!detect_hit_cap(&run, &inputs));
    let stop = turn_stop_for_with_cause(
        Some("different prose"),
        inputs.failure_cause,
        false,
        true,
        |_| "ignored",
    )
    .expect("typed cause yields a stop");
    assert_eq!(
        stop.status_message(),
        "stopped: breaker permission on write_file"
    );
}

#[test]
fn only_leading_system_messages_count_as_the_frozen_prefix() {
    let messages = vec![
        Message::system("abc"),
        Message::system("de"),
        Message::user("hello"),
        Message::system("late"),
    ];
    assert_eq!(system_segment_bytes(&messages), vec![3, 2]);
    assert!(system_segment_bytes(&[]).is_empty());
}

#[test]
fn logging_provenance_tolerates_an_empty_list() {
    log_compaction_provenance("m", &[]);
}
