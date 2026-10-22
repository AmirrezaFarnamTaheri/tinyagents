use super::*;
use crate::progress::AgentProgress;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Payload {
    thread: String,
    inherited: Option<String>,
    queued_at_ms: u64,
}

impl RequeuePayload for Payload {
    fn requeued(thread_id: &str, queued_at_ms: u64) -> Self {
        Self {
            thread: thread_id.to_owned(),
            queued_at_ms,
            ..Self::default()
        }
    }

    fn adopt(&mut self, running: &Self) {
        if self.inherited.is_none() {
            self.inherited.clone_from(&running.inherited);
        }
    }
}

#[test]
fn requeued_payload_can_inherit_running_turn_metadata() {
    let running = Payload {
        thread: "thread-1".into(),
        inherited: Some("client-context".into()),
        queued_at_ms: 1,
    };
    let mut queued = Payload::requeued("thread-1", 2);

    queued.adopt(&running);

    assert_eq!(queued.thread, "thread-1");
    assert_eq!(queued.queued_at_ms, 2);
    assert_eq!(queued.inherited.as_deref(), Some("client-context"));
}

#[test]
fn work_tracker_emits_balanced_signals_for_overlapping_tool_calls() {
    let mut tracker = WorkTracker::default();
    let started = |call_id: &str| AgentProgress::ToolCallStarted {
        call_id: call_id.to_owned(),
        tool_name: "tool".into(),
        arguments: serde_json::Value::Null,
        iteration: 1,
        display_label: None,
        display_detail: None,
    };

    assert!(matches!(tracker.observe(&started("a")), Some(TurnSignal::WorkStarted)));
    assert!(tracker.observe(&started("b")).is_none());
    assert!(tracker.observe(&tool_completed("a")).is_none());
    assert!(matches!(
        tracker.observe(&tool_completed("b")),
        Some(TurnSignal::WorkFinished)
    ));
}

#[test]
fn failure_classifier_default_preserves_the_legacy_class_and_message() {
    struct Legacy;
    impl FailureClassifier for Legacy {
        fn classify(&self, _error: &str) -> (String, String) {
            ("unavailable".into(), "Try again".into())
        }
    }

    assert_eq!(
        Legacy.classify_failure("provider unavailable"),
        FailureClassification {
            class: "unavailable".into(),
            message: "Try again".into(),
            details: None,
        }
    );
}

fn tool_completed(call_id: &str) -> AgentProgress {
    AgentProgress::ToolCallCompleted {
        call_id: call_id.to_owned(),
        tool_name: "tool".into(),
        success: true,
        output_chars: 0,
        output: String::new(),
        arguments: None,
        elapsed_ms: 0,
        iteration: 1,
        failure: None,
        display_label: None,
        display_detail: None,
        structured: None,
    }
}
