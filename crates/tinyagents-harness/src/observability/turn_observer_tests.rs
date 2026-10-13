use super::*;
use crate::ids::{CallId, RunId};

#[derive(Default)]
struct Recorder(Mutex<Vec<TurnObservation>>);

impl Observer for Recorder {
    fn on_event(&self, event: &TurnObservation) {
        self.0.lock().unwrap().push(event.clone());
    }
}

fn started(input: Option<Value>) -> AgentEvent {
    AgentEvent::ToolStarted {
        call_id: CallId::new("c1"),
        tool_name: "shell".into(),
        input,
        parent_call_id: None,
    }
}

fn emit_all(scope: &Arc<ObserverScope>, events: Vec<AgentEvent>) {
    let sink = EventSink::new();
    sink.subscribe(scope.clone());
    for event in events {
        sink.emit(event);
    }
}

#[test]
fn metadata_only_scope_never_exposes_tool_payloads() {
    let rec = Arc::new(Recorder::default());
    let scope = ObserverScope::new(rec.clone(), TraceContent::MetadataOnly);
    emit_all(
        &scope,
        vec![
            AgentEvent::RunStarted {
                run_id: RunId::new("r1"),
                thread_id: None,
            },
            started(Some(serde_json::json!({"secret": 1}))),
        ],
    );
    let seen = rec.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    match &seen[0] {
        TurnObservation::Tool {
            run_id,
            call_id,
            name,
            failed,
            input,
            ..
        } => {
            assert_eq!(run_id.as_deref(), Some("r1"));
            assert_eq!(call_id, "c1");
            assert_eq!(name, "shell");
            assert_eq!(*failed, None);
            assert!(input.is_none());
        }
        other => panic!("unexpected observation {other:?}"),
    }
}

#[test]
fn include_scope_forwards_tool_input() {
    let rec = Arc::new(Recorder::default());
    let scope = ObserverScope::new(rec.clone(), TraceContent::Include);
    emit_all(&scope, vec![started(Some(serde_json::json!({"a": 1})))]);
    let seen = rec.0.lock().unwrap();
    assert!(matches!(
        &seen[0],
        TurnObservation::Tool { input: Some(v), .. } if v == &serde_json::json!({"a": 1})
    ));
}

#[test]
fn events_without_tool_identity_are_ignored() {
    let rec = Arc::new(Recorder::default());
    let scope = ObserverScope::new(rec.clone(), TraceContent::Include);
    emit_all(&scope, vec![AgentEvent::MemoryLoaded]);
    assert!(rec.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scope_is_visible_only_inside_with_observer() {
    assert!(current_scope().is_none());
    let scope = ObserverScope::new(Arc::new(Recorder::default()), TraceContent::default());
    let inside = with_observer(scope, async { current_scope().is_some() }).await;
    assert!(inside);
    assert!(current_scope().is_none());
}

#[tokio::test]
async fn install_attaches_only_for_root_turns_with_a_scope() {
    let scope = ObserverScope::new(Arc::new(Recorder::default()), TraceContent::Include);
    let mut harness: AgentHarness<(), ()> = AgentHarness::new();
    let events = EventSink::new();
    install(&mut harness, &events, true);
    assert!(!harness.policy().capture.tool_io, "no scope, no capture");
    with_observer(scope.clone(), async {
        install(&mut harness, &events, false);
        assert!(!harness.policy().capture.tool_io, "non-root is skipped");
        install(&mut harness, &events, true);
    })
    .await;
    assert!(harness.policy().capture.tool_io);
}
