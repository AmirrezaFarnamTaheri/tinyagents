use super::*;
use crate::events::{AgentEvent, EventListener, EventSink, RecordingListener};
use crate::ids::RunId;
use tinyinference_llm::{
    model::{ModelRequest, ModelResponse},
    usage::{ChargedAmount, Usage},
};

struct Enricher;

impl SupplementalEnricher for Enricher {
    fn enrich(
        &self,
        model: &str,
        _response: &ModelResponse,
    ) -> (Option<String>, GenerationEnrichment) {
        (
            Some(format!("actual-{model}")),
            GenerationEnrichment {
                provider_id: Some("provider".into()),
                ..Default::default()
            },
        )
    }
}

fn setup() -> (Arc<SupplementalCallTrace>, Arc<RecordingListener>) {
    let events = EventSink::new();
    let listener = Arc::new(RecordingListener::new());
    events.subscribe(listener.clone() as Arc<dyn EventListener>);
    (
        SupplementalCallTrace::new(events, RunId::new("run-1"), Arc::new(Enricher)),
        listener,
    )
}

#[test]
fn completed_supplemental_call_is_correlated_and_enriched_once() {
    let (trace, listener) = setup();
    let mut request =
        ModelRequest::new(vec![tinyinference_llm::message::Message::user("question")]);
    let mut call = SupplementalCallGuard::start(&trace, "selected", &mut request);
    let correlation = request.correlation.expect("correlation is attached");
    assert_eq!(correlation.run_id, "run-1");
    assert!(correlation.call_id.ends_with("-host-supplemental-1"));
    call.complete(&ModelResponse::assistant("answer"));
    drop(call);

    let events = listener
        .events()
        .into_iter()
        .map(|record| record.event)
        .collect::<Vec<_>>();
    assert!(matches!(
        &events[..],
        [
            AgentEvent::ModelStarted { .. },
            AgentEvent::ModelCompleted { .. },
            AgentEvent::GenerationEnriched { .. }
        ]
    ));
    assert_eq!(trace.actual_model().as_deref(), Some("actual-selected"));
    assert!(!trace.is_cache_hit());
}

#[test]
fn dropping_incomplete_supplemental_call_emits_one_failure() {
    let (trace, listener) = setup();
    let mut request = ModelRequest::default();
    drop(SupplementalCallGuard::start(
        &trace,
        "selected",
        &mut request,
    ));
    let events = listener.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].event, AgentEvent::ModelStarted { .. }));
    assert!(matches!(events[1].event, AgentEvent::ModelFailed { .. }));
}

#[test]
fn cached_completion_clears_charge_before_usage_event() {
    let (trace, listener) = setup();
    let mut request = ModelRequest::default();
    let mut call = SupplementalCallGuard::start(&trace, "selected", &mut request);
    let mut response = ModelResponse::assistant("answer");
    response.served_from_cache = true;
    response.usage = Some(Usage {
        charged_amount: Some(ChargedAmount::usd_micros(7)),
        ..Default::default()
    });
    call.complete(&response);

    let events = listener
        .events()
        .into_iter()
        .map(|record| record.event)
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 5);
    assert!(matches!(events[0], AgentEvent::ModelStarted { .. }));
    assert!(matches!(events[1], AgentEvent::ModelResponseCached { .. }));
    assert!(
        matches!(&events[2], AgentEvent::UsageRecorded { usage } if usage.charged_amount.is_none())
    );
    assert!(
        matches!(&events[3], AgentEvent::ModelCompleted { usage: Some(usage), .. } if usage.charged_amount.is_none())
    );
    assert!(matches!(events[4], AgentEvent::GenerationEnriched { .. }));
    assert!(trace.is_cache_hit());
}
