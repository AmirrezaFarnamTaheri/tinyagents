use super::*;

use async_trait::async_trait;
use tinyinference_llm::model::budget::{Spend, SpendLimits};
use tinyinference_llm::model::{ModelRequest, ModelResponse};
use tinyinference_llm::usage::Usage;

struct ChargedModel;

struct MalformedBuyerChargeModel;

#[async_trait]
impl ChatModel<()> for ChargedModel {
    async fn invoke(
        &self,
        _state: &(),
        _request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        Ok(ModelResponse {
            usage: Some(Usage::new(2, 3)),
            raw: Some(serde_json::json!({"usage":{"buyer_cost_micro":7,"cost":0.000999}})),
            ..ModelResponse::assistant("ok")
        })
    }
}

#[async_trait]
impl ChatModel<()> for MalformedBuyerChargeModel {
    async fn invoke(
        &self,
        _state: &(),
        _request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        Ok(ModelResponse {
            usage: Some(Usage::new(2, 3)),
            raw: Some(serde_json::json!({"usage":{"buyer_cost_micro":"invalid","cost":0.000999}})),
            ..ModelResponse::assistant("ok")
        })
    }
}

#[tokio::test]
async fn explicit_dispatch_observer_runs_after_charge_normalization() {
    use crate::observability::turn_observer::TurnObservation;

    let observer = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
    let ambient = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
    let scope = crate::observability::turn_observer::ObserverScope::new(
        ambient.clone(),
        crate::observability::turn_observer::TraceContent::MetadataOnly,
    );
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(10_000),
            cost_micros: Some(20),
        }),
        CallBudget {
            input_tokens: 5_000,
            output_tokens: 10,
            cost_micros: 10,
        },
    );
    let wrapped = budget.wrap_model_with_observer(
        Arc::new(ChargedModel),
        Some(Arc::new(ExplicitDispatchObserver(observer.clone()))),
    );
    crate::observability::turn_observer::with_observer(scope, async {
        wrapped
            .invoke(&(), ModelRequest::default())
            .await
            .expect("budgeted provider call");
    })
    .await;

    let events = observer.0.lock().unwrap();
    let usage = events.iter().find_map(|event| match event {
        TurnObservation::ModelDispatch { usage, .. } => usage.as_ref(),
        _ => None,
    });
    assert_eq!(usage.and_then(|usage| usage.cost_usd), Some(0.000007));
    drop(events);
    let ambient_events = ambient.0.lock().unwrap();
    let ambient_usage = ambient_events.iter().find_map(|event| match event {
        TurnObservation::ModelDispatch { usage, .. } => usage.as_ref(),
        _ => None,
    });
    assert_eq!(
        ambient_usage.and_then(|usage| usage.cost_usd),
        Some(0.000007)
    );
}

struct RecordingObserver(
    std::sync::Mutex<Vec<crate::observability::turn_observer::TurnObservation>>,
);

impl crate::observability::turn_observer::Observer for RecordingObserver {
    fn on_event(&self, event: &crate::observability::turn_observer::TurnObservation) {
        self.0.lock().unwrap().push(event.clone());
    }
}

struct ExplicitDispatchObserver(Arc<RecordingObserver>);
struct ExplicitDispatchTerminal(Arc<RecordingObserver>);

impl crate::observability::PhysicalDispatchObserver for ExplicitDispatchObserver {
    fn on_start(
        &self,
        _: &crate::observability::PhysicalDispatchMetadata,
    ) -> Box<dyn crate::observability::PhysicalDispatchTerminal> {
        Box::new(ExplicitDispatchTerminal(self.0.clone()))
    }
}

impl crate::observability::PhysicalDispatchTerminal for ExplicitDispatchTerminal {
    fn on_terminal(self: Box<Self>, outcome: crate::observability::PhysicalDispatchOutcome) {
        use crate::observability::turn_observer::{ObservedUsage, TurnObservation};

        crate::observability::turn_observer::Observer::on_event(
            self.0.as_ref(),
            &TurnObservation::ModelDispatch {
                run_id: "explicit".into(),
                call_id: None,
                requested_model: None,
                answered_model: outcome.answered_model,
                finish_reason: outcome.finish_reason,
                duration_ms: outcome.duration.as_millis() as u64,
                status: outcome.status,
                refused: outcome.refused,
                usage: outcome.usage.map(|usage| ObservedUsage {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cached_tokens: usage.cached_tokens,
                    reasoning_tokens: usage.reasoning_tokens,
                    cost_usd: usage.cost_usd,
                }),
            },
        );
    }
}

#[tokio::test]
async fn wrapped_model_settles_the_reported_charge_once_into_shared_ledger() {
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(10_000),
            cost_micros: Some(20),
        }),
        CallBudget {
            input_tokens: 5_000,
            output_tokens: 10,
            cost_micros: 10,
        },
    );
    let wrapped = budget.wrap_model(Arc::new(ChargedModel));
    let response = wrapped
        .invoke(&(), ModelRequest::default())
        .await
        .expect("budgeted call");

    assert_eq!(
        response.usage.unwrap().charged_amount.unwrap().micros,
        7,
        "gateway charge normalization feeds the same amount the ledger settles"
    );
    assert_eq!(
        budget.ledger.snapshot().spent,
        Spend {
            tokens: 5,
            cost_micros: 7,
        }
    );
}

#[tokio::test]
async fn physical_dispatch_observes_the_normalized_buyer_charge() {
    use crate::observability::turn_observer::{
        ObserverScope, TraceContent, TurnObservation, with_observer,
    };

    let observer = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
    let scope = ObserverScope::new(observer.clone(), TraceContent::MetadataOnly);
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(10_000),
            cost_micros: Some(20),
        }),
        CallBudget {
            input_tokens: 5_000,
            output_tokens: 10,
            cost_micros: 10,
        },
    );
    let wrapped = budget.wrap_model(Arc::new(ChargedModel));
    let response = with_observer(scope, async {
        wrapped
            .invoke(&(), ModelRequest::default())
            .await
            .expect("budgeted provider call")
    })
    .await;

    assert_eq!(response.usage.unwrap().charged_amount.unwrap().micros, 7);
    assert_eq!(budget.ledger.snapshot().spent.cost_micros, 7);
    let events = observer.0.lock().unwrap();
    let dispatch_usage = events.iter().find_map(|event| match event {
        TurnObservation::ModelDispatch { usage, .. } => usage.as_ref(),
        _ => None,
    });
    assert_eq!(
        dispatch_usage.and_then(|usage| usage.cost_usd),
        Some(0.000007)
    );
}

#[tokio::test]
async fn malformed_authoritative_buyer_charge_stays_unknown_for_both_observers() {
    use crate::observability::turn_observer::{
        ObserverScope, TraceContent, TurnObservation, with_observer,
    };

    let explicit = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
    let ambient = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
    let scope = ObserverScope::new(ambient.clone(), TraceContent::MetadataOnly);
    let budget = ModelBudget::new(
        Budget::new(SpendLimits {
            tokens: Some(10_000),
            cost_micros: Some(20),
        }),
        CallBudget {
            input_tokens: 5_000,
            output_tokens: 10,
            cost_micros: 10,
        },
    );
    let wrapped = budget.wrap_model_with_observer(
        Arc::new(MalformedBuyerChargeModel),
        Some(Arc::new(ExplicitDispatchObserver(explicit.clone()))),
    );
    let response = with_observer(scope, async {
        wrapped
            .invoke(&(), ModelRequest::default())
            .await
            .expect("budgeted provider call")
    })
    .await;

    assert!(response.usage.unwrap().charged_amount.is_none());
    assert_eq!(budget.ledger.snapshot().spent.cost_micros, 10);
    for recorder in [explicit, ambient] {
        let events = recorder.0.lock().unwrap();
        let usage = events.iter().find_map(|event| match event {
            TurnObservation::ModelDispatch { usage, .. } => usage.as_ref(),
            _ => None,
        });
        assert_eq!(usage.and_then(|usage| usage.cost_usd), None);
    }
}
