use super::*;

use async_trait::async_trait;
use tinyinference_llm::model::budget::{Spend, SpendLimits};
use tinyinference_llm::model::{ModelRequest, ModelResponse};
use tinyinference_llm::usage::Usage;

struct ChargedModel;

#[async_trait]
impl ChatModel<()> for ChargedModel {
    async fn invoke(
        &self,
        _state: &(),
        _request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        Ok(ModelResponse {
            usage: Some(Usage::new(2, 3)),
            raw: Some(serde_json::json!({"usage":{"buyer_cost_micro":7}})),
            ..ModelResponse::assistant("ok")
        })
    }
}

struct RecordingObserver(std::sync::Mutex<Vec<crate::observability::turn_observer::TurnObservation>>);

impl crate::observability::turn_observer::Observer for RecordingObserver {
    fn on_event(&self, event: &crate::observability::turn_observer::TurnObservation) {
        self.0.lock().unwrap().push(event.clone());
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
    assert_eq!(dispatch_usage.and_then(|usage| usage.cost_usd), Some(0.000007));
}
