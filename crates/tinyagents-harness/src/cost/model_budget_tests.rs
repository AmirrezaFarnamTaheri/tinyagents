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
