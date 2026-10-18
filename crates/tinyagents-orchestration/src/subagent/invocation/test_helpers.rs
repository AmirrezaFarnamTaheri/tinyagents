use async_trait::async_trait;
use tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};

pub(super) struct PanickingModel;

#[async_trait]
impl ChatModel<()> for PanickingModel {
    async fn invoke(&self, _state: &(), _request: ModelRequest) -> tinyinference_llm::Result<ModelResponse> {
        panic!("child model exploded");
    }
}
