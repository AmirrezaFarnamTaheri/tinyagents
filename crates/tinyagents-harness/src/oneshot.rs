//! Single-call model helpers without agent-loop, prompt, or tool policy.

use std::sync::Arc;

use crate::cost::ModelBudget;
use crate::tinyinference_llm::message::Message;
use crate::tinyinference_llm::model::{ChatModel, ModelRequest, ModelResponse};

/// Provider-option keys reserved for typed request fields or the tool surface.
///
/// The order is part of the stateless completion error contract.
pub const RESERVED_PROVIDER_OPTIONS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "tool_choice",
    "functions",
    "function_call",
    "response_format",
    "stream",
];

/// Options for a one-shot completion.
#[derive(Clone, Default)]
pub struct OneshotOptions {
    /// Optional spend ledger applied to the supplied model call.
    ///
    /// To account for physical retries or fallbacks, apply this budget before
    /// installing middleware that can make those additional calls. Leave this
    /// unset when the supplied model is already budgeted to avoid double charging.
    pub budget: Option<ModelBudget>,
}

/// Invokes a preconstructed model once with a provider-neutral request.
///
/// Tool calls and reserved provider-option keys are rejected before the model
/// is invoked or a budget reservation is attempted. The nonblank model id is
/// validated but preserved exactly as supplied. Provider selection, endpoint
/// configuration, headers, and host policy remain the caller's responsibility.
pub async fn complete_once(
    model: Arc<dyn ChatModel<()>>,
    request: ModelRequest,
    options: OneshotOptions,
) -> std::result::Result<ModelResponse, String> {
    if !request.tools.is_empty() {
        return Err(
            "complete_once: tools are not supported on a stateless completion; use an agent turn"
                .to_owned(),
        );
    }

    if let Some(key) = reserved_provider_option(&request.provider_options) {
        return Err(format!(
            "complete_once: provider_options may not set `{key}`; use the typed request fields"
        ));
    }

    if !request
        .model
        .as_deref()
        .is_some_and(|model| !model.trim().is_empty())
    {
        return Err("complete_once: request.model is required".to_owned());
    }

    let model = options
        .budget
        .as_ref()
        .map_or(model.clone(), |budget| budget.wrap_model(model));
    model
        .invoke(&(), request)
        .await
        .map_err(|error| format!("complete_once: {error}"))
}

/// Runs a text completion after the supplied ordered conversation history.
///
/// The concrete model is already selected by the caller, so this convenience
/// call does not infer a model id or add agent-loop behavior.
pub async fn simple_chat(
    model: Arc<dyn ChatModel<()>>,
    history: Vec<Message>,
    prompt: impl Into<String>,
) -> std::result::Result<ModelResponse, String> {
    let mut messages = history;
    messages.push(Message::user(prompt));
    model
        .invoke(&(), ModelRequest::new(messages))
        .await
        .map_err(|error| error.to_string())
}

fn reserved_provider_option(options: &serde_json::Value) -> Option<&'static str> {
    let object = options.as_object()?;
    RESERVED_PROVIDER_OPTIONS
        .iter()
        .copied()
        .find(|key| object.contains_key(*key))
}

#[cfg(test)]
#[path = "oneshot_tests.rs"]
mod tests;
