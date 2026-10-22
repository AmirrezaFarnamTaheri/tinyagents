//! Single-call model helpers without agent-loop, prompt, or tool policy.

use std::sync::Arc;

use crate::cost::{
    ControlledChatModel, ControlledDispatchError, DispatchModelOptions, ModelBudget,
    PhysicalDispatchAdmission,
};
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

/// Failure from the validated native one-shot dispatch.
#[derive(Debug)]
pub enum CompleteOnceError {
    /// The request was rejected before policies or the provider were invoked.
    InvalidRequest(String),
    /// A physical call was refused or the provider failed.
    Dispatch(ControlledDispatchError),
}

impl std::fmt::Display for CompleteOnceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(message) => formatter.write_str(message),
            Self::Dispatch(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for CompleteOnceError {}

/// Invokes a preconstructed model once with a provider-neutral request.
///
/// Tool calls and reserved provider-option keys are rejected before the model
/// is invoked or a budget reservation is attempted. The nonblank model id is
/// validated but preserved exactly as supplied. Provider selection, endpoint
/// configuration, headers, and host policy remain the caller's responsibility.
///
/// # Errors
///
/// Returns a request validation message or the model error prefixed with
/// `complete_once:`.
pub async fn complete_once(
    model: Arc<dyn ChatModel<()>>,
    request: ModelRequest,
    options: OneshotOptions,
) -> std::result::Result<ModelResponse, String> {
    complete_once_controlled(model, request, options, None, None)
        .await
        .map_err(|error| match error {
            CompleteOnceError::InvalidRequest(message) => message,
            CompleteOnceError::Dispatch(ControlledDispatchError::Admission(error)) => {
                format!("complete_once: {error}")
            }
            CompleteOnceError::Dispatch(ControlledDispatchError::Model(error)) => {
                format!("complete_once: {error}")
            }
        })
}

/// Validates and invokes a one-shot request through TinyAgents' shared policy stack.
///
/// Validation runs before admission, budget reservation, observer notification,
/// or provider invocation. Hosts can retain typed admission failures without
/// rebuilding the validation or policy wrappers themselves.
///
/// # Errors
///
/// Returns [`CompleteOnceError::InvalidRequest`] for an invalid request and
/// [`CompleteOnceError::Dispatch`] for admission refusals or provider failures.
pub async fn complete_once_controlled(
    model: Arc<dyn ChatModel<()>>,
    request: ModelRequest,
    options: OneshotOptions,
    observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
    admission: Option<Arc<dyn PhysicalDispatchAdmission>>,
) -> Result<ModelResponse, CompleteOnceError> {
    validate_complete_once_request(&request).map_err(CompleteOnceError::InvalidRequest)?;
    if options.budget.is_none() && observer.is_none() && admission.is_none() {
        return model
            .invoke(&(), request)
            .await
            .map_err(|error| CompleteOnceError::Dispatch(ControlledDispatchError::Model(error)));
    }
    let controlled = ControlledChatModel::new(
        model,
        DispatchModelOptions::new()
            .monetary_budget(options.budget)
            .observer(observer)
            .admission(admission),
    );
    controlled
        .invoke_controlled(&(), request)
        .await
        .map_err(CompleteOnceError::Dispatch)
}

/// Validates the request shape accepted by [`complete_once`].
///
/// # Errors
///
/// Returns a display-ready validation message when the request is not valid
/// for a stateless completion.
pub fn validate_complete_once_request(request: &ModelRequest) -> Result<(), String> {
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

    if request
        .model
        .as_deref()
        .is_none_or(|model| model.trim().is_empty())
    {
        return Err("complete_once: request.model is required".to_owned());
    }
    Ok(())
}

/// Runs a text completion after the supplied ordered conversation history.
///
/// The concrete model is already selected by the caller, so this convenience
/// call does not infer a model id or add agent-loop behavior.
///
/// # Errors
///
/// Returns the model's display-ready error without adding an operation prefix.
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
