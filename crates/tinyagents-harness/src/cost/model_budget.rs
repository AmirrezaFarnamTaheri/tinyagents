//! Shared run ledger and conservative physical-call reservation bounds.

use std::sync::Arc;

use async_trait::async_trait;
use tinyinference_llm::model::ChatModel;
use tinyinference_llm::model::DispatchCheckedModel;
use tinyinference_llm::model::budget::{BeforeDispatch, Budget, BudgetedModel, CallBudget};
use tinyinference_llm::model::{
    DeferredHandle, DeferredStatus, ModelRequest, ModelResponse, ModelStream, ModelStreamItem,
};

use super::dispatch_admission::{ControlledDispatchError, DispatchModelOptions};

/// Budget state shared by the concrete models used for one turn and its child
/// agents.
///
/// Wrap each prebuilt route before retry, fallback, and cache middleware. The
/// cache then prevents replayed responses from reaching the budgeted model,
/// while retries and fallback attempts each pass through it and settle once.
#[derive(Debug, Clone)]
pub struct ModelBudget {
    /// Shared ledger. Clones and child ledgers retain the same accounting tree.
    pub ledger: Budget,
    /// Conservative reservation for one physical provider call.
    pub call: CallBudget,
}

impl ModelBudget {
    /// Creates a shared budget for the supplied run ledger and call bound.
    pub fn new(ledger: Budget, call: CallBudget) -> Self {
        Self { ledger, call }
    }

    /// Waits when live reservations temporarily consume available capacity.
    /// Settled spend that exceeds the budget still fails immediately.
    pub fn wait_for_capacity(mut self) -> Self {
        self.ledger = self.ledger.wait_for_capacity();
        self
    }

    /// Wraps a concrete model with charge normalization and ledger accounting.
    ///
    /// Apply this below retry/fallback and above the provider; response caching
    /// should remain outside it so a cache hit does not reserve or settle twice.
    pub fn wrap_model(&self, model: Arc<dyn ChatModel<()>>) -> Arc<dyn ChatModel<()>> {
        self.wrap_model_with_observer(model, None)
    }

    /// Wrap a model and report its physical calls to an explicit observer.
    ///
    /// Charge normalization runs before either observer so explicit completion
    /// observers and the turn-scoped observer see the same authoritative usage
    /// that the budget ledger settles.
    pub fn wrap_model_with_observer(
        &self,
        model: Arc<dyn ChatModel<()>>,
        observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
    ) -> Arc<dyn ChatModel<()>> {
        wrap_model_with_policies(model, Some(self), observer, None)
    }
}

fn wrap_observed_model(
    model: Arc<dyn ChatModel<()>>,
    observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
) -> Arc<dyn ChatModel<()>> {
    let charged = Arc::new(super::GatewayChargeModel::new(model));
    let explicitly_observed: Arc<dyn ChatModel<()>> = match observer {
        Some(observer) => Arc::new(crate::observability::ObservedChatModel::new(
            charged, observer,
        )),
        None => charged,
    };
    crate::observability::turn_observer::observe_physical_dispatches(explicitly_observed)
}

fn wrap_model_with_policies(
    model: Arc<dyn ChatModel<()>>,
    budget: Option<&ModelBudget>,
    observer: Option<Arc<dyn crate::observability::PhysicalDispatchObserver>>,
    before_dispatch: Option<Arc<dyn BeforeDispatch>>,
) -> Arc<dyn ChatModel<()>> {
    let observed = wrap_observed_model(model, observer);
    match budget {
        Some(budget) => {
            let budgeted = BudgetedModel::new(observed, budget.ledger.clone(), budget.call);
            match before_dispatch {
                Some(hook) => Arc::new(budgeted.before_dispatch(hook)),
                None => Arc::new(budgeted),
            }
        }
        None => match before_dispatch {
            Some(hook) => Arc::new(DispatchCheckedModel::new(observed, hook)),
            None => observed,
        },
    }
}

/// Controlled invocation facade around the existing budget and observer stack.
pub struct ControlledChatModel {
    model: Arc<dyn ChatModel<()>>,
    options: DispatchModelOptions,
}

impl ControlledChatModel {
    /// Builds the compatibility wrapper from an existing model and policies.
    pub fn new(model: Arc<dyn ChatModel<()>>, options: DispatchModelOptions) -> Self {
        Self { model, options }
    }

    /// Invokes this model with a typed physical-admission result.
    pub async fn invoke_controlled(
        &self,
        state: &(),
        request: ModelRequest,
    ) -> Result<ModelResponse, ControlledDispatchError> {
        let permit = match &self.options.admission {
            Some(admission) => Some(
                admission
                    .acquire()
                    .await
                    .map_err(ControlledDispatchError::from)?,
            ),
            None => None,
        };
        let dispatch_outcome = Arc::new(std::sync::Mutex::new(None));
        let hook = permit.as_ref().map(|permit| {
            Arc::new(AdmissionBeforeDispatch {
                permit: permit.clone(),
                outcome: dispatch_outcome.clone(),
            }) as Arc<dyn BeforeDispatch>
        });
        let wrapped = wrap_model_with_policies(
            self.model.clone(),
            self.options.monetary_budget.as_ref(),
            self.options.observer.clone(),
            hook,
        );

        match wrapped.invoke(state, request).await {
            Ok(response) => Ok(response),
            Err(error) => match dispatch_outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                Some(refusal) => Err(refusal.into()),
                None => Err(ControlledDispatchError::Model(error)),
            },
        }
    }

    /// Opens a stream with a typed physical-admission result.
    pub async fn stream_controlled(
        &self,
        state: &(),
        request: ModelRequest,
    ) -> Result<ModelStream, ControlledDispatchError> {
        let permit = match &self.options.admission {
            Some(admission) => Some(
                admission
                    .acquire()
                    .await
                    .map_err(ControlledDispatchError::from)?,
            ),
            None => None,
        };
        let dispatch_outcome = Arc::new(std::sync::Mutex::new(None));
        let hook = permit.as_ref().map(|permit| {
            Arc::new(AdmissionBeforeDispatch {
                permit: permit.clone(),
                outcome: dispatch_outcome.clone(),
            }) as Arc<dyn BeforeDispatch>
        });
        let wrapped = wrap_model_with_policies(
            self.model.clone(),
            self.options.monetary_budget.as_ref(),
            self.options.observer.clone(),
            hook,
        );
        let stream = match wrapped.stream(state, request).await {
            Ok(stream) => stream,
            Err(error) => {
                return match dispatch_outcome
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    Some(refusal) => Err(refusal.into()),
                    None => Err(ControlledDispatchError::Model(error)),
                };
            }
        };
        Ok(guard_dispatch_stream(stream, permit))
    }
}

struct AdmissionBeforeDispatch {
    permit: Arc<dyn super::dispatch_admission::PhysicalDispatchPermit>,
    outcome: Arc<std::sync::Mutex<Option<super::dispatch_admission::DispatchRefusal>>>,
}

impl BeforeDispatch for AdmissionBeforeDispatch {
    fn check(&self) -> tinyinference_llm::Result<()> {
        match self.permit.mark_dispatched() {
            Ok(()) => Ok(()),
            Err(refusal) => Err(self.record_refusal(refusal)),
        }
    }

    fn physical_attempt(&self) -> tinyinference_llm::Result<()> {
        match self.permit.mark_physical_attempt() {
            Ok(()) => Ok(()),
            Err(refusal) => Err(self.record_refusal(refusal)),
        }
    }
}

impl AdmissionBeforeDispatch {
    fn record_refusal(
        &self,
        refusal: super::dispatch_admission::DispatchRefusal,
    ) -> tinyinference_llm::Error {
        super::dispatch_admission::record_invocation_refusal(refusal);
        *self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(refusal);
        tinyinference_llm::Error::Validation("physical dispatch admission refused".to_owned())
    }
}

fn guard_dispatch_stream(
    stream: ModelStream,
    permit: Option<Arc<dyn super::dispatch_admission::PhysicalDispatchPermit>>,
) -> ModelStream {
    use futures::Stream;

    let metadata = stream.metadata().clone();
    let mut stream = Box::pin(stream);
    let mut permit = permit;
    let mut ended = false;
    let guarded = futures::stream::poll_fn(move |cx| {
        if ended {
            return std::task::Poll::Ready(None);
        }
        let item = stream.as_mut().poll_next(cx);
        let terminal_item = matches!(
            &item,
            std::task::Poll::Ready(Some(
                ModelStreamItem::Completed(_)
                    | ModelStreamItem::Failed(_)
                    | ModelStreamItem::ProviderFailed(_)
                    | ModelStreamItem::Deferred(_)
            ))
        );
        if terminal_item || matches!(item, std::task::Poll::Ready(None)) {
            ended = true;
            drop(permit.take());
        }
        item
    });
    ModelStream::new(Box::pin(guarded)).with_metadata(metadata)
}

#[async_trait]
impl ChatModel<()> for ControlledChatModel {
    fn profile(&self) -> Option<&tinyinference_llm::model::ModelProfile> {
        self.model.profile()
    }

    fn supports_input(
        &self,
        modality: tinyinference_llm::model::InputModality,
        mime: &str,
        source: tinyinference_llm::model::InputSource,
    ) -> bool {
        self.model.supports_input(modality, mime, source)
    }

    fn cache_identity(&self) -> Option<String> {
        self.model.cache_identity()
    }

    fn tracks_physical_attempts(&self) -> bool {
        self.model.tracks_physical_attempts()
    }

    async fn invoke(
        &self,
        state: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelResponse> {
        self.invoke_controlled(state, request)
            .await
            .map_err(|error| {
                if let ControlledDispatchError::Admission(refusal) = error {
                    super::dispatch_admission::record_invocation_refusal(refusal);
                }
                tinyinference_llm::Error::from(error)
            })
    }

    async fn stream(
        &self,
        state: &(),
        request: ModelRequest,
    ) -> tinyinference_llm::Result<ModelStream> {
        self.stream_controlled(state, request)
            .await
            .map_err(|error| {
                if let ControlledDispatchError::Admission(refusal) = error {
                    super::dispatch_admission::record_invocation_refusal(refusal);
                }
                tinyinference_llm::Error::from(error)
            })
    }

    async fn fetch_deferred(
        &self,
        handle: &DeferredHandle,
    ) -> tinyinference_llm::Result<DeferredStatus> {
        self.model.fetch_deferred(handle).await
    }
}

#[cfg(test)]
#[path = "model_budget_tests.rs"]
mod tests;
