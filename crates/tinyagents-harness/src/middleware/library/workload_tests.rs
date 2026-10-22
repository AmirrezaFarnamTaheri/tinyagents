use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::agent_loop::AgentStreamItem;
use crate::context::{LoopTarget, MiddlewareControl, RunConfig, RunContext};
use crate::error::TinyAgentsError;
use crate::events::{EventRecord, GenerationPricing, RecordingListener};
use crate::ids::CallId;
use crate::middleware::{
    BoxModelFuture, MiddlewareModelOutcome, MiddlewareStack, ModelBaseCall, ModelHandler,
    ModelMiddleware,
};
use crate::runtime::AgentHarness;
use crate::testkit::ScriptedModel;
use futures::StreamExt;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{
    ModelCallCorrelation, ModelRequest, ModelResolutionSource, ModelResponse, ResolvedModel,
    ResolvedModelRoute, RouteRecordingModel,
};
use tinyinference_llm::usage::{BilledUsage, Usage};

#[derive(Default)]
struct RouteContext(Arc<Mutex<Option<ResolvedModelRoute>>>);

struct TestCostEstimator {
    calls: AtomicUsize,
    models: Mutex<Vec<String>>,
    usage: Mutex<Vec<BilledUsage>>,
}

impl TestCostEstimator {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            models: Mutex::new(Vec::new()),
            usage: Mutex::new(Vec::new()),
        }
    }
}

impl CostEstimator for Arc<TestCostEstimator> {
    fn estimate(&self, model: &str, usage: &BilledUsage) -> Option<f64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.models.lock().unwrap().push(model.to_owned());
        self.usage.lock().unwrap().push(*usage);
        if model == "uncatalogued-model" {
            None
        } else {
            Some(if model.trim().to_ascii_lowercase().ends_with(":free") {
                0.0
            } else {
                0.125
            })
        }
    }
}

struct FakeModelBase {
    response: Option<ModelResponse>,
    error: bool,
}

struct InitialEventCheckingBase {
    recorder: Arc<RecordingListener>,
    calls: Arc<AtomicUsize>,
    response: ModelResponse,
}

impl ModelBaseCall<(), RouteContext> for InitialEventCheckingBase {
    fn call<'a>(
        &'a self,
        _ctx: &'a mut RunContext<RouteContext>,
        _state: &'a (),
        _request: ModelRequest,
    ) -> BoxModelFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let initial_enrichment_seen = self.recorder.events().iter().any(|record| {
            matches!(
                &record.event,
                AgentEvent::GenerationEnriched { enrichment, .. }
                    if enrichment.provider_id.as_deref() == Some("configured-provider")
            )
        });
        assert!(
            initial_enrichment_seen,
            "configured provider is emitted before dispatch"
        );
        let response = self.response.clone();
        Box::pin(async move { Ok(response) })
    }
}

impl ModelBaseCall<(), RouteContext> for FakeModelBase {
    fn call<'a>(
        &'a self,
        _ctx: &'a mut RunContext<RouteContext>,
        _state: &'a (),
        _request: ModelRequest,
    ) -> BoxModelFuture<'a> {
        let result = if self.error {
            Err(TinyAgentsError::Model("provider failed".into()))
        } else {
            Ok(self.response.clone().expect("successful base has response"))
        };
        Box::pin(async move { result })
    }
}

struct CommandMiddleware;

#[async_trait]
impl ModelMiddleware<(), RouteContext> for CommandMiddleware {
    fn name(&self) -> &str {
        "test.command"
    }

    async fn wrap_model(
        &self,
        _ctx: &mut RunContext<RouteContext>,
        _state: &(),
        _request: ModelRequest,
        _next: ModelHandler<'_, (), RouteContext>,
    ) -> crate::error::Result<MiddlewareModelOutcome> {
        Ok(MiddlewareModelOutcome::Command {
            control: MiddlewareControl::JumpTo(LoopTarget::End),
        })
    }
}

fn configured_middleware(
    estimator: Arc<TestCostEstimator>,
    route_sink: impl Fn(&RouteContext, ResolvedModelRoute) + Send + Sync + 'static,
    pricing_models: Arc<Mutex<Vec<String>>>,
) -> ResolvedRouteMiddleware<RouteContext, Arc<TestCostEstimator>> {
    ResolvedRouteMiddleware::new(
        "configured-provider",
        "configured-model",
        estimator,
        route_sink,
    )
    .with_pricing_lookup(move |model| {
        pricing_models.lock().unwrap().push(model.to_owned());
        (model == "catalogued-model").then_some(GenerationPricing {
            input_per_mtok_usd: Some(1.0),
            cached_input_per_mtok_usd: Some(0.5),
            output_per_mtok_usd: Some(2.0),
        })
    })
}

fn response_with_route(route: ResolvedModelRoute) -> ModelResponse {
    ModelResponse {
        usage: Some(Usage {
            cache_read_tokens: 4,
            cache_creation_tokens: 3,
            reasoning_tokens: 2,
            context_window_tokens: Some(64_000),
            ..Usage::new(17, 9)
        }),
        raw: Some(serde_json::json!({
            "provider": "kept",
            "openhuman_usage_meta": {"context_window": 64000}
        })),
        resolved_model: Some(ResolvedModel {
            name: "fallback-model".into(),
            requested: None,
            source: ModelResolutionSource::AgentDefault,
        }),
        resolved_route: Some(route),
        ..ModelResponse::assistant("done")
    }
}

fn run(
    middleware: ResolvedRouteMiddleware<RouteContext, Arc<TestCostEstimator>>,
    request: ModelRequest,
    base: FakeModelBase,
    active_call: Option<CallId>,
) -> (MiddlewareModelOutcome, RouteContext, Arc<RecordingListener>) {
    futures::executor::block_on(async move {
        let mut ctx = RunContext::new(RunConfig::new("route-test"), RouteContext::default());
        ctx.active_model_call = active_call;
        let recorder = Arc::new(RecordingListener::new());
        ctx.events.subscribe(recorder.clone());
        let mut stack = MiddlewareStack::new();
        stack.push_model_middleware(Arc::new(middleware));
        let outcome = stack
            .run_wrapped_model(&mut ctx, &(), request, &base)
            .await
            .expect("model middleware succeeds");
        (outcome, ctx.data, recorder)
    })
}

fn events(recorder: &RecordingListener) -> Vec<AgentEvent> {
    recorder
        .events()
        .into_iter()
        .map(|record: EventRecord| record.event)
        .collect()
}

#[test]
fn resolved_route_enrichment_uses_response_route_full_usage_and_call_identity() {
    let estimator = Arc::new(TestCostEstimator::new());
    let pricing_models = Arc::new(Mutex::new(Vec::new()));
    let route = ResolvedModelRoute::new("anthropic", "catalogued-model", "backup");
    let response = response_with_route(route.clone());
    let expected_response = response.clone();
    let (outcome, context, recorder) = run(
        configured_middleware(
            estimator.clone(),
            |context, route| *context.0.lock().unwrap() = Some(route),
            pricing_models.clone(),
        ),
        ModelRequest {
            correlation: Some(ModelCallCorrelation::new("run", "correlation-call")),
            ..ModelRequest::default()
        },
        FakeModelBase {
            response: Some(response),
            error: false,
        },
        Some(CallId::new("active-call")),
    );

    let MiddlewareModelOutcome::Response(response) = outcome else {
        panic!("response outcome is preserved")
    };
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        serde_json::to_value(&expected_response).unwrap(),
        "middleware returns the complete response unchanged"
    );
    assert_eq!(*context.0.lock().unwrap(), Some(route));
    assert_eq!(estimator.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        estimator.models.lock().unwrap().as_slice(),
        ["catalogued-model"]
    );
    let usage = estimator.usage.lock().unwrap()[0];
    assert_eq!(usage.input_tokens, 17);
    assert_eq!(usage.output_tokens, 9);
    assert_eq!(usage.cached_input_tokens(), 4);
    assert_eq!(usage.cache_creation_tokens, 3);
    assert_eq!(usage.reasoning_tokens, 2);
    assert_eq!(usage.context_window(), 64_000);
    assert_eq!(
        pricing_models.lock().unwrap().as_slice(),
        ["catalogued-model"]
    );

    let events = events(&recorder);
    let enrichments = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::GenerationEnriched {
                call_id,
                enrichment,
            } => Some((call_id.as_str(), enrichment)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        enrichments.len(),
        2,
        "configured enrichment precedes final enrichment"
    );
    assert_eq!(enrichments[0].0, "correlation-call");
    assert_eq!(
        enrichments[0].1.provider_id.as_deref(),
        Some("configured-provider")
    );
    assert_eq!(enrichments[1].0, "correlation-call");
    assert_eq!(enrichments[1].1.provider_id.as_deref(), Some("anthropic"));
    assert_eq!(enrichments[1].1.estimated_cost_usd, Some(0.125));
    assert_eq!(
        enrichments[1]
            .1
            .pricing
            .as_ref()
            .unwrap()
            .input_per_mtok_usd,
        Some(1.0)
    );
}

#[tokio::test]
async fn configured_provider_is_emitted_before_exactly_one_model_invocation() {
    let estimator = Arc::new(TestCostEstimator::new());
    let recorder = Arc::new(RecordingListener::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = RunContext::new(RunConfig::new("route-event-order"), RouteContext::default());
    ctx.events.subscribe(recorder.clone());
    let mut stack: MiddlewareStack<(), RouteContext> = MiddlewareStack::new();
    stack.push_model_middleware(Arc::new(configured_middleware(
        estimator,
        |_, _| {},
        Arc::new(Mutex::new(Vec::new())),
    )));
    stack
        .run_wrapped_model(
            &mut ctx,
            &(),
            ModelRequest {
                correlation: Some(ModelCallCorrelation::new("r", "ordered-call")),
                ..ModelRequest::default()
            },
            &InitialEventCheckingBase {
                recorder: recorder.clone(),
                calls: calls.clone(),
                response: response_with_route(ResolvedModelRoute::new("openai", "model", "route")),
            },
        )
        .await
        .expect("model call succeeds");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let enrichments = events(&recorder)
        .into_iter()
        .filter_map(|event| match event {
            AgentEvent::GenerationEnriched {
                call_id,
                enrichment,
            } => Some((call_id.as_str().to_owned(), enrichment.provider_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        enrichments,
        [
            (
                "ordered-call".to_owned(),
                Some("configured-provider".to_owned())
            ),
            ("ordered-call".to_owned(), Some("openai".to_owned())),
        ]
    );
}

#[test]
fn route_sink_receives_route_without_call_id_and_uses_resolved_model_fallback() {
    let estimator = Arc::new(TestCostEstimator::new());
    let pricing_models = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(Mutex::new(None));
    let observed_sink = observed.clone();
    let mut response = ModelResponse::assistant("no correlation");
    response.resolved_model = Some(ResolvedModel {
        name: "catalogued-model".into(),
        requested: None,
        source: ModelResolutionSource::AgentDefault,
    });
    let (outcome, context, recorder) = run(
        configured_middleware(
            estimator.clone(),
            move |context, route| {
                *context.0.lock().unwrap() = Some(route.clone());
                *observed_sink.lock().unwrap() = Some(route);
            },
            pricing_models.clone(),
        ),
        ModelRequest::default(),
        FakeModelBase {
            response: Some(response),
            error: false,
        },
        Some(CallId::new("active-only-call")),
    );
    assert!(matches!(outcome, MiddlewareModelOutcome::Response(_)));
    assert!(context.0.lock().unwrap().is_none());
    assert!(observed.lock().unwrap().is_none());
    assert_eq!(estimator.calls.load(Ordering::SeqCst), 0);
    let fallback_events = events(&recorder);
    assert!(fallback_events.iter().any(|event| matches!(
        event,
        AgentEvent::GenerationEnriched { call_id, enrichment }
            if call_id.as_str() == "active-only-call"
                && enrichment.pricing.is_some()
                && enrichment.provider_id.as_deref() == Some("configured-provider")
    )));
    assert_eq!(
        pricing_models.lock().unwrap().as_slice(),
        ["catalogued-model"]
    );

    let routed = ResolvedModelRoute::new("openai", "routed", "chat");
    let mut response = ModelResponse::assistant("route without call id");
    response.resolved_route = Some(routed.clone());
    let (outcome, context, no_call_id_recorder) = run(
        configured_middleware(
            estimator,
            |context, route| *context.0.lock().unwrap() = Some(route),
            pricing_models,
        ),
        ModelRequest::default(),
        FakeModelBase {
            response: Some(response),
            error: false,
        },
        None,
    );
    assert!(matches!(outcome, MiddlewareModelOutcome::Response(_)));
    assert_eq!(*context.0.lock().unwrap(), Some(routed));
    assert!(
        events(&no_call_id_recorder)
            .iter()
            .all(|event| { !matches!(event, AgentEvent::GenerationEnriched { .. }) })
    );
}

#[test]
fn cache_reported_charge_and_free_route_follow_cost_authority_rules() {
    for (cached, raw, expected_estimates) in [
        (true, None, 0),
        (
            false,
            Some(serde_json::json!({
                "openhuman_usage_meta": {"charged_amount_usd": 0.00000025}
            })),
            0,
        ),
        (
            false,
            Some(serde_json::json!({
                "openhuman_usage_meta": {"charged_amount_usd": 0.0}
            })),
            0,
        ),
        (false, None, 1),
    ] {
        let estimator = Arc::new(TestCostEstimator::new());
        let pricing_models = Arc::new(Mutex::new(Vec::new()));
        let mut response = response_with_route(ResolvedModelRoute::new(
            "openai",
            "catalogued-model",
            "chat",
        ));
        response.served_from_cache = cached;
        response.raw = raw;
        let (outcome, _, recorder) = run(
            configured_middleware(estimator.clone(), |_, _| {}, pricing_models),
            ModelRequest {
                correlation: Some(ModelCallCorrelation::new("r", "c")),
                ..ModelRequest::default()
            },
            FakeModelBase {
                response: Some(response),
                error: false,
            },
            None,
        );
        assert!(matches!(outcome, MiddlewareModelOutcome::Response(_)));
        assert_eq!(estimator.calls.load(Ordering::SeqCst), expected_estimates);
        let final_enrichment = events(&recorder)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::GenerationEnriched { enrichment, .. }
                    if enrichment.pricing.is_some() =>
                {
                    Some(enrichment)
                }
                _ => None,
            })
            .next_back()
            .expect("catalogued route has pricing");
        if expected_estimates == 0 {
            assert_eq!(final_enrichment.estimated_cost_usd, None);
        } else {
            assert_eq!(final_enrichment.estimated_cost_usd, Some(0.125));
        }
    }

    let estimator = Arc::new(TestCostEstimator::new());
    let pricing_models = Arc::new(Mutex::new(Vec::new()));
    let mut response = response_with_route(ResolvedModelRoute::new(
        "openrouter",
        "some-model:free",
        "free",
    ));
    response.usage = Some(Usage::new(2, 3));
    let (outcome, _, recorder) = run(
        configured_middleware(estimator.clone(), |_, _| {}, pricing_models.clone()),
        ModelRequest {
            correlation: Some(ModelCallCorrelation::new("r", "free-call")),
            ..ModelRequest::default()
        },
        FakeModelBase {
            response: Some(response),
            error: false,
        },
        None,
    );
    assert!(matches!(outcome, MiddlewareModelOutcome::Response(_)));
    assert_eq!(estimator.calls.load(Ordering::SeqCst), 1);
    assert!(pricing_models.lock().unwrap().is_empty());
    let event = events(&recorder)
        .into_iter()
        .find_map(|event| match event {
            AgentEvent::GenerationEnriched { enrichment, .. } if enrichment.pricing.is_some() => {
                Some(enrichment)
            }
            _ => None,
        })
        .expect("free route has known pricing");
    let pricing = event.pricing.unwrap();
    assert_eq!(pricing.input_per_mtok_usd, Some(0.0));
    assert_eq!(pricing.cached_input_per_mtok_usd, Some(0.0));
    assert_eq!(pricing.output_per_mtok_usd, Some(0.0));
    assert_eq!(event.estimated_cost_usd, Some(0.0));
}

#[tokio::test]
async fn errors_retain_initial_provider_enrichment_and_commands_remain_commands() {
    let estimator = Arc::new(TestCostEstimator::new());
    let pricing_models = Arc::new(Mutex::new(Vec::new()));
    let middleware = configured_middleware(
        estimator.clone(),
        |_, _| panic!("failed call has no resolved route"),
        pricing_models,
    );
    let mut ctx = RunContext::new(RunConfig::new("route-error"), RouteContext::default());
    let recorder = Arc::new(RecordingListener::new());
    ctx.events.subscribe(recorder.clone());
    let mut stack: MiddlewareStack<(), RouteContext> = MiddlewareStack::new();
    stack.push_model_middleware(Arc::new(middleware));
    let base = FakeModelBase {
        response: None,
        error: true,
    };
    assert!(
        stack
            .run_wrapped_model(
                &mut ctx,
                &(),
                ModelRequest {
                    correlation: Some(ModelCallCorrelation::new("r", "failed-call")),
                    ..ModelRequest::default()
                },
                &base,
            )
            .await
            .is_err()
    );
    let error_events = events(&recorder);
    assert!(error_events.iter().any(|event| matches!(
        event,
        AgentEvent::GenerationEnriched { call_id, enrichment }
            if call_id.as_str() == "failed-call"
                && enrichment.provider_id.as_deref() == Some("configured-provider")
    )));
    assert_eq!(
        error_events
            .iter()
            .filter(|event| matches!(event, AgentEvent::GenerationEnriched { .. }))
            .count(),
        1
    );
    assert_eq!(estimator.calls.load(Ordering::SeqCst), 0);

    let middleware = configured_middleware(estimator, |_, _| {}, Arc::new(Mutex::new(Vec::new())));
    let mut ctx = RunContext::new(RunConfig::new("route-command"), RouteContext::default());
    let mut stack: MiddlewareStack<(), RouteContext> = MiddlewareStack::new();
    stack.push_model_middleware(Arc::new(middleware));
    stack.push_model_middleware(Arc::new(CommandMiddleware));
    let outcome = stack
        .run_wrapped_model(
            &mut ctx,
            &(),
            ModelRequest::default(),
            &FakeModelBase {
                response: Some(ModelResponse::assistant("unused")),
                error: false,
            },
        )
        .await
        .expect("command is a successful short-circuit");
    assert!(matches!(
        outcome,
        MiddlewareModelOutcome::Command {
            control: MiddlewareControl::JumpTo(LoopTarget::End)
        }
    ));
}

#[test]
fn invalid_configured_and_resolved_provider_ids_are_omitted_from_events() {
    let estimator = Arc::new(TestCostEstimator::new());
    let pricing_models = Arc::new(Mutex::new(Vec::new()));
    let route = ResolvedModelRoute::new("provider/secret-token", "catalogued-model", "chat");
    let mut response = response_with_route(route);
    response.usage = Some(Usage::new(10, 2));
    let middleware = ResolvedRouteMiddleware::new(
        "configured/provider-secret",
        "configured-model",
        estimator.clone(),
        |_, _| {},
    )
    .with_pricing_lookup({
        let pricing_models = pricing_models.clone();
        move |model| {
            pricing_models.lock().unwrap().push(model.to_owned());
            Some(GenerationPricing {
                input_per_mtok_usd: Some(1.0),
                ..Default::default()
            })
        }
    });
    let (_, _, recorder) = run(
        middleware,
        ModelRequest {
            correlation: Some(ModelCallCorrelation::new("r", "safe-call-id")),
            ..ModelRequest::default()
        },
        FakeModelBase {
            response: Some(response),
            error: false,
        },
        None,
    );
    let events = events(&recorder);
    let enrichments = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::GenerationEnriched { enrichment, .. } => Some(enrichment),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(enrichments.len(), 1);
    assert!(enrichments[0].provider_id.is_none());
    assert!(enrichments[0].pricing.is_some());
    assert_eq!(enrichments[0].estimated_cost_usd, Some(0.125));
    assert_eq!(
        pricing_models.lock().unwrap().as_slice(),
        ["catalogued-model"]
    );
    let rendered = format!("{events:?}");
    assert!(!rendered.contains("provider/secret-token"));
    assert!(!rendered.contains("configured/provider-secret"));
}

#[test]
fn missing_usage_and_unknown_price_do_not_invent_estimates() {
    for (model, usage, estimator_calls) in [
        ("uncatalogued-model", Some(Usage::new(5, 2)), 1),
        ("catalogued-model", None, 0),
    ] {
        let estimator = Arc::new(TestCostEstimator::new());
        let pricing_models = Arc::new(Mutex::new(Vec::new()));
        let mut response = ModelResponse::assistant("no estimate");
        response.usage = usage;
        response.resolved_route = Some(ResolvedModelRoute::new("openai", model, "chat"));
        let (_, _, recorder) = run(
            configured_middleware(estimator.clone(), |_, _| {}, pricing_models),
            ModelRequest {
                correlation: Some(ModelCallCorrelation::new("r", "call")),
                ..ModelRequest::default()
            },
            FakeModelBase {
                response: Some(response),
                error: false,
            },
            None,
        );
        assert_eq!(estimator.calls.load(Ordering::SeqCst), estimator_calls);
        let final_event = events(&recorder)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::GenerationEnriched { enrichment, .. }
                    if enrichment.provider_id.as_deref() == Some("openai") =>
                {
                    Some(enrichment)
                }
                _ => None,
            })
            .next_back()
            .expect("valid provider remains enriched");
        assert_eq!(final_event.estimated_cost_usd, None);
        assert_eq!(final_event.pricing.is_some(), model == "catalogued-model");
    }
}

#[tokio::test]
async fn resolved_route_middleware_observes_stream_folded_terminal_response() {
    let route = ResolvedModelRoute::new("stream-provider", "stream-model", "stream-route");
    let estimator = Arc::new(TestCostEstimator::new());
    let middleware = ResolvedRouteMiddleware::new(
        "configured-provider",
        "configured-model",
        estimator,
        |context: &RouteContext, route| *context.0.lock().unwrap() = Some(route),
    );
    let mut harness: AgentHarness<(), RouteContext> = AgentHarness::new();
    harness
        .register_model(
            "stream",
            Arc::new(RouteRecordingModel::new(
                Arc::new(ScriptedModel::replies(vec!["streamed"])),
                route.clone(),
            )),
        )
        .set_default_model("stream");
    harness.push_model_middleware(Arc::new(middleware));
    let context = RouteContext::default();
    let slot = context.0.clone();
    let context = RunContext::new(RunConfig::new("route-stream"), context);
    let mut stream =
        Box::pin(harness.invoke_stream_in_context(&(), context, vec![Message::user("route")]));
    let mut completed = false;
    while let Some(item) = stream.next().await {
        match item {
            AgentStreamItem::Completed(_) => {
                completed = true;
                break;
            }
            AgentStreamItem::Failed { error, .. } => panic!("stream failed: {error}"),
            AgentStreamItem::Event(_) => {}
        }
    }
    assert!(completed, "stream yields a terminal run item");
    assert_eq!(*slot.lock().unwrap(), Some(route));
}
