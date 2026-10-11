use super::*;
use crate::steering::{SteeringCommand, SteeringHandle};

#[derive(Clone, Default)]
struct Metrics {
    completed_calls: usize,
}

impl StopHookMetrics for Metrics {
    fn record_model(&mut self, _model: &str, _response: &ModelResponse) {
        self.completed_calls += 1;
    }
}

type Observations = Arc<Mutex<Vec<(&'static str, u32, usize)>>>;

struct Hook {
    name: &'static str,
    stop: bool,
    observations: Observations,
}

#[async_trait]
impl StopHook<Metrics> for Hook {
    fn name(&self) -> &str {
        self.name
    }

    async fn check(&self, state: &TurnState<'_, Metrics>) -> StopDecision {
        self.observations.lock().unwrap().push((
            self.name, state.iteration, state.cost.completed_calls,
        ));
        assert_eq!(state.max_iterations, 5);
        assert_eq!(state.model, "model");
        if self.stop {
            StopDecision::Stop { reason: format!("{} voted stop", self.name) }
        } else {
            StopDecision::Continue
        }
    }
}

/// Each run receives its own pause queue, even when middleware is shared.
fn run() -> RunContext {
    RunContext::new(RunConfig::new("same-run-id").with_max_model_calls(5), ())
        .with_steering(SteeringHandle::allow_all())
}

/// A later observer still accounts for the call that made an earlier policy stop.
#[tokio::test]
async fn every_stop_hook_observes_the_stopping_call_and_the_first_reason_pauses_once() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let report_sink = reports.clone();
    let middleware = StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "first", stop: true, observations: observations.clone() }),
        Arc::new(Hook { name: "second", stop: true, observations: observations.clone() }),
        Arc::new(Hook { name: "observer", stop: false, observations: observations.clone() }),
    ]).with_stop_observer(move |report| report_sink.lock().unwrap().push(report.clone()));
    let mut context = run();
    let mut response = ModelResponse::assistant("done");
    middleware.after_model(&mut context, &(), &mut response).await.unwrap();
    middleware.after_model(&mut context, &(), &mut response).await.unwrap();

    assert_eq!(*observations.lock().unwrap(), vec![
        ("first", 1, 1), ("second", 1, 1), ("observer", 1, 1),
    ]);
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].hook, "first");
    assert_eq!(reports[0].reason, "first voted stop");
    assert_eq!(reports[0].iteration, 1);
    assert_eq!(context.steering.as_ref().unwrap().drain().iter()
        .filter(|command| matches!(command, SteeringCommand::Pause)).count(), 1);
}

/// Equal durable ids do not share counters, metrics or pause latches.
#[tokio::test]
async fn stop_hook_state_is_isolated_between_context_instances() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "budget", stop: true, observations: observations.clone() }),
    ]);
    let mut first = run();
    let mut second = run();
    assert_ne!(first.instance_id(), second.instance_id());
    for context in [&mut first, &mut second] {
        middleware.after_model(context, &(), &mut ModelResponse::assistant("done")).await.unwrap();
        assert_eq!(context.steering.as_ref().unwrap().drain().len(), 1);
    }
    assert_eq!(*observations.lock().unwrap(), vec![("budget", 1, 1), ("budget", 1, 1)]);
}

/// Error cleanup retires a partial tally before the same context is reused.
#[tokio::test]
async fn stop_hook_error_cleanup_restarts_the_completed_call_tally() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "observer", stop: false, observations: observations.clone() }),
    ]);
    let mut context = run();
    middleware.after_model(&mut context, &(), &mut ModelResponse::assistant("first")).await.unwrap();
    <StopHookMiddleware<Metrics> as Middleware<(), ()>>::on_error(
        &middleware, &mut context, &TinyAgentsError::Model("failed".into()),
    ).await.unwrap();
    middleware.after_model(&mut context, &(), &mut ModelResponse::assistant("second")).await.unwrap();
    assert_eq!(*observations.lock().unwrap(), vec![("observer", 1, 1), ("observer", 1, 1)]);
}

/// A stopping policy must not silently continue when its pause cannot be delivered.
#[tokio::test]
async fn stop_hooks_require_a_run_steering_handle_before_observing_calls() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "budget", stop: true, observations: observations.clone() }),
    ]);
    let mut context = RunContext::new(RunConfig::new("unsteered"), ());
    let error = middleware.after_model(&mut context, &(), &mut ModelResponse::assistant("done"))
        .await.unwrap_err();
    assert!(matches!(error, TinyAgentsError::Middleware(_)));
    assert!(observations.lock().unwrap().is_empty());
}

/// Successful cleanup also makes a reused context begin with an empty tally.
#[tokio::test]
async fn stop_hook_success_cleanup_restarts_the_completed_call_tally() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "observer", stop: false, observations: observations.clone() }),
    ]);
    let mut context = run();
    middleware.after_model(&mut context, &(), &mut ModelResponse::assistant("first")).await.unwrap();
    middleware.after_agent(&mut context, &(), &mut crate::middleware::AgentRun::new()).await.unwrap();
    middleware.after_model(&mut context, &(), &mut ModelResponse::assistant("second")).await.unwrap();
    assert_eq!(*observations.lock().unwrap(), vec![("observer", 1, 1), ("observer", 1, 1)]);
}

/// A harmless read tool gives the model a reason to request a second call.
struct ReadTool;

#[async_trait]
impl tinytools::Tool for ReadTool {
    fn name(&self) -> &str { "read" }
    fn description(&self) -> &str { "Read fixed test data" }
    fn parameters_schema(&self) -> serde_json::Value { serde_json::json!({"type": "object"}) }
    fn policy(&self) -> tinytools::ToolPolicy { tinytools::ToolPolicy::read_only() }
    async fn execute(&self, _: serde_json::Value) -> anyhow::Result<tinytools::ToolResult> {
        Ok(tinytools::ToolResult::success("fixed result"))
    }
}

/// The hook stops the real loop before a second provider call can be dispatched.
#[tokio::test]
async fn stop_hook_pause_prevents_the_next_model_call_in_the_native_loop() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(tinyinference_llm::providers::MockModel::with_tool_call(
        "read", serde_json::json!({}),
    ));
    let mut harness: crate::runtime::AgentHarness<()> = crate::runtime::AgentHarness::new();
    harness.register_model("model", model.clone());
    harness.register_tool(Arc::new(ReadTool));
    harness.push_middleware(Arc::new(StopHookMiddleware::<Metrics>::new("model", vec![
        Arc::new(Hook { name: "budget", stop: true, observations: observations.clone() }),
    ])));
    let outcome = harness.invoke_in_context_with_status(
        &(), run(), vec![Message::user("read and continue")],
    ).await.unwrap();
    assert_eq!(model.call_count(), 1);
    assert_eq!(outcome.run.model_calls, 1);
    assert_eq!(outcome.run.tool_calls, 1, "the completed call keeps its pending tool round");
    assert_eq!(*observations.lock().unwrap(), vec![("budget", 1, 1)]);
}
