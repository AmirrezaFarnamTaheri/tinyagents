use super::*;
use crate::context::{MiddlewareControl, RunConfig, RunContext};
use crate::middleware::{Middleware, StopHook};
use std::sync::Mutex;
use tinyinference_llm::model::ModelResponse;

struct VoteHook {
    name: &'static str,
    vote: StopDecision,
    order: Arc<Mutex<Vec<&'static str>>>,
    observed: Option<Arc<Mutex<Vec<(u32, u32, u32, String)>>>>,
}

#[async_trait]
impl StopHook<TurnCost> for VoteHook {
    fn name(&self) -> &str {
        self.name
    }

    async fn check(&self, state: &TurnState<'_, TurnCost>) -> StopDecision {
        self.order.lock().unwrap().push(self.name);
        if let Some(observed) = &self.observed {
            observed.lock().unwrap().push((
                state.iteration,
                state.max_iterations,
                state.cost.call_count,
                state.model.to_owned(),
            ));
        }
        self.vote.clone()
    }
}

fn hook(
    name: &'static str,
    vote: StopDecision,
    order: Arc<Mutex<Vec<&'static str>>>,
) -> Arc<dyn StopHook<TurnCost>> {
    Arc::new(VoteHook {
        name,
        vote,
        order,
        observed: None,
    })
}

fn response() -> ModelResponse {
    ModelResponse {
        message: tinyinference_llm::message::AssistantMessage::default(),
        usage: Some(tinyinference_llm::usage::Usage {
            input_tokens: 10,
            output_tokens: 2,
            ..Default::default()
        }),
        finish_reason: None,
        raw: None,
        resolved_model: None,
        continue_turn: None,
        served_from_cache: false,
        correlation: None,
        resolved_route: None,
    }
}

#[tokio::test]
async fn continue_vote_keeps_the_run_open_and_hooks_run_in_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::new(
        "model-a",
        vec![
            hook("first", StopDecision::Continue, order.clone()),
            hook("second", StopDecision::Continue, order.clone()),
        ],
    );
    let mut ctx = RunContext::new(RunConfig::new("continue"), ());

    middleware
        .after_model(&mut ctx, &(), &mut response())
        .await
        .unwrap();

    assert_eq!(*order.lock().unwrap(), ["first", "second"]);
    assert!(ctx.take_control().is_none());
}

#[tokio::test]
async fn stop_vote_uses_seeded_metrics_and_reports_the_winning_hook() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer_reports = Arc::new(Mutex::new(Vec::new()));
    let first = VoteHook {
        name: "budget",
        vote: StopDecision::Stop {
            reason: "budget reached".into(),
        },
        order: order.clone(),
        observed: Some(observed.clone()),
    };
    let middleware = StopHookMiddleware::with_metrics_seed(
        "model-b",
        vec![Arc::new(first)],
        TurnCost {
            call_count: 4,
            ..Default::default()
        },
    )
    .with_stop_observer({
        let observer_reports = observer_reports.clone();
        move |report| observer_reports.lock().unwrap().push(report)
    });
    let mut ctx = RunContext::new(RunConfig::new("stop"), ());

    middleware
        .after_model(&mut ctx, &(), &mut response())
        .await
        .unwrap();

    assert_eq!(
        *observed.lock().unwrap(),
        [(1, 25, 5, "model-b".to_owned())]
    );
    assert_eq!(
        *observer_reports.lock().unwrap(),
        [StopReport {
            hook: "budget".into(),
            iteration: 1,
            model: "model-b".into(),
            reason: "budget reached".into(),
        }]
    );
    assert!(matches!(
        ctx.take_control(),
        Some(MiddlewareControl::Interrupt { node, message })
            if node == "stop_hook" && message == "budget reached"
    ));
}

#[tokio::test]
async fn first_stop_vote_short_circuits_later_async_hooks() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let middleware = StopHookMiddleware::new(
        "model-c",
        vec![
            hook("continue", StopDecision::Continue, order.clone()),
            hook(
                "stop",
                StopDecision::Stop {
                    reason: "stop now".into(),
                },
                order.clone(),
            ),
            hook("unreached", StopDecision::Continue, order.clone()),
        ],
    );
    let mut ctx = RunContext::new(RunConfig::new("ordering"), ());

    middleware
        .after_model(&mut ctx, &(), &mut response())
        .await
        .unwrap();

    assert_eq!(*order.lock().unwrap(), ["continue", "stop"]);
}
