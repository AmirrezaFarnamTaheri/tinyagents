//! End-to-end coverage for middleware lifecycle controls and a preloaded
//! steering cancellation sent directly to a real run.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use tinyagents_graph::*;
use tinyagents_harness::context::{MiddlewareControl, RunConfig, RunContext};
use tinyagents_harness::events::AgentEvent;
use tinyagents_harness::middleware::Middleware;
use tinyagents_harness::runtime::AgentHarness;
use tinyagents_harness::steering::{SteeringCommand, SteeringHandle};
use tinyagents_harness::testkit::{EventRecorder, FakeTool};
use tinyagents_harness::*;
use tinyagents_registry::*;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::ModelResponse;
use tinyinference_llm::providers::MockModel;

// ── Part A: MiddlewareControl ────────────────────────────────────────────────

/// Middleware that stops the run with a fixed final response after the model
/// call, exercising [`MiddlewareControl::StopWithFinal`].
struct StopControlMiddleware;

#[async_trait]
impl Middleware<(), ()> for StopControlMiddleware {
    fn name(&self) -> &str {
        "stop_control"
    }

    async fn after_model(
        &self,
        ctx: &mut RunContext<()>,
        _state: &(),
        _response: &mut ModelResponse,
    ) -> Result<()> {
        ctx.request_control(MiddlewareControl::StopWithFinal("stopped".into()));
        Ok(())
    }
}

/// Middleware that requests an interrupt after the model call, exercising
/// [`MiddlewareControl::Interrupt`].
struct InterruptControlMiddleware;

#[async_trait]
impl Middleware<(), ()> for InterruptControlMiddleware {
    fn name(&self) -> &str {
        "interrupt_control"
    }

    async fn after_model(
        &self,
        ctx: &mut RunContext<()>,
        _state: &(),
        _response: &mut ModelResponse,
    ) -> Result<()> {
        ctx.request_control(MiddlewareControl::Interrupt {
            node: "review".into(),
            message: "needs approval".into(),
        });
        Ok(())
    }
}

/// Middleware that requests two controls in one turn to exercise precedence:
/// the order is configurable, but the higher-precedence [`Interrupt`] must
/// always win over [`StopWithFinal`].
///
/// [`Interrupt`]: MiddlewareControl::Interrupt
/// [`StopWithFinal`]: MiddlewareControl::StopWithFinal
struct TwoControlMiddleware {
    interrupt_first: bool,
}

#[async_trait]
impl Middleware<(), ()> for TwoControlMiddleware {
    fn name(&self) -> &str {
        "two_control"
    }

    async fn after_model(
        &self,
        ctx: &mut RunContext<()>,
        _state: &(),
        _response: &mut ModelResponse,
    ) -> Result<()> {
        let stop = MiddlewareControl::StopWithFinal("stopped".into());
        let interrupt = MiddlewareControl::Interrupt {
            node: "review".into(),
            message: "needs approval".into(),
        };
        if self.interrupt_first {
            ctx.request_control(interrupt);
            ctx.request_control(stop);
        } else {
            ctx.request_control(stop);
            ctx.request_control(interrupt);
        }
        Ok(())
    }
}

/// When two controls are requested in a single turn, the higher-precedence
/// `Interrupt` wins regardless of request order, the run surfaces as
/// [`TinyAgentsError::Interrupted`], and the honored control is journaled as a
/// `control.applied` event carrying the interrupt detail.
#[tokio::test]
async fn higher_precedence_control_wins_and_is_journaled() {
    for interrupt_first in [true, false] {
        let mut harness: AgentHarness<()> = AgentHarness::new();
        harness.with_model_resolver(Arc::new(
            tinyagents_harness::host::FixedModelResolver::named(
                "mock",
                Arc::new(MockModel::constant("hi")),
            ),
        ));
        harness.push_middleware(Arc::new(TwoControlMiddleware { interrupt_first }));

        let recorder = EventRecorder::new();
        let ctx = RunContext::new(RunConfig::new("precedence"), ()).with_events(recorder.sink());

        let err = harness
            .invoke_in_context(&(), ctx, vec![Message::user("go")])
            .await
            .expect_err("the interrupt control must win and surface as an error");

        match err {
            TinyAgentsError::Interrupted { node, message } => {
                assert_eq!(node, "review", "interrupt_first={interrupt_first}");
                assert_eq!(message, "needs approval");
            }
            other => {
                panic!("expected Interrupted, got {other:?} (interrupt_first={interrupt_first})")
            }
        }

        // The honored control was journaled, and it is the interrupt (not the
        // stop) that was applied.
        let applied = recorder.events().into_iter().find_map(|e| match e {
            AgentEvent::ControlApplied { control, detail } => Some((control, detail)),
            _ => None,
        });
        let (control, detail) = applied.expect("a control.applied event must be emitted");
        assert_eq!(control, "interrupt", "the interrupt control was applied");
        assert_eq!(detail, "review: needs approval");
    }
}

#[tokio::test]
async fn stop_with_final_control_ends_run_before_tools() {
    // The model asks for a tool it would normally execute and loop on, but the
    // control middleware stops the run at the checkpoint after the model call.
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(
        tinyagents_harness::host::FixedModelResolver::named(
            "mock",
            Arc::new(MockModel::with_tool_call("lookup", json!({}))),
        ),
    ));
    harness.register_tool(Arc::new(FakeTool::returning("lookup", "out")));
    harness.push_middleware(Arc::new(StopControlMiddleware));

    // Capture events by driving the run inside a context whose sink the recorder
    // observes.
    let recorder = EventRecorder::new();
    let ctx = RunContext::new(RunConfig::new("stop-control"), ()).with_events(recorder.sink());

    let run = harness
        .invoke_in_context(&(), ctx, vec![Message::user("go")])
        .await
        .expect("control stop yields a completed run");

    assert_eq!(
        run.final_response.expect("final response present").text(),
        "stopped",
        "StopWithFinal must set the fixed final text"
    );
    assert_eq!(
        run.tool_calls, 0,
        "the tool must never run because the loop stopped at the checkpoint"
    );
    assert!(
        recorder.kinds().iter().any(|k| k == "run.completed"),
        "a StopWithFinal run must still emit a run.completed event, got {:?}",
        recorder.kinds()
    );
    assert!(
        recorder.kinds().iter().any(|k| k == "control.applied"),
        "honoring a control outcome must be journaled as control.applied, got {:?}",
        recorder.kinds()
    );
}

#[tokio::test]
async fn interrupt_control_surfaces_as_interrupted_error() {
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(
        tinyagents_harness::host::FixedModelResolver::named(
            "mock",
            Arc::new(MockModel::constant("hi")),
        ),
    ));
    harness.push_middleware(Arc::new(InterruptControlMiddleware));

    let err = harness
        .invoke_default(&(), vec![Message::user("go")])
        .await
        .expect_err("Interrupt control must surface as an error");

    match err {
        TinyAgentsError::Interrupted { node, message } => {
            assert_eq!(node, "review");
            assert_eq!(message, "needs approval");
        }
        other => panic!("expected Interrupted, got {other:?}"),
    }
}

#[tokio::test]
async fn preloaded_cancel_steering_cancels_a_real_run() {
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.with_model_resolver(Arc::new(
        tinyagents_harness::host::FixedModelResolver::named(
            "mock",
            Arc::new(MockModel::constant("done")),
        ),
    ));

    // Attach a steering handle already carrying a Cancel command; the agent loop
    // drains it at the checkpoint before the first model call and ends the run.
    let steering = SteeringHandle::allow_all();
    steering.send(SteeringCommand::Cancel);
    let ctx = RunContext::new(RunConfig::new("cancel-steer"), ()).with_steering(steering);

    let err = harness
        .invoke_in_context(&(), ctx, vec![Message::user("go")])
        .await
        .expect_err("a preloaded Cancel steering command must cancel the run");

    assert!(
        matches!(err, TinyAgentsError::Cancelled),
        "steered Cancel must surface as Cancelled, got {err:?}"
    );
}
