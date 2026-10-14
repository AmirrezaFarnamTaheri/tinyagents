#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::turn_driver::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyagents_harness::context::RunContext;
use tinyagents_harness::error::Result as HarnessResult;
use tinyagents_harness::middleware::{
    Middleware, PolicyDecision, ToolCallPolicy, ToolInvocationIdentity,
};
use tinyagents_harness::run_queue::{QueueLane, QueuedMessage, RunQueue};
use tinyagents_harness::steering::{SteeringCommandKind, SteeringHandle};
use tinyagents_harness::testkit::{FakeTool, ScriptedModel, text_response, tool_call_response};
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ChatModel, ModelRequest};
use tinyinference_llm::tool::ToolCall;
use tinytools::{Tool, ToolResult};

use crate::{
    DriverRequest, RuntimeError, Session, SessionBuilder, SessionTurnOutcome, SessionTurnRequest,
    ToolSnapshot, TurnOptions,
};
use tinyagents_harness::tool::ToolExecScope;

type Configure = Arc<dyn Fn(&mut TurnPlan<(), ()>) + Send + Sync>;

fn call(id: &str, name: &str, n: u32) -> ToolCall {
    ToolCall::new(id, name, serde_json::json!({ "n": n }))
}

fn snapshot_of(tools: &[Box<dyn Tool>], names: &[&str]) -> ToolSnapshot {
    ToolSnapshot::new(
        tools
            .iter()
            .filter(|tool| names.contains(&tool.name()))
            .map(|tool| tool.spec())
            .collect(),
    )
    .unwrap()
}

/// A session over a [`TurnDriver`] that plans `model` + `tools` and lets the
/// test adjust the plan. `visible` names the tools the session froze.
fn session(
    model: &Arc<ScriptedModel>,
    tools: Vec<Box<dyn Tool>>,
    visible: &[&str],
    configure: Configure,
) -> Session<()> {
    let snapshot = snapshot_of(&tools, visible);
    let set: SharedToolSet = Arc::new(tools);
    let model = model.clone();
    let planner = move |_: &DriverRequest<()>| {
        let chat: Arc<dyn ChatModel<()>> = model.clone();
        let mut plan = TurnPlan::new(vec![("scripted".to_string(), chat)]);
        plan.tools = ToolSetBuilder::new().set(set.clone());
        configure(&mut plan);
        Ok(plan)
    };
    let driver = TurnDriver::new(Arc::new(planner), Arc::new(()));
    SessionBuilder::new(Arc::new(driver))
        .tool_snapshot(snapshot)
        .build()
        .unwrap()
}

fn no_configure() -> Configure {
    Arc::new(|_| {})
}

async fn turn(
    session: &mut Session<()>,
    controls: TurnControls,
) -> Result<SessionTurnOutcome, RuntimeError> {
    session
        .turn(
            SessionTurnRequest::new(Message::user("go")),
            TurnOptions {
                controls,
                ..TurnOptions::default()
            },
        )
        .await
}

fn all_text(request: &ModelRequest) -> String {
    request
        .messages
        .iter()
        .map(Message::text)
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Default)]
struct CountingSink(AtomicUsize);

impl TurnCompletionSink for CountingSink {
    fn turn_completed(&self, _: &TurnReport) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn with_sink(sink: &Arc<CountingSink>) -> Configure {
    let sink = sink.clone();
    Arc::new(move |plan| plan.completion = Some(sink.clone()))
}

#[tokio::test]
async fn early_exit_tool_pauses_the_loop_and_surfaces_its_output() {
    let model = Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "ask_user", 1)),
        text_response("must never be requested"),
    ]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::returning("ask_user", "Which file?"))];
    let mut session = session(&model, tools, &["ask_user"], no_configure());
    let controls = TurnControls {
        early_exit_tools: ["ask_user".to_string()].into(),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let outcome = turn(&mut session, controls).await.unwrap();

    assert!(outcome.interrupted, "an early exit is a pause");
    assert_eq!(outcome.output.as_deref(), Some("Which file?"));
    assert_eq!(model.requests().len(), 1, "no model call after the exit");
    let exit = slot.get().unwrap().early_exit.unwrap();
    assert_eq!(
        (exit.tool.as_str(), exit.question.as_str()),
        ("ask_user", "Which file?")
    );
}

#[tokio::test]
async fn a_failing_early_exit_tool_does_not_pause() {
    let model = Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "ask_user", 1)),
        text_response("recovered"),
    ]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::failing("ask_user", "boom"))];
    let mut session = session(&model, tools, &["ask_user"], no_configure());
    let controls = TurnControls {
        early_exit_tools: ["ask_user".to_string()].into(),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let outcome = turn(&mut session, controls).await.unwrap();

    assert!(!outcome.interrupted);
    assert_eq!(outcome.output.as_deref(), Some("recovered"));
    assert!(slot.get().unwrap().early_exit.is_none());
}

#[tokio::test]
async fn turn_completed_fires_once_unless_deferred_to_the_caller() {
    // Not deferred: the driver fires it.
    let sink = Arc::new(CountingSink::default());
    let model = Arc::new(ScriptedModel::replies(vec!["hi"]));
    let mut immediate = session(&model, Vec::new(), &[], with_sink(&sink));
    let controls = TurnControls::default();
    let slot = controls.report.clone();
    turn(&mut immediate, controls).await.unwrap();
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
    assert!(slot.get().unwrap().completed_emitted());

    // Deferred: nothing fires until the caller does, and only once.
    let sink = Arc::new(CountingSink::default());
    let model = Arc::new(ScriptedModel::replies(vec!["hi"]));
    let mut deferred = session(&model, Vec::new(), &[], with_sink(&sink));
    let controls = TurnControls {
        defer_turn_completed: true,
        ..TurnControls::default()
    };
    let slot = controls.report.clone();
    turn(&mut deferred, controls).await.unwrap();
    let report = slot.get().unwrap();
    assert!(report.completion_deferred);
    assert_eq!(
        sink.0.load(Ordering::SeqCst),
        0,
        "the wrap-up is still streaming"
    );
    assert!(report.emit_completed());
    assert!(!report.emit_completed(), "exactly once");
    assert!(!slot.get().unwrap().emit_completed(), "across every clone");
    assert_eq!(sink.0.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct RecordingRegistrar {
    events: Mutex<Vec<String>>,
}

impl SteeringRegistrar for RecordingRegistrar {
    fn register(&self, task_id: &str, handle: &SteeringHandle) {
        let cancel = handle.policy().is_allowed(SteeringCommandKind::Cancel);
        self.events
            .lock()
            .unwrap()
            .push(format!("register:{task_id}:cancel={cancel}"));
    }
    fn deregister(&self, task_id: &str) {
        self.events
            .lock()
            .unwrap()
            .push(format!("deregister:{task_id}"));
    }
}

fn spawn_admission() -> Configure {
    Arc::new(|plan| {
        let tools = std::mem::take(&mut plan.tools);
        plan.tools = tools.admission(crate::ToolAdmission {
            spawn_tools: ["spawn_agent".to_string()].into(),
            ..crate::ToolAdmission::default()
        });
    })
}

fn child_tools() -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(FakeTool::new("read_file")),
        Box::new(FakeTool::new("spawn_agent")),
    ]
}

#[tokio::test]
async fn subagent_scope_refuses_spawn_tools_and_registers_its_steering_handle() {
    let sink = Arc::new(CountingSink::default());
    let registrar = Arc::new(RecordingRegistrar::default());
    let model = Arc::new(ScriptedModel::replies(vec!["child done"]));
    let configure = {
        let (spawn, sink) = (spawn_admission(), with_sink(&sink));
        Arc::new(move |plan: &mut TurnPlan<(), ()>| {
            spawn(plan);
            sink(plan);
        })
    };
    // The session froze only `read_file`: the child must not be handed a
    // spawn tool, so the harness's declared tools equal the snapshot.
    let mut child = session(&model, child_tools(), &["read_file"], configure);
    let controls = TurnControls {
        scope: TurnScope::Subagent(SubagentScope {
            agent_id: "researcher".into(),
            task_id: "task-7".into(),
        }),
        registrar: Some(registrar.clone()),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let outcome = turn(&mut child, controls).await.unwrap();

    assert_eq!(outcome.output.as_deref(), Some("child done"));
    let report = slot.get().unwrap();
    assert_eq!(
        report.tools.plan.spawn_refused,
        vec!["spawn_agent".to_string()]
    );
    assert_eq!(report.tools.registered, vec!["read_file".to_string()]);
    assert_eq!(
        *registrar.events.lock().unwrap(),
        vec!["register:task-7:cancel=true", "deregister:task-7"],
        "a child's handle accepts cancel and is gone once the run is over"
    );
    assert_eq!(
        sink.0.load(Ordering::SeqCst),
        0,
        "a child reports through its parent"
    );
    assert!(
        model.requests().len() == 1
            && model.requests()[0]
                .tools
                .iter()
                .all(|t| t.name != "spawn_agent")
    );
}

#[tokio::test]
async fn root_scope_keeps_spawn_tools_and_steering_is_not_cancelable() {
    let registrar = Arc::new(RecordingRegistrar::default());
    let model = Arc::new(ScriptedModel::replies(vec!["root done"]));
    let mut root = session(
        &model,
        child_tools(),
        &["read_file", "spawn_agent"],
        spawn_admission(),
    );
    let controls = TurnControls {
        registrar: Some(registrar.clone()),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    turn(&mut root, controls).await.unwrap();

    assert!(slot.get().unwrap().tools.plan.spawn_refused.is_empty());
    assert!(
        registrar.events.lock().unwrap().is_empty(),
        "only children register"
    );
    assert!(
        !TurnScope::Root
            .steering_policy()
            .is_allowed(SteeringCommandKind::Cancel)
    );
}

#[derive(Debug)]
struct Queued {
    id: String,
    text: String,
}

impl QueuedMessage for Queued {
    fn id(&self) -> &str {
        &self.id
    }
    fn text(&self) -> &str {
        &self.text
    }
    fn requeued(id: String, text: String, _: &str, _: u64) -> Self {
        Self { id, text }
    }
}

#[tokio::test]
async fn queued_steering_messages_reach_the_first_model_call() {
    let queue = Arc::new(RunQueue::<Queued>::new());
    queue
        .push(
            QueueLane::Steer,
            Queued {
                id: "q1".into(),
                text: "change course".into(),
            },
        )
        .await;
    let model = Arc::new(ScriptedModel::replies(vec!["steered"]));
    let mut session = session(&model, Vec::new(), &[], no_configure());
    let controls = TurnControls {
        steering: Some(Arc::new(RunQueueFeed::silent(queue.clone()))),
        ..TurnControls::default()
    };

    let outcome = turn(&mut session, controls).await.unwrap();

    assert_eq!(outcome.output.as_deref(), Some("steered"));
    let first = all_text(&model.requests()[0]);
    assert!(
        first.contains("[User steering message]: change course"),
        "the pre-run drain lands ahead of the first call: {first}"
    );
    assert_eq!(queue.status().await.total, 0, "delivered, not left behind");
}

struct DenyDanger;

#[async_trait]
impl ToolCallPolicy<()> for DenyDanger {
    fn name(&self) -> &str {
        "test_policy"
    }
    async fn check(&self, _: &RunContext<()>, call: &ToolCall) -> PolicyDecision {
        if call.name == "danger" {
            PolicyDecision::deny("not on this channel")
        } else {
            PolicyDecision::Allow
        }
    }
}

struct CountingTool(Arc<AtomicUsize>, &'static str);

#[async_trait]
impl Tool for CountingTool {
    fn name(&self) -> &str {
        self.1
    }
    fn description(&self) -> &str {
        "counts executions"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    async fn execute(&self, _: serde_json::Value) -> anyhow::Result<ToolResult> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolResult::success("ran"))
    }
}

#[tokio::test]
async fn tool_policy_rejection_is_returned_to_the_model_and_never_executes() {
    let executions = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "danger", 1)),
        text_response("understood"),
    ]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(CountingTool(executions.clone(), "danger"))];
    let configure: Configure = Arc::new(|plan| plan.tool_policy = Some(Arc::new(DenyDanger)));
    let mut session = session(&model, tools, &["danger"], configure);

    let outcome = turn(&mut session, TurnControls::default()).await.unwrap();

    assert_eq!(outcome.output.as_deref(), Some("understood"));
    assert_eq!(
        executions.load(Ordering::SeqCst),
        0,
        "the denied tool never ran"
    );
    let second = all_text(&model.requests()[1]);
    assert!(
        second.contains("denied by policy 'test_policy': not on this channel"),
        "the model sees why: {second}"
    );
}

struct Trace {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    rewrite_result: bool,
}

#[async_trait]
impl Middleware<(), ()> for Trace {
    fn name(&self) -> &str {
        self.name
    }
    async fn before_model(
        &self,
        _: &mut RunContext<()>,
        _: &(),
        _: &mut ModelRequest,
    ) -> HarnessResult<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}.before_model", self.name));
        Ok(())
    }
    async fn after_tool(
        &self,
        _: &mut RunContext<()>,
        _: &(),
        _: &ToolInvocationIdentity,
        result: &mut ToolResult,
    ) -> HarnessResult<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}.after_tool", self.name));
        if self.rewrite_result {
            *result = ToolResult::success("[capped]");
        }
        Ok(())
    }
}

fn traced(log: &Arc<Mutex<Vec<String>>>, rewrite_outer: bool) -> Configure {
    let log = log.clone();
    Arc::new(move |plan| {
        for (name, rewrite_result) in [("outer", rewrite_outer), ("inner", false)] {
            plan.context.push(Arc::new(Trace {
                name,
                log: log.clone(),
                rewrite_result,
            }));
        }
    })
}

#[tokio::test]
async fn context_middleware_runs_in_plan_order_and_unwinds_in_reverse() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "lookup", 1)),
        text_response("done"),
    ]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::returning("lookup", "raw"))];
    let mut session = session(&model, tools, &["lookup"], traced(&log, false));

    turn(&mut session, TurnControls::default()).await.unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "outer.before_model",
            "inner.before_model",
            "inner.after_tool",
            "outer.after_tool",
            "outer.before_model",
            "inner.before_model",
        ]
    );
}

#[tokio::test]
async fn early_exit_records_the_raw_result_before_context_middleware_rewrites_it() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(ScriptedModel::new(vec![tool_call_response(call(
        "c1", "ask", 1,
    ))]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::returning("ask", "raw question"))];
    let mut session = session(&model, tools, &["ask"], traced(&log, true));
    let controls = TurnControls {
        early_exit_tools: ["ask".to_string()].into(),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    turn(&mut session, controls).await.unwrap();

    assert_eq!(
        slot.get().unwrap().early_exit.unwrap().question,
        "raw question"
    );
}

fn looping_model() -> Arc<ScriptedModel> {
    Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "step", 1)),
        tool_call_response(call("c2", "step", 2)),
        tool_call_response(call("c3", "step", 3)),
        text_response("past the cap"),
    ]))
}

fn step_tools() -> Vec<Box<dyn Tool>> {
    vec![Box::new(FakeTool::returning("step", "ok"))]
}

#[tokio::test]
async fn pause_at_cap_ends_the_turn_with_its_partial_transcript() {
    let model = looping_model();
    let mut session = session(&model, step_tools(), &["step"], no_configure());
    let controls = TurnControls {
        max_iterations: Some(2),
        pause_at_cap: true,
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let outcome = turn(&mut session, controls).await.unwrap();

    assert!(outcome.interrupted, "paused, not finished");
    assert_eq!(
        model.requests().len(),
        2,
        "exactly the capped number of calls"
    );
    let report = slot.get().unwrap();
    assert!(report.hit_cap);
    assert_eq!((report.model_calls, report.max_iterations), (2, 2));
    assert!(
        outcome
            .history
            .iter()
            .filter(|m| matches!(m, Message::Tool(_)))
            .count()
            == 2,
        "the completed rounds stay in history"
    );
}

#[tokio::test]
async fn without_pause_at_cap_the_cap_fails_the_run() {
    let model = looping_model();
    let mut session = session(&model, step_tools(), &["step"], no_configure());
    let controls = TurnControls {
        max_iterations: Some(2),
        pause_at_cap: false,
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let result = turn(&mut session, controls).await;

    assert!(matches!(result, Err(RuntimeError::Driver(_))), "{result:?}");
    let report = slot.get().expect("a failed turn still reports");
    assert!(!report.hit_cap);
    assert_eq!(report.max_iterations, 2);
}

#[tokio::test]
async fn a_zero_iteration_cap_means_unset() {
    let model = Arc::new(ScriptedModel::replies(vec!["fine"]));
    let mut session = session(&model, Vec::new(), &[], no_configure());
    let controls = TurnControls {
        max_iterations: Some(0),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    assert_eq!(
        turn(&mut session, controls)
            .await
            .unwrap()
            .output
            .as_deref(),
        Some("fine")
    );
    assert!(slot.get().unwrap().max_iterations > 0);
}

#[tokio::test]
async fn a_snapshot_the_plan_does_not_honour_is_refused() {
    let model = Arc::new(ScriptedModel::replies(vec!["unreachable"]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::new("only"))];
    // Frozen with `only`, but the plan allows nothing.
    let configure: Configure = Arc::new(|plan| {
        let tools = std::mem::take(&mut plan.tools);
        plan.tools = tools.admission(crate::ToolAdmission {
            allowed: Some(Default::default()),
            ..crate::ToolAdmission::default()
        });
    });
    let mut session = session(&model, tools, &["only"], configure);
    let controls = TurnControls::default();
    let slot = controls.report.clone();

    assert_eq!(
        turn(&mut session, controls).await,
        Err(RuntimeError::ToolSnapshotMismatch)
    );
    assert!(model.requests().is_empty());
    let report = slot.get().expect("a refused turn still reports why");
    assert!(report.tools.registered.is_empty());
    assert_eq!(report.tools.plan.candidates, vec!["only".to_string()]);
}

struct Stamp(Arc<Mutex<Vec<&'static str>>>);

impl ToolExecScope for Stamp {
    fn scope<'a>(
        &'a self,
        _: Option<&'a dyn tinytools::ToolRunContext>,
        execution: tinyagents_harness::tool::ToolExecution<'a>,
    ) -> tinyagents_harness::tool::ToolExecution<'a> {
        Box::pin(async move {
            self.0.lock().unwrap().push("enter");
            let result = execution.await;
            self.0.lock().unwrap().push("exit");
            result
        })
    }
}

#[tokio::test]
async fn execution_scope_wraps_every_tool_call() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(ScriptedModel::new(vec![
        tool_call_response(call("c1", "lookup", 1)),
        text_response("done"),
    ]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::returning("lookup", "x"))];
    let configure: Configure = {
        let trace = trace.clone();
        Arc::new(move |plan| {
            let builder = std::mem::take(&mut plan.tools);
            plan.tools = builder.exec_scope(Arc::new(Stamp(trace.clone())));
        })
    };
    let mut session = session(&model, tools, &["lookup"], configure);

    turn(&mut session, TurnControls::default()).await.unwrap();

    assert_eq!(*trace.lock().unwrap(), vec!["enter", "exit"]);
}

#[tokio::test]
async fn the_pause_trips_on_the_stricter_of_config_and_policy_caps() {
    let model = looping_model();
    let configure: Configure = Arc::new(|plan| plan.policy.limits.max_model_calls = 2);
    let mut session = session(&model, step_tools(), &["step"], configure);
    // The run context asks for 100; the policy's 2 is what the harness enforces.
    let mut options = TurnOptions {
        controls: TurnControls {
            pause_at_cap: true,
            ..TurnControls::default()
        },
        ..TurnOptions::default()
    };
    options.run_context.config.max_model_calls = Some(100);
    let slot = options.controls.report.clone();

    let outcome = session
        .turn(SessionTurnRequest::new(Message::user("go")), options)
        .await
        .unwrap();

    assert!(outcome.interrupted, "paused gracefully, not a limit error");
    assert_eq!(slot.get().unwrap().max_iterations, 2);
    assert_eq!(model.requests().len(), 2);
}

#[tokio::test]
async fn a_planner_error_reports_the_scope_it_failed_under() {
    let driver = TurnDriver::<(), ()>::new(
        Arc::new(|_: &DriverRequest<()>| Err(RuntimeError::MissingDependency("model"))),
        Arc::new(()),
    );
    let mut session = SessionBuilder::new(Arc::new(driver)).build().unwrap();
    let controls = TurnControls {
        defer_turn_completed: true,
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    assert!(turn(&mut session, controls).await.is_err());
    assert!(slot.get().unwrap().completion_deferred);
}

/// A typed dispatch built the way the contract asks: over the adapter it is given.
struct OverAdapter(Arc<dyn Tool>);

#[async_trait]
impl tinyagents_harness::tool::ToolDispatch<(), ()> for OverAdapter {
    fn tool(&self) -> Arc<dyn Tool> {
        self.0.clone()
    }
    async fn execute(
        &self,
        _: &(),
        _: tinyagents_harness::ids::CallId,
        arguments: serde_json::Value,
        options: tinytools::ToolCallOptions,
        _: &RunContext<()>,
    ) -> anyhow::Result<ToolResult> {
        self.0.execute_with_context(arguments, options, None).await
    }
}

#[tokio::test]
async fn a_typed_dispatch_over_the_adapter_keeps_early_exit_and_scope() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(ScriptedModel::new(vec![tool_call_response(call(
        "c1", "ask", 1,
    ))]));
    let tools: Vec<Box<dyn Tool>> = vec![Box::new(FakeTool::returning("ask", "need input"))];
    let configure: Configure = {
        let trace = trace.clone();
        Arc::new(move |plan| {
            let builder = std::mem::take(&mut plan.tools);
            plan.tools = builder
                .exec_scope(Arc::new(Stamp(trace.clone())))
                .dispatch(Arc::new(|_, adapter| Some(Arc::new(OverAdapter(adapter)))));
        })
    };
    let mut session = session(&model, tools, &["ask"], configure);
    let controls = TurnControls {
        early_exit_tools: ["ask".to_string()].into(),
        ..TurnControls::default()
    };
    let slot = controls.report.clone();

    let outcome = turn(&mut session, controls).await.unwrap();

    assert!(outcome.interrupted);
    assert_eq!(
        slot.get().unwrap().early_exit.unwrap().question,
        "need input"
    );
    assert_eq!(*trace.lock().unwrap(), vec!["enter", "exit"]);
}
