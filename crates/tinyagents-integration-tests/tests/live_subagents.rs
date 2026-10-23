//! LIVE end-to-end: a real OpenAI parent agent composes the answer of a real
//! OpenAI child sub-agent.
//!
//! This is the network-backed sibling of the mock-backed subagent coverage.
//! It uses the public planner/executor lifecycle and `DriverSubAgentTool`,
//! then waits for the detached child through `SubAgentJobsTool`.
//!
//! # Skips gracefully
//!
//! This test is `#[ignore]`d and only runs opted in via
//! `tests/common/live.rs::require_live`, so `cargo test` passes with no key
//! configured and never dials a real provider by accident.

mod common;

#[tokio::test]
#[ignore = "network: set TINYAGENTS_LIVE=1 and run with --ignored"]
async fn live_openai_parent_composes_child_subagent() {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::{Value, json};
    use tinyagents_harness::cancel::CancellationToken;
    use tinyagents_harness::context::{RunConfig, RunContext};
    use tinyagents_harness::runtime::AgentHarness;
    use tinyagents_harness::testkit::{EventRecorder, Trajectory};
    use tinyagents_harness::tool::ToolSnapshot;
    use tinyagents_harness_tools::subagents::{DriverSubAgentTool, SubAgentJobsTool};
    use tinyagents_orchestration::subagent::*;
    use tinyinference_llm::message::Message;
    use tinyinference_llm::model::ChatModel;
    use tinyinference_llm::providers::openai::OpenAiModel;

    if !common::live::require_live(&["OPENAI_API_KEY"]) {
        return;
    }

    #[derive(Default)]
    struct MemoryPersistence {
        terminals: Mutex<HashMap<SubagentTaskKey, SubagentOutcome>>,
        pauses: Mutex<HashMap<SubagentTaskKey, SubagentOutcome>>,
    }

    #[async_trait]
    impl SubagentPersistence for MemoryPersistence {
        async fn load_terminal(
            &self,
            key: &SubagentTaskKey,
        ) -> Result<Option<SubagentOutcome>, SubagentError> {
            Ok(self.terminals.lock().unwrap().get(key).cloned())
        }

        async fn load(
            &self,
            key: &SubagentTaskKey,
        ) -> Result<Option<SubagentResume>, SubagentError> {
            Ok(self
                .pauses
                .lock()
                .unwrap()
                .get(key)
                .and_then(|outcome| match &outcome.status {
                    SubagentOutcomeKind::AwaitingInput(pause) => Some(pause.resume.clone()),
                    _ => None,
                }))
        }

        async fn load_pause(
            &self,
            key: &SubagentTaskKey,
        ) -> Result<Option<SubagentOutcome>, SubagentError> {
            Ok(self.pauses.lock().unwrap().get(key).cloned())
        }

        async fn save_pause(
            &self,
            pause: PersistedSubagentPause,
        ) -> Result<SubagentPausePersistenceDisposition, SubagentError> {
            self.pauses.lock().unwrap().insert(pause.key, pause.outcome);
            Ok(SubagentPausePersistenceDisposition::Inserted)
        }

        async fn record_terminal(
            &self,
            key: &SubagentTaskKey,
            outcome: &SubagentOutcome,
            _: Option<&SubagentResume>,
        ) -> Result<SubagentTerminalPersistenceDisposition, SubagentError> {
            let mut terminals = self.terminals.lock().unwrap();
            if terminals.contains_key(key) {
                return Ok(SubagentTerminalPersistenceDisposition::Existing);
            }
            self.pauses.lock().unwrap().remove(key);
            terminals.insert(key.clone(), outcome.clone());
            Ok(SubagentTerminalPersistenceDisposition::Inserted)
        }
    }

    struct Planner;

    #[async_trait]
    impl SubagentPlanner for Planner {
        async fn prepare(
            &self,
            request: SubagentRequest,
        ) -> Result<PreparedSubagent, SubagentError> {
            let parts = request.into_parts();
            Ok(PreparedSubagent::new(
                parts.task_key.task_id,
                "math_expert",
                vec![
                    Message::system(
                        "You are a meticulous arithmetic engine. Reply with only the numeric answer.",
                    ),
                    Message::user(parts.input),
                ],
                ToolSnapshot::new(vec![]).expect("empty child tool snapshot is valid"),
                parts.run_context,
            ))
        }
    }

    struct Executor {
        child: Arc<AgentHarness<()>>,
    }

    #[async_trait]
    impl SubagentExecutor for Executor {
        async fn execute(
            &self,
            execution: SubagentExecution,
        ) -> Result<SubagentOutcome, SubagentError> {
            let task_id = execution.prepared.task_id;
            let run = self
                .child
                .invoke_in_context(
                    &(),
                    execution.prepared.run_context,
                    execution.prepared.input,
                )
                .await
                .map_err(|error| SubagentError::Execution(error.to_string()))?;
            Ok(SubagentOutcome::completed(
                task_id,
                run.text().unwrap_or_default(),
            ))
        }
    }

    let model: Arc<dyn ChatModel<()>> =
        Arc::new(OpenAiModel::from_env().expect("OPENAI_API_KEY present"));

    let mut child = AgentHarness::new();
    child.with_model(model.clone());
    let child = Arc::new(child);

    let driver = Arc::new(
        SubagentDriver::new(SubagentCapabilities {
            planner: Some(Arc::new(Planner)),
            executor: Some(Arc::new(Executor { child })),
            persistence: Some(Arc::new(MemoryPersistence::default())),
        })
        .expect("all subagent lifecycle capabilities are configured"),
    );
    let jobs = SubAgentJobRegistry::new();
    let request_jobs = jobs.clone();
    let spawn = DriverSubAgentTool::new(
        "spawn_subagent",
        "Start a named expert agent on a task and return its task id.",
        driver,
        move |_: &(),
              parent: &RunContext<()>,
              args: &Value,
              call_id: &tinyagents_harness::ids::CallId| {
            let child = parent
                .child(
                    RunConfig::new(format!("live-child-{}", call_id.as_str())),
                    (),
                )
                .map_err(|error| SubagentError::InvalidRequest(error.to_string()))?;
            SubagentRequest::fresh_from_parent(
                parent,
                child,
                format!("math-expert-{}", call_id.as_str()),
                (),
                args.get("prompt")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                None,
            )
            .map(|request| request.with_target("math_expert"))
        },
        |_| None,
    )
    .with_parameters(json!({
        "type": "object",
        "required": ["agent_id", "prompt"],
        "properties": {
            "agent_id": {"type": "string", "enum": ["math_expert"]},
            "prompt": {"type": "string"}
        }
    }))
    .with_job_registry(request_jobs.clone());

    let mut parent = AgentHarness::new();
    parent.register_tool_dispatch(Arc::new(spawn));
    parent.register_tool_dispatch(Arc::new(SubAgentJobsTool::new(request_jobs.clone())));
    parent.with_model(model);

    let recorder = EventRecorder::new();
    let ctx = RunContext::new(RunConfig::new("live-parent-run"), ()).with_events(recorder.sink());
    let run = parent
        .invoke_in_context(
            &(),
            ctx,
            vec![Message::user(
                "Use spawn_subagent to ask math_expert to compute 17 * 23. Then use subagent_jobs with action=await and the returned task_id to wait for the child. State the numeric result in a short sentence.",
            )],
        )
        .await
        .expect("live parent run succeeds");

    let final_text = run.text().unwrap_or_default();
    assert!(
        !final_text.trim().is_empty(),
        "parent should produce a non-empty composed answer"
    );
    assert!(
        run.model_calls >= 1,
        "parent should make at least one model call"
    );

    let trajectory = Trajectory::from_events(recorder.events());
    trajectory.assert_completed();
    assert!(
        trajectory.tool_was_called("spawn_subagent"),
        "parent should start the real child through DriverSubAgentTool"
    );

    let job = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            if let Some(job) = jobs.list().into_iter().next()
                && job.status.is_terminal()
            {
                return job;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("live math subagent job finishes within two minutes");
    assert!(job.output.is_some(), "completed math job has output");
}
