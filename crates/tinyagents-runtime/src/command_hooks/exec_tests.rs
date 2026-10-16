use super::*;

#[test]
fn empty_stdout_is_a_noop() {
    let output = parse_stdout("   \n").expect("empty stdout is allowed");
    assert!(output.permission.is_none());
}

#[test]
fn plain_json_is_parsed() {
    let output = parse_stdout(r#"{"permission":"deny","agent_message":"no"}"#).unwrap();
    assert!(output.is_deny());
    assert_eq!(output.agent_message.as_deref(), Some("no"));
}

#[test]
fn json_after_log_lines_is_recovered() {
    let stdout = "checking policy...\nstill checking\n{\"permission\":\"allow\"}\n";
    let output = parse_stdout(stdout).unwrap();
    assert_eq!(output.permission, Some(HookPermission::Allow));
}

#[test]
fn unparseable_stdout_is_an_error() {
    let error = parse_stdout("not json at all").unwrap_err();
    assert!(error.contains("not a hook decision object"), "{error}");
}

#[test]
fn truncate_marks_elision() {
    assert_eq!(truncate("abcdef", 3), "abc…");
    assert_eq!(truncate("abc", 3), "abc");
}

mod spawner {
    use super::*;
    use crate::command_hooks::config::{HookLayer, LoopLimit};
    use crate::command_hooks::environment::{CommandOutput, CommandRequest, CommandSpawner};
    use crate::command_hooks::types::{HookEvent, HookPayload, ShellPayload};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct Scripted {
        answer: Result<CommandOutput, String>,
        seen: Mutex<Vec<CommandRequest>>,
    }

    #[async_trait]
    impl CommandSpawner for Scripted {
        async fn run(&self, request: CommandRequest) -> Result<CommandOutput, String> {
            self.seen.lock().unwrap().push(request);
            self.answer.clone()
        }
    }

    fn hook(fail_closed: bool) -> HookDefinition {
        HookDefinition {
            command: "./never-spawned-locally.sh".into(),
            kind: HookKind::Command,
            timeout: Some(5),
            matcher: None,
            loop_limit: LoopLimit::Default,
            fail_closed,
            model: None,
            enabled: true,
            layer: Some(HookLayer::Project),
            source_dir: None,
        }
    }

    fn input() -> HookInput {
        HookInput {
            hook_event_name: HookEvent::PreToolUse.as_str().into(),
            conversation_id: None,
            generation_id: None,
            session_id: None,
            model: None,
            agent_id: None,
            openhuman_version: "test".into(),
            workspace_roots: vec![],
            cwd: None,
            payload: HookPayload::Shell(ShellPayload {
                command: "ls".into(),
                cwd: None,
                output: None,
                duration_ms: None,
            }),
        }
    }

    async fn run_with(spawner: Arc<Scripted>, hook: &HookDefinition) -> HookRun {
        let environment = HookEnvironment::new("Test", None).with_command_spawner(spawner);
        run(
            hook,
            &input(),
            &BTreeMap::from([("K".to_string(), "V".to_string())]),
            Duration::from_secs(5),
            &environment,
        )
        .await
    }

    #[tokio::test]
    async fn a_spawner_runs_the_hook_instead_of_the_local_shell() {
        let spawner = Arc::new(Scripted {
            answer: Ok(CommandOutput {
                exit_code: Some(0),
                stdout: br#"{"permission":"deny","agent_message":"jailed"}"#.to_vec(),
                stderr: vec![],
            }),
            seen: Mutex::default(),
        });
        let result = run_with(spawner.clone(), &hook(false)).await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(result.output.is_deny());
        let seen = spawner.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].command, "./never-spawned-locally.sh");
        assert_eq!(seen[0].env.get("K").map(String::as_str), Some("V"));
        assert!(!seen[0].stdin.is_empty(), "the event JSON is handed over");
    }

    #[tokio::test]
    async fn spawner_exit_code_two_denies_with_stderr() {
        let spawner = Arc::new(Scripted {
            answer: Ok(CommandOutput {
                exit_code: Some(2),
                stdout: vec![],
                stderr: b"blocked".to_vec(),
            }),
            seen: Mutex::default(),
        });
        let result = run_with(spawner, &hook(false)).await;
        assert!(result.output.is_deny());
        assert_eq!(result.output.agent_message.as_deref(), Some("blocked"));
    }

    #[tokio::test]
    async fn a_refusing_spawner_fails_open_or_closed_per_definition() {
        let refuse = || {
            Arc::new(Scripted {
                answer: Err("no isolation available".into()),
                seen: Mutex::default(),
            })
        };
        let open = run_with(refuse(), &hook(false)).await;
        assert!(!open.output.is_deny());
        assert!(open.error.unwrap().contains("no isolation available"));
        let closed = run_with(refuse(), &hook(true)).await;
        assert!(closed.output.is_deny());
    }

    #[tokio::test]
    async fn a_signal_death_is_a_failure_not_success() {
        let spawner = Arc::new(Scripted {
            answer: Ok(CommandOutput::default()),
            seen: Mutex::default(),
        });
        let result = run_with(spawner, &hook(false)).await;
        assert!(result.error.unwrap().contains("signal"));
    }
}
