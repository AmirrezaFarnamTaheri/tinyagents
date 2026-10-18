use super::*;

use std::sync::Mutex;

use crate::limits::RunLimits;
use crate::middleware::{FinishActivity, VerifyBeforeFinishMiddleware};
use crate::runtime::{AgentHarness, RunPolicy};
use crate::testkit::ScriptedModel;
use serde_json::{Value, json};
use tinyinference_llm::message::Message;
use tinyinference_llm::model::ModelResponse;
use tinyinference_llm::tool::ToolCall;
use tinytools::PermissionLevel;

struct MetadataTool {
    minimum: PermissionLevel,
    external: bool,
}

#[async_trait]
impl Tool for MetadataTool {
    fn name(&self) -> &str {
        "metadata"
    }

    fn description(&self) -> &str {
        "A mixed-permission tool behind a shared registry."
    }

    fn parameters_schema(&self) -> Value {
        json!({"type": "object", "properties": {"dry_run": {"type": "boolean"}}})
    }

    fn permission_level(&self) -> PermissionLevel {
        self.minimum
    }

    fn permission_level_with_args(&self, args: &Value) -> PermissionLevel {
        if self.minimum > PermissionLevel::ReadOnly {
            self.minimum
        } else if args.get("dry_run") == Some(&Value::Bool(true)) {
            PermissionLevel::ReadOnly
        } else {
            PermissionLevel::Write
        }
    }

    fn external_effect(&self) -> bool {
        self.external
    }

    fn external_effect_with_args(&self, args: &Value) -> bool {
        self.external && args.get("dry_run") != Some(&Value::Bool(true))
    }

    async fn execute(&self, _args: Value) -> anyhow::Result<ToolResult> {
        Ok(ToolResult::success("executed"))
    }
}

fn metadata_adapter(minimum: PermissionLevel, external: bool) -> CanonicalSharedToolAdapter {
    let tools: Arc<Vec<Box<dyn Tool>>> =
        Arc::new(vec![Box::new(MetadataTool { minimum, external })]);
    CanonicalSharedToolAdapter::for_name(vec![tools], "metadata").expect("registered target")
}

#[test]
fn shared_adapter_preserves_static_permission_and_external_effect() {
    let adapter = metadata_adapter(PermissionLevel::Dangerous, true);
    assert_eq!(adapter.permission_level(), PermissionLevel::Dangerous);
    assert!(adapter.external_effect());
}

#[test]
fn shared_adapter_forwards_argument_aware_permission_and_external_effect() {
    let adapter = metadata_adapter(PermissionLevel::ReadOnly, true);
    assert_eq!(adapter.permission_level(), PermissionLevel::ReadOnly);
    let preview = json!({"dry_run": true});
    assert_eq!(
        adapter.permission_level_with_args(&preview),
        PermissionLevel::ReadOnly
    );
    assert!(!adapter.external_effect_with_args(&preview));
    for args in [
        json!({}),
        json!({"dry_run": false}),
        json!({"dry_run": "true"}),
        json!({"dry_run": null}),
    ] {
        assert_eq!(
            adapter.permission_level_with_args(&args),
            PermissionLevel::Write
        );
        assert!(adapter.external_effect_with_args(&args));
    }
}

#[test]
fn unresolved_shared_adapter_does_not_claim_read_only_or_effect_free() {
    let mut adapter = metadata_adapter(PermissionLevel::ReadOnly, false);
    adapter.sets.clear();
    assert_eq!(adapter.permission_level(), PermissionLevel::Dangerous);
    assert_eq!(
        adapter.permission_level_with_args(&json!({"dry_run": true})),
        PermissionLevel::Dangerous
    );
    assert!(adapter.external_effect());
    assert!(adapter.external_effect_with_args(&json!({"dry_run": true})));
}

#[tokio::test]
async fn shared_target_finish_activity_distinguishes_preview_from_write() {
    for preview in [true, false] {
        let captured: Arc<Mutex<Vec<FinishActivity>>> = Arc::default();
        let observer = captured.clone();
        let mut call = ModelResponse::assistant("");
        call.message.content.clear();
        call.message.tool_calls = vec![ToolCall::new(
            "shared-call",
            "metadata",
            json!({"dry_run": preview}),
        )];
        call.finish_reason = Some("tool_calls".into());
        let mut harness: AgentHarness<()> = AgentHarness::new();
        harness.register_model(
            "mock",
            Arc::new(ScriptedModel::new(vec![
                call,
                ModelResponse::assistant("draft"),
                ModelResponse::assistant("checked"),
            ])),
        );
        harness.register_tool(Arc::new(metadata_adapter(PermissionLevel::ReadOnly, false)));
        harness.with_policy(RunPolicy {
            limits: RunLimits::default().with_max_model_calls(10),
            ..RunPolicy::default()
        });
        harness.push_middleware(Arc::new(
            VerifyBeforeFinishMiddleware::new("verify").with_trigger(move |activity| {
                observer.lock().unwrap().push(activity.clone());
                activity.executed_with_side_effects("metadata")
            }),
        ));
        let run = harness
            .invoke_default(&(), vec![Message::user("use the shared tool")])
            .await
            .unwrap();
        assert_eq!(
            run.text().as_deref(),
            Some(if preview { "draft" } else { "checked" })
        );
        let observations = captured.lock().unwrap();
        let activity = observations
            .first()
            .expect("finish policy observed this run");
        assert!(activity.executed("metadata"));
        assert_eq!(activity.executed_with_side_effects("metadata"), !preview);
        assert!(run.messages.iter().any(|message| matches!(message,
            Message::Assistant(assistant) if assistant.tool_calls.iter().any(|call|
                call.id == "shared-call" && call.name == "metadata")
        )));
    }
}
