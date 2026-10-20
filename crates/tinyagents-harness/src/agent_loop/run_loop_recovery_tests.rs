use std::sync::Arc;

use super::recover_text_dialect_calls;
use crate::agent_loop::dialect::TextRecovery;
use crate::context::{RunConfig, RunContext};
use crate::ids::CallId;
use tinyinference_llm::model::ModelResponse;
use tinyinference_llm::tool::ToolSchema;

fn offered(names: &[&str]) -> TextRecovery {
    TextRecovery {
        offered: Arc::new(
            names
                .iter()
                .map(|name| ToolSchema::new(*name, "", serde_json::json!({"type": "object"})))
                .collect(),
        ),
        registry: None,
        dropped: Arc::default(),
        withhold: false,
        special_tokens_only: false,
    }
}

#[test]
fn text_dialect_markup_is_not_recovered_when_the_request_offered_no_tools() {
    let ctx: RunContext<()> = RunContext::new(RunConfig::new("recovery-test"), ());
    let mut response = ModelResponse::assistant(
        "<tool_call><name>shell</name><arguments>{\"command\":\"id\"}</arguments></tool_call>",
    );

    recover_text_dialect_calls(
        &ctx,
        &mut response,
        &CallId::new("model-1"),
        &TextRecovery::default(),
    );

    assert!(response.message.tool_calls.is_empty());
    assert!(response.text().contains("<tool_call>"));
}

/// I-2 regression: `RunPolicy::text_dialect_recovery` resolving to off
/// (what `Auto` yields for a model whose profile reports native tool
/// calling) is represented as an empty `TextRecovery`, exactly like a
/// turn that offered no tools — so `<tool_call>` markup the model merely
/// quoted must not be executed.
#[test]
fn text_dialect_markup_is_not_recovered_when_the_policy_disables_it() {
    let ctx: RunContext<()> = RunContext::new(RunConfig::new("recovery-test"), ());
    let mut response = ModelResponse::assistant(
        r#"<tool_call>{"name": "shell", "arguments": {"command": "id"}}</tool_call>"#,
    );

    recover_text_dialect_calls(
        &ctx,
        &mut response,
        &CallId::new("model-1"),
        &TextRecovery::default(),
    );

    assert!(response.message.tool_calls.is_empty());
    assert!(response.text().contains("<tool_call>"));
}

/// I-2 regression: a final answer that quotes `<tool_call>` markup inside
/// a language-tagged fenced code block must never be executed, even when
/// recovery is otherwise enabled and tools were offered. (A *bare* fence
/// is not protected: `tinytools-agent` treats it as a wrapped call.)
#[test]
fn text_dialect_markup_inside_a_fenced_code_block_is_never_recovered() {
    let ctx: RunContext<()> = RunContext::new(RunConfig::new("recovery-test"), ());
    let mut response = ModelResponse::assistant(
        "Here is the format:\n```xml\n<tool_call>{\"name\": \"shell\", \"arguments\": {}}</tool_call>\n```\n",
    );

    recover_text_dialect_calls(
        &ctx,
        &mut response,
        &CallId::new("model-1"),
        &offered(&["shell"]),
    );

    assert!(
        response.message.tool_calls.is_empty(),
        "markup quoted inside a fenced code block must not become a real call"
    );
    assert!(response.text().contains("<tool_call>"));
}

/// Sanity check for the fence policy: markup outside any fence is still
/// recovered when the policy and tool offer both allow it.
#[test]
fn text_dialect_markup_outside_a_fenced_code_block_is_recovered() {
    let ctx: RunContext<()> = RunContext::new(RunConfig::new("recovery-test"), ());
    let mut response = ModelResponse::assistant(
        r#"<tool_call>{"name": "shell", "arguments": {"command": "id"}}</tool_call>"#,
    );

    recover_text_dialect_calls(
        &ctx,
        &mut response,
        &CallId::new("model-1"),
        &offered(&["shell"]),
    );

    assert_eq!(response.message.tool_calls.len(), 1);
    assert_eq!(response.message.tool_calls[0].name, "shell");
}

#[test]
fn only_the_last_and_invalid_calls_are_possibly_truncated() {
    use tinyinference_llm::tool::ToolCall;

    let calls = vec![
        ToolCall::new("n1", "a", serde_json::json!({})),
        ToolCall::invalid("n2", "a", "{", "eof"),
        ToolCall::new("n3", "a", serde_json::json!({})),
        ToolCall::new("n3", "a", serde_json::json!({})),
    ];
    let mut positions: Vec<usize> = super::truncated_call_positions(&calls)
        .into_iter()
        .collect();
    positions.sort();
    assert_eq!(
        positions,
        vec![1, 3],
        "positional: a duplicate id does not widen it"
    );
    assert!(super::truncated_call_positions(&[]).is_empty());
}

/// #6733: which text-dialect markup runs as a call, per policy, on the stream
/// and on the unary path. Each row is (policy, the profile's `tool_calling`,
/// or `None` for a model with no profile, reply, streamed, whether it runs).
#[tokio::test]
async fn text_dialect_recovery_policy_on_stream_and_unary() {
    use crate::limits::{LimitBehavior, RunLimits};
    use crate::runtime::TextDialectRecovery::{Auto, Off, On};
    use crate::runtime::{AgentHarness, RunPolicy};
    use crate::testkit::{FakeTool, StreamingMock};
    use tinyinference_llm::message::Message;
    use tinyinference_llm::model::ModelProfile;

    const PROSE: &str =
        r#"Wrap it like <tool_call>{"name": "lookup", "arguments": {}}</tool_call> in your reply."#;
    const BARE_FENCE: &str =
        "```\n<tool_call>{\"name\": \"lookup\", \"arguments\": {}}</tool_call>\n```";
    const DSML: &str = "<｜DSML｜invoke name=\"lookup\"><｜DSML｜parameter name=\"q\" string=\"true\">x</｜DSML｜parameter></｜DSML｜invoke>";
    const PIPE_TAG: &str = r#"<|tool_call>call:{"name": "lookup", "arguments": {}}<tool_call|>"#;
    const XML_FENCE: &str =
        "```xml\n<tool_call>{\"name\": \"lookup\", \"arguments\": {}}</tool_call>\n```";

    for (policy, native, reply, streamed, runs) in [
        // The fix: under `Auto` a native model's plain markup is quoting, so
        // the stream no longer runs it...
        (Auto, Some(true), PROSE, true, false),
        (Auto, Some(true), BARE_FENCE, true, false),
        // ...while its own special tokens are still recovered on the stream.
        (Auto, Some(true), DSML, true, true),
        (Auto, Some(true), PIPE_TAG, true, true),
        // `Off` is off for every model and every spelling.
        (Off, None, PROSE, true, false),
        (Off, Some(true), DSML, true, false),
        // Unchanged: `Auto` still recovers for a model not known to call
        // natively.
        (Auto, None, PROSE, true, true),
        // Unchanged: `On` recovers for a native model, streamed too.
        (On, Some(true), PROSE, true, true),
        // Unchanged: a language-tagged fence is a quoted example even when
        // recovery is on.
        (On, Some(true), XML_FENCE, true, false),
        // Control, unchanged: the unary path.
        (Auto, Some(true), BARE_FENCE, false, false),
        (On, Some(true), PROSE, false, true),
    ] {
        let tool = Arc::new(FakeTool::new("lookup"));
        let mut model = StreamingMock::from_text_chunks([reply]);
        if let Some(tool_calling) = native {
            model = model.with_profile(ModelProfile {
                tool_calling,
                ..ModelProfile::default()
            });
        }
        let mut harness: AgentHarness<()> = AgentHarness::new();
        harness.with_model_resolver(Arc::new(tinyagents_harness::host::FixedModelResolver::new(Arc::new(model))));
        harness.register_tool(tool.clone());
        harness.with_policy(RunPolicy {
            text_dialect_recovery: policy,
            limits: RunLimits {
                max_model_calls: 1,
                behavior: LimitBehavior::StopWithPartial,
                ..RunLimits::default()
            },
            ..RunPolicy::default()
        });
        let input = vec![Message::user("go")];
        // The one-call cap stops the run cleanly after a dispatched call, so a
        // row that runs nothing cannot pass on a failed run.
        let result = if streamed {
            harness
                .invoke_streaming(&(), (), RunConfig::new("policy-row"), input)
                .await
        } else {
            harness.invoke_default(&(), input).await
        };
        assert!(result.is_ok(), "run failed: {:?}", result.err());
        assert_eq!(
            !tool.calls().is_empty(),
            runs,
            "policy {policy:?}, tool_calling {native:?}, streamed {streamed}: {reply}"
        );
        // Markup the stream scrubbed must not come back in the final text.
        if streamed && !runs && policy == Auto && native == Some(true) {
            let text = result.unwrap().text().unwrap_or_default();
            assert!(
                !text.contains("tool_call>"),
                "scrubbed markup came back: {text}"
            );
        }
    }
}
