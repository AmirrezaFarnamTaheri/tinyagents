//! The call after a dead one (`RunPolicy::truncated_empty_reasoning_off`).

use super::*;
use std::sync::Arc;

use crate::runtime::RunPolicy;
use crate::testkit::ScriptedModel;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ModelResponse, ReasoningConfig, ReasoningEffort};
use tinyinference_llm::usage::Usage;

/// A call that reasoned to its output cap and returned nothing.
fn dead(tokens: u64) -> ModelResponse {
    let mut response = crate::testkit::text_response(String::new())
        .with_usage(Usage::new(4, tokens))
        .with_finish_reason("length");
    response.message.content = Vec::new();
    response
}

/// The reasoning effort each request of a run went out with, for a model that
/// dies twice and then answers.
async fn efforts(reasoning_off: bool) -> Vec<Option<ReasoningEffort>> {
    let model = Arc::new(ScriptedModel::new(vec![
        dead(2048),
        dead(2048),
        crate::testkit::text_response("done").with_usage(Usage::new(4, 3)),
    ]));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("mock", Arc::clone(&model) as _);
    harness.with_policy(RunPolicy {
        default_reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
        truncated_empty_reasoning_off: reasoning_off,
        ..RunPolicy::default()
    });
    let ctx = RunContext::new(
        RunConfig::new("dead-call").with_max_turn_output_tokens(2048),
        (),
    );
    let run = harness
        .invoke_in_context(&(), ctx, vec![Message::user("hi")])
        .await
        .expect("the dead calls are recovered");
    assert_eq!(run.text(), Some("done".to_string()));
    model
        .requests()
        .iter()
        .map(|request| request.reasoning.as_ref().and_then(|config| config.effort))
        .collect()
}

#[tokio::test]
async fn the_calls_after_a_dead_one_go_out_with_reasoning_off() {
    assert_eq!(
        efforts(true).await,
        vec![
            Some(ReasoningEffort::High),
            Some(ReasoningEffort::None),
            Some(ReasoningEffort::None),
        ]
    );
}

#[tokio::test]
async fn without_the_policy_every_call_keeps_its_reasoning() {
    assert_eq!(efforts(false).await, vec![Some(ReasoningEffort::High); 3]);
}

/// Efforts for a model that dies, acts, dies again, acts, then answers: each
/// tool call is its own step, so only a run-wide rule can carry reasoning-off
/// past the step it died in.
async fn efforts_across_steps(sticky_after: u32) -> Vec<Option<ReasoningEffort>> {
    let call = |id: &str| {
        crate::testkit::tool_call_response(tinyinference_llm::tool::ToolCall::new(
            id,
            "probe",
            serde_json::json!({}),
        ))
        .with_usage(Usage::new(4, 3))
    };
    let model = Arc::new(ScriptedModel::new(vec![
        dead(2048),
        call("c1"),
        dead(2048),
        call("c2"),
        crate::testkit::text_response("done").with_usage(Usage::new(4, 3)),
    ]));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("mock", Arc::clone(&model) as _);
    harness.register_tool(Arc::new(crate::testkit::FakeTool::new("probe")));
    harness.with_policy(RunPolicy {
        default_reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
        truncated_empty_reasoning_off: true,
        truncated_empty_reasoning_off_sticky_after: sticky_after,
        ..RunPolicy::default()
    });
    let ctx = RunContext::new(
        RunConfig::new("dead-steps").with_max_turn_output_tokens(2048),
        (),
    );
    harness
        .invoke_in_context(&(), ctx, vec![Message::user("hi")])
        .await
        .expect("the run recovers");
    model
        .requests()
        .iter()
        .map(|request| request.reasoning.as_ref().and_then(|config| config.effort))
        .collect()
}

#[tokio::test]
async fn reasoning_off_sticks_for_the_rest_of_the_run_after_repeated_dead_calls() {
    let high = Some(ReasoningEffort::High);
    let none = Some(ReasoningEffort::None);
    // One-call only: reasoning comes back after each recovery.
    assert_eq!(
        efforts_across_steps(0).await,
        vec![high, none, high, none, high]
    );
    // Sticky after the second dead call: the last call stays off too.
    assert_eq!(
        efforts_across_steps(2).await,
        vec![high, none, high, none, none]
    );
}

#[tokio::test]
async fn constant_cap_retries_reach_the_provider_instead_of_replaying_cached_blanks() {
    let model = Arc::new(ScriptedModel::new(vec![
        dead(2048),
        crate::testkit::text_response("recovered"),
    ]));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("mock", model.clone());
    harness.with_response_cache(Arc::new(crate::cache::InMemoryResponseCache::new()));
    harness.with_policy(RunPolicy {
        truncated_empty_constant_cap: true,
        truncated_empty_retries: 1,
        truncated_empty_nudges: 0,
        ..RunPolicy::default()
    });
    let input = vec![Message::user("same question")];
    for run_id in ["recover", "reuse-answer"] {
        let ctx = RunContext::new(
            RunConfig::new(run_id).with_max_turn_output_tokens(2048),
            (),
        );
        let run = harness.invoke_in_context(&(), ctx, input.clone()).await.unwrap();
        assert_eq!(run.text(), Some("recovered".into()));
    }
    assert_eq!(model.requests().len(), 2, "only the usable answer is cached");
}

#[tokio::test]
async fn truncated_empty_recovery_bypasses_a_blank_cached_before_retries_were_enabled() {
    let cache = Arc::new(crate::cache::InMemoryResponseCache::new());
    let input = vec![Message::user("same question")];
    let seed_model = Arc::new(ScriptedModel::new(vec![dead(2048)]));
    let mut seed: AgentHarness<()> = AgentHarness::new();
    seed.register_model("mock", seed_model.clone());
    seed.with_response_cache(cache.clone());
    seed.with_policy(RunPolicy {
        truncated_empty_retries: 0,
        truncated_empty_nudges: 0,
        ..RunPolicy::default()
    });
    for run_id in ["seed-blank", "replay-with-recovery-disabled"] {
        let ctx = RunContext::new(
            RunConfig::new(run_id).with_max_turn_output_tokens(2048),
            (),
        );
        seed.invoke_in_context(&(), ctx, input.clone()).await.unwrap();
    }
    assert_eq!(seed_model.requests().len(), 1, "disabled recovery retains exact replay");

    let model = Arc::new(ScriptedModel::new(vec![crate::testkit::text_response("fresh answer")]));
    let mut harness: AgentHarness<()> = AgentHarness::new();
    harness.register_model("mock", model.clone());
    harness.with_response_cache(cache);
    harness.with_policy(RunPolicy {
        truncated_empty_constant_cap: true,
        truncated_empty_retries: 1,
        truncated_empty_nudges: 0,
        ..RunPolicy::default()
    });
    let ctx = RunContext::new(RunConfig::new("recover-cached-blank").with_max_turn_output_tokens(2048), ());
    let run = harness.invoke_in_context(&(), ctx, input).await.unwrap();
    assert_eq!(run.text(), Some("fresh answer".into()));
    assert_eq!(model.requests().len(), 1, "the prior blank cache hit is bypassed");
}
