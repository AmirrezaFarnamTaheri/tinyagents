use super::*;
use crate::context::{RunConfig, RunContext};
use crate::error::TinyAgentsError;
use crate::middleware::{AgentRun, Middleware};
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ModelProfile, ModelRequest};

fn context() -> RunContext {
    RunContext::new(RunConfig::new("run"), ())
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![
            Message::system("persona"),
            Message::user("read"),
            Message::tool("call", "original"),
        ],
        ..Default::default()
    }
}

/// Delivery occurs once in FIFO order on an ephemeral request copy.
#[tokio::test]
async fn ephemeral_instructions_are_delivered_once_without_changing_the_source_history() {
    let mut context = context();
    let queue = EphemeralInstructionQueue::new();
    queue.push(&context, "first");
    queue.push(&context, "second");
    let injector = queue.middleware();
    assert!(<EphemeralInstructionMiddleware as Middleware<(), ()>>::is_observer(&injector));
    let original = request();
    let mut outgoing = original.clone();
    injector
        .before_model(&mut context, &(), &mut outgoing)
        .await
        .unwrap();
    assert_eq!(original.messages.len(), 3);
    assert_eq!(outgoing.messages[3].text(), "first");
    assert_eq!(outgoing.messages[4].text(), "second");
    let mut next = original.clone();
    injector
        .before_model(&mut context, &(), &mut next)
        .await
        .unwrap();
    assert_eq!(next.messages, original.messages);
}

/// Hoisting providers keep the prompt head unchanged and receive a tail instruction.
#[tokio::test]
async fn ephemeral_instructions_for_hoisting_models_keep_the_system_prefix_unchanged() {
    let mut context = context();
    let queue = EphemeralInstructionQueue::new();
    queue.push(&context, "try a bounded read");
    let injector = queue.middleware();
    context.model_profile = Some(ModelProfile {
        hoists_system_messages: true,
        mid_conversation_system_messages: true,
        ..Default::default()
    });
    let mut outgoing = request();
    let prefix = outgoing.messages[0].clone();
    injector
        .before_model(&mut context, &(), &mut outgoing)
        .await
        .unwrap();
    assert_eq!(outgoing.messages.len(), 3);
    assert_eq!(outgoing.messages[0], prefix);
    let tail = outgoing.messages.last().unwrap().text();
    assert!(tail.starts_with("original"));
    assert!(tail.contains("try a bounded read"));
}

/// A run ending before another request does not carry pending corrections forward.
#[tokio::test]
async fn ephemeral_instructions_are_cleared_after_success_and_error() {
    let mut context = context();
    let queue = EphemeralInstructionQueue::new();
    let injector = queue.middleware();
    queue.push(&context, "not sent");
    injector
        .after_agent(&mut context, &(), &mut AgentRun::default())
        .await
        .unwrap();
    assert!(queue.drain_all().is_empty());
    queue.push(&context, "also not sent");
    <EphemeralInstructionMiddleware as Middleware<(), ()>>::on_error(
        &injector,
        &mut context,
        &TinyAgentsError::Validation("failed".into()),
    )
    .await
    .unwrap();
    assert!(queue.drain_all().is_empty());
}

/// Concurrent contexts with the same durable id never consume each other's corrections.
#[tokio::test]
async fn ephemeral_instructions_are_isolated_by_context_instance() {
    let queue = EphemeralInstructionQueue::new();
    let mut first = context();
    let mut second = context();
    queue.push(&first, "first run");
    queue.push(&second, "second run");
    let injector = queue.middleware();
    let mut outgoing = request();
    injector
        .before_model(&mut first, &(), &mut outgoing)
        .await
        .unwrap();
    assert_eq!(outgoing.messages.last().unwrap().text(), "first run");
    assert_eq!(queue.drain_for(&second), vec!["second run"]);
    queue.push(&first, "still first");
    <EphemeralInstructionMiddleware as Middleware<(), ()>>::on_error(
        &injector,
        &mut second,
        &TinyAgentsError::Validation("failed".into()),
    )
    .await
    .unwrap();
    assert_eq!(queue.drain_for(&first), vec!["still first"]);
}

/// Dropped invocations leave no correction for a later context admission.
#[tokio::test]
async fn ephemeral_instructions_reclaim_abandoned_contexts_on_admission() {
    let queue = EphemeralInstructionQueue::new();
    {
        let abandoned = context();
        queue.push(&abandoned, "stale");
    }
    let mut live = context();
    queue.push(&live, "live");
    queue
        .middleware()
        .before_agent(&mut live, &())
        .await
        .unwrap();
    assert_eq!(queue.drain_all(), vec!["live"]);
}
