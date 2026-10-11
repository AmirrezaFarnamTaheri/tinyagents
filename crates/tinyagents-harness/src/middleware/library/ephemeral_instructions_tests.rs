use super::*;
use async_trait::async_trait;
use crate::context::{RunConfig, RunContext};
use crate::middleware::{AgentRun, Middleware};
use crate::error::TinyAgentsError;
use tinyinference_llm::message::Message;
use tinyinference_llm::model::{ModelProfile, ModelRequest};

fn context() -> RunContext { RunContext::new(RunConfig::new("run"), ()) }
fn request() -> ModelRequest {
    ModelRequest { messages: vec![Message::system("persona"), Message::user("read"), Message::tool("call", "original")], ..Default::default() }
}

/// Delivery occurs once in FIFO order on an ephemeral request copy.
#[tokio::test]
async fn ephemeral_instructions_are_delivered_once_without_changing_the_source_history() {
    let queue = EphemeralInstructionQueue::new();
    queue.push("first");
    queue.push("second");
    let injector = queue.middleware();
    assert!(<EphemeralInstructionMiddleware as Middleware<(), ()>>::is_observer(&injector));
    let original = request();
    let mut outgoing = original.clone();
    injector.before_model(&mut context(), &(), &mut outgoing).await.unwrap();
    assert_eq!(original.messages.len(), 3);
    assert_eq!(outgoing.messages[3].text(), "first");
    assert_eq!(outgoing.messages[4].text(), "second");
    let mut next = original.clone();
    injector.before_model(&mut context(), &(), &mut next).await.unwrap();
    assert_eq!(next.messages, original.messages);
}

/// Hoisting providers keep the prompt head unchanged and receive a tail instruction.
#[tokio::test]
async fn ephemeral_instructions_for_hoisting_models_keep_the_system_prefix_unchanged() {
    let queue = EphemeralInstructionQueue::new();
    queue.push("try a bounded read");
    let injector = queue.middleware();
    let mut context = context();
    context.model_profile = Some(ModelProfile { hoists_system_messages: true, mid_conversation_system_messages: true, ..Default::default() });
    let mut outgoing = request();
    let prefix = outgoing.messages[0].clone();
    injector.before_model(&mut context, &(), &mut outgoing).await.unwrap();
    assert_eq!(outgoing.messages.len(), 3);
    assert_eq!(outgoing.messages[0], prefix);
    let tail = outgoing.messages.last().unwrap().text();
    assert!(tail.starts_with("original"));
    assert!(tail.contains("try a bounded read"));
}

/// A run ending before another request does not carry pending corrections forward.
#[tokio::test]
async fn ephemeral_instructions_are_cleared_after_success_and_error() {
    let queue = EphemeralInstructionQueue::new();
    let injector = queue.middleware();
    queue.push("not sent");
    injector.after_agent(&mut context(), &(), &mut AgentRun::default()).await.unwrap();
    assert!(queue.drain().is_empty());
    queue.push("also not sent");
    <EphemeralInstructionMiddleware as Middleware<(), ()>>::on_error(&injector, &mut context(), &TinyAgentsError::Validation("failed".into())).await.unwrap();
    assert!(queue.drain().is_empty());
}
