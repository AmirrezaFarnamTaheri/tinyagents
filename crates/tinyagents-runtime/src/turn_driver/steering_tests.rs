use super::*;
use tinyagents_harness::run_queue::QueueLane;
use tinyagents_harness::steering::{SteeringCommand, SteeringHandle};
use tinyinference_llm::message::{ContentBlock, ImageRef, Message, UserMessage};

#[tokio::test]
async fn native_message_feed_primes_complete_user_content_and_requeues_residuals() {
    let queue = Arc::new(RunQueue::new());
    let media = ContentBlock::Image(ImageRef {
        url: "data:image/png;base64,abc".into(),
        mime_type: Some("image/png".into()),
    });
    let original = Message::User(UserMessage {
        content: vec![ContentBlock::Text("look closely".into()), media.clone()],
    });
    queue.push_now(QueueLane::Steer, original.clone());
    queue.push_now(QueueLane::Steer, Message::system("retain unsupported role"));
    queue.push_now(QueueLane::Collect, original);

    let feed = MessageRunQueueFeed::silent(queue.clone());
    let handle = SteeringHandle::allow_all();
    feed.prime(&handle, "thread").await;
    let delivered = handle.drain();
    assert!(matches!(
        delivered.as_slice(),
        [
            SteeringCommand::InjectMessage(Message::User(steer)),
            SteeringCommand::InjectMessage(Message::User(collect))
        ] if steer.content == vec![
            ContentBlock::Text("[User steering message]: look closely".into()),
            media.clone(),
        ] && collect.content == vec![
            ContentBlock::Text("[Additional context from user]: look closely".into()),
            media.clone(),
        ]
    ));
    assert!(matches!(
        queue.drain_now(QueueLane::Steer).as_slice(),
        [Message::System(_)]
    ));

    let guard = feed.arm(handle.clone(), "thread".into(), None);
    handle.send(SteeringCommand::InjectMessage(Message::User(UserMessage {
        content: vec![
            ContentBlock::Text("[Additional context from user]: later".into()),
            media.clone(),
        ],
    })));
    drop(guard);
    assert!(matches!(
        queue.drain_now(QueueLane::Collect).as_slice(),
        [Message::User(user)] if user.content == vec![ContentBlock::Text("later".into()), media]
    ));
}
