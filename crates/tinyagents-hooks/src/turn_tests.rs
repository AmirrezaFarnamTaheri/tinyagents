use super::*;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tinyagents_harness::error::Result;

struct Rendezvous {
    barrier: Arc<tokio::sync::Barrier>,
    completed: Arc<AtomicUsize>,
    done: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl LearningSink for Rendezvous {
    async fn on_turn_complete(&self, _: &TurnSummary) -> Result<()> {
        self.barrier.wait().await;
        if self.completed.fetch_add(1, Ordering::SeqCst) == 1 { self.done.notify_one(); }
        Ok(())
    }
}

#[tokio::test]
async fn learning_sinks_are_dispatched_concurrently() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let completed = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let hooks: Vec<Arc<dyn LearningSink>> = (0..2).map(|_| Arc::new(Rendezvous {
        barrier: barrier.clone(), completed: completed.clone(), done: done.clone(),
    }) as Arc<dyn LearningSink>).collect();
    fire_learning_hooks(&hooks, TurnSummary::new("thread", "agent"));
    tokio::time::timeout(std::time::Duration::from_secs(2), done.notified()).await.expect("both callbacks start concurrently");
    assert_eq!(completed.load(Ordering::SeqCst), 2);
}
