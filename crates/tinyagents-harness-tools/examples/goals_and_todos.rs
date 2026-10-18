//! Shows the host-facing goal preparation API and model-facing todo tool.
//!
//! Run with `cargo run -p tinyagents-harness-tools --example goals_and_todos`.

use std::sync::Arc;

use tinyagents_harness::store::{InMemoryStore, Store};
use tinyagents_harness_tools::goals::GoalStore;
use tinyagents_harness_tools::todos::TodoTool;
use tinytools::Tool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::default());
    let goals = GoalStore::new(Arc::clone(&store));
    goals
        .set("release-thread", "ship the release", Some(20_000))
        .await?;

    let prepared = goals.prepare_turn("release-thread").await?;
    println!("Goal context: {}", prepared.context_block.unwrap_or_default());
    println!("Budget hook required: {}", prepared.budget_guard.is_some());

    let todo = TodoTool::new(store);
    println!("Todo tool: {}", todo.name());
    Ok(())
}
