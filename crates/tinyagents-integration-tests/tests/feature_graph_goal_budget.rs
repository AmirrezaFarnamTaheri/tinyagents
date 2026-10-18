//! Public coverage for goal accounting and budget guard preparation.

use std::sync::Arc;
use std::time::Duration;

use tinyagents_harness::store::{InMemoryStore, Store};
use tinyagents_harness_tools::goals::{BudgetVerdict, GoalStore, ThreadGoalStatus};

#[tokio::test]
async fn prepared_budget_guard_tracks_atomic_accounting_and_limited_status() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::default());
    let goals = GoalStore::new(store);
    goals.set("thread", "finish", Some(10)).await.unwrap();
    let prepared = goals.prepare_turn("thread").await.unwrap();
    let guard = prepared.budget_guard.unwrap();
    assert_eq!(guard.check(&goals.backing_store(), 9).await.unwrap(), BudgetVerdict::Continue);

    let update = goals.account_turn("thread", 6, 0, Duration::from_millis(500)).await.unwrap().unwrap();
    assert_eq!(update.tokens_used, 6);
    assert_eq!(update.status, ThreadGoalStatus::Active);
    assert!(guard.check(&goals.backing_store(), 4).await.unwrap().is_stop());

    let limited = goals.account_turn("thread", 4, 0, Duration::from_millis(500)).await.unwrap().unwrap();
    assert_eq!(limited.status, ThreadGoalStatus::BudgetLimited);
    assert!(limited.over_budget());
    assert_eq!(limited.time_used_subsec_nanos, 0);
}
