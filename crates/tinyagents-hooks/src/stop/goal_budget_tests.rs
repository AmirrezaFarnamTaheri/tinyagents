use super::*;
use std::sync::Arc;
use tinyagents_harness_tools::goals::store;
use tinyagents_harness::store::{InMemoryStore, Store};

#[tokio::test]
async fn active_goal_budget_stops_when_in_flight_usage_reaches_the_cap() {
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    let goal = store::set(&store, "thread", "finish", Some(10))
        .await
        .unwrap();
    let hook = GoalBudgetStopHook::for_goal(Arc::clone(&store), &goal).unwrap();
    let mut cost = TurnCost::new();
    cost.input_tokens = 8;
    cost.output_tokens = 2;
    let state = TurnState {
        iteration: 1,
        max_iterations: 10,
        cost: &cost,
        model: "test",
    };
    assert!(matches!(
        hook.check(&state).await,
        StopDecision::Stop { .. }
    ));
}

#[test]
fn inactive_or_unbudgeted_goals_do_not_install_a_hook() {
    use tinyagents_harness_tools::goals::{ThreadGoal, ThreadGoalStatus};
    let goal = ThreadGoal {
        thread_id: "thread".into(),
        goal_id: "goal".into(),
        objective: "finish".into(),
        status: ThreadGoalStatus::Paused,
        token_budget: Some(10),
        tokens_used: 0,
        time_used_seconds: 0,
        time_used_subsec_nanos: 0,
        created_at_ms: 0,
        updated_at_ms: 0,
    };
    let store: Arc<dyn Store> = Arc::new(InMemoryStore::new());
    assert!(GoalBudgetStopHook::for_goal(store, &goal).is_none());
}
