use super::*;

#[tokio::test]
async fn budget_hook_stops_at_the_known_cost_cap() {
    let mut cost = TurnCost::new();
    cost.cost.known_usd = 1.25;
    let state = TurnState { iteration: 2, max_iterations: 10, cost: &cost, model: "test" };
    assert!(matches!(BudgetStopHook::new(1.0).check(&state).await, StopDecision::Stop { .. }));
    assert_eq!(BudgetStopHook::new(2.0).check(&state).await, StopDecision::Continue);
}

#[tokio::test]
async fn invalid_cost_cap_stops_with_a_configuration_reason() {
    let state = TurnState { iteration: 1, max_iterations: 10, cost: &TurnCost::new(), model: "test" };
    assert!(matches!(BudgetStopHook::new(f64::NAN).check(&state).await, StopDecision::Stop { reason } if reason.contains("invalid budget")));
}
