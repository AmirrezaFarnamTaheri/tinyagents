use super::*;

fn counts(input: u64, output: u64, cached: u64) -> CallUsage {
    let mut usage = Usage::new(input, output);
    usage.cache_read_tokens = cached;
    CallUsage {
        usage,
        ..Default::default()
    }
}

fn charged(mut usage: CallUsage, usd: f64) -> CallUsage {
    usage.charged_amount_usd = usd;
    usage.charge_reported = true;
    usage
}

/// $3 / $0.30 / $15 per MTok.
fn sonnet() -> ModelPricing {
    ModelPricing {
        input_per_token: Some(3.0 / 1_000_000.0),
        cache_read_input_per_token: Some(0.30 / 1_000_000.0),
        output_per_token: Some(15.0 / 1_000_000.0),
        ..Default::default()
    }
}

#[test]
fn an_unpriced_model_has_no_estimate() {
    let usage = counts(5_000_000, 30_790, 5_398_592);
    assert_eq!(estimate_call_cost_usd(None, &usage), None);
    assert_eq!(call_cost(None, &usage), CallCost::Unknown);
}

#[test]
fn a_priced_model_is_estimated_at_its_list_rates() {
    let pricing = sonnet();
    // 1M uncached input at $3 + 1M cached at $0.30 + 1M output at $15.
    let usage = counts(2_000_000, 1_000_000, 1_000_000);
    let estimate = estimate_call_cost_usd(Some(&pricing), &usage).expect("priced");
    assert!((estimate - 18.30).abs() < 1e-9, "estimate {estimate}");
    assert_eq!(
        call_cost(Some(&pricing), &usage),
        CallCost::Estimated(estimate)
    );
}

#[test]
fn a_reported_charge_wins_even_when_it_is_zero() {
    let paid = charged(counts(1_000, 100, 0), 0.0123);
    assert_eq!(call_cost(None, &paid), CallCost::Charged(0.0123));
    let free = charged(counts(1_000, 100, 0), 0.0);
    assert_eq!(call_cost(None, &free), CallCost::Charged(0.0));
    assert_eq!(call_cost(None, &counts(1_000, 100, 0)), CallCost::Unknown);
}

#[test]
fn an_estimate_carried_on_the_usage_is_not_taken_for_a_charge() {
    let mut usage = counts(1_000_000, 0, 0);
    usage.charged_amount_usd = 9.99;
    usage.cost_is_estimate = true;
    usage.charge_reported = true;
    assert_eq!(
        call_cost(Some(&sonnet()), &usage),
        CallCost::Estimated(3.0),
        "a carried estimate is re-priced, not trusted as billed"
    );
}

#[test]
fn a_turn_with_any_unknown_call_has_no_total() {
    let pricing = sonnet();
    let mut turn = TurnCost::new();
    turn.add_call(None, &charged(counts(10, 1, 0), 0.5));
    assert_eq!(turn.total_usd(), Some(0.5));
    assert_eq!(turn.cost.source, CostSource::Charged);

    turn.add_call(Some(&pricing), &counts(1_000_000, 0, 0));
    assert_eq!(turn.cost.source, CostSource::Estimated);
    assert!((turn.total_usd().unwrap() - 3.5).abs() < 1e-9);

    turn.add_call(None, &counts(10, 1, 0));
    assert_eq!(turn.cost.source, CostSource::Unknown);
    assert_eq!(turn.total_usd(), None, "an unknown call voids the total");
    assert_eq!(turn.call_count, 3);
    assert_eq!(turn.input_tokens, 1_000_020);
}

#[test]
fn merged_tallies_keep_the_least_certain_source() {
    let mut parent = CostTally::default();
    parent.add(CallCost::Charged(1.0));
    let mut child = CostTally::default();
    child.add(CallCost::Estimated(0.25));
    parent.merge(child);
    assert_eq!(parent.source, CostSource::Estimated);
    assert_eq!(parent.usd(), Some(1.25));

    let mut unknown = CostTally::default();
    unknown.add(CallCost::Unknown);
    parent.merge(unknown);
    assert_eq!(parent.usd(), None);
}

fn response_with(usage: Usage, charge: Option<f64>) -> tinyinference_llm::model::ModelResponse {
    let mut response = tinyinference_llm::model::ModelResponse::assistant("done");
    response.usage = Some(usage);
    response.raw = charge.map(|usd| serde_json::json!({ "total_cost_usd": usd }));
    response
}

#[test]
fn recorded_responses_are_priced_through_the_installed_lookup() {
    use crate::middleware::StopHookMetrics;
    let lookup: PricingLookup = std::sync::Arc::new(|model: &str| (model == "priced").then(sonnet));
    let mut turn = TurnCost::with_pricing(lookup);

    // Provider charge wins, zero or not.
    turn.record_model("unpriced", &response_with(Usage::new(10, 1), Some(0.5)));
    assert_eq!(turn.total_usd(), Some(0.5));
    assert_eq!(turn.cost.source, CostSource::Charged);

    // No charge, priced model: list-rate estimate (1M input at $3).
    turn.record_model("priced", &response_with(Usage::new(1_000_000, 0), None));
    assert_eq!(turn.cost.source, CostSource::Estimated);
    assert!((turn.total_usd().unwrap() - 3.5).abs() < 1e-9);

    // No charge, unpriced model: the total is void.
    turn.record_model("unpriced", &response_with(Usage::new(10, 1), None));
    assert_eq!(turn.total_usd(), None);
    assert_eq!(turn.call_count, 3);
    assert_eq!(turn.input_tokens, 1_000_020);
}

#[test]
fn a_turn_without_a_pricing_lookup_prices_nothing() {
    use crate::middleware::StopHookMetrics;
    let mut turn = TurnCost::new();
    turn.record_model("priced", &response_with(Usage::new(1_000_000, 0), None));
    assert_eq!(turn.total_usd(), None);
}
