use tinyagents_registry::pricing::{
    catalog_entry, estimate_cost_usd, lookup, model_accepts_image_input,
};

#[test]
fn pricing_contract_preserves_decorated_lookup_and_cached_rates() {
    let row = lookup("anthropic/CLAUDE-OPUS-4-8[1m]").unwrap();
    assert_eq!(row.model_id, "claude-opus-4-8");
    assert_eq!(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 0, 1_000_000),
        0.5
    );
    let entry = catalog_entry(row, "consumer-catalog");
    assert_eq!(entry.source, "consumer-catalog");
    assert_eq!(entry.pricing.input_per_token, Some(5.0 / 1_000_000.0));
    assert!(model_accepts_image_input("minimax/minimax-m3"));
}
