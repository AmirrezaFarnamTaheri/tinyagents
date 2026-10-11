use tinyagents_registry::ModelCatalogSnapshot;
use tinyagents_registry::catalog::KNOWN_PROVIDERS;
use tinyagents_registry::pricing::{
    KNOWN_MODEL_PRICING, estimate_cost_usd, flat_catalog_entry, lookup, model_accepts_image_input,
};

#[test]
fn pricing_contract_preserves_decorated_lookup_and_cached_rates() {
    let row = lookup("anthropic/CLAUDE-OPUS-4-8[1m]").unwrap();
    assert_eq!(row.model_id, "claude-opus-4-8");
    assert_eq!(
        estimate_cost_usd("claude-opus-4-8", 1_000_000, 0, 1_000_000),
        0.5
    );
    let entry = flat_catalog_entry(row, "consumer-catalog");
    assert_eq!(entry.source, "consumer-catalog");
    assert_eq!(entry.pricing.input_per_token, Some(5.0 / 1_000_000.0));
    assert!(model_accepts_image_input("minimax/minimax-m3"));
}

#[test]
fn projected_pricing_rows_use_recognized_provider_ids() {
    let snapshot = ModelCatalogSnapshot {
        schema_version: 1,
        snapshot_id: "pricing-test".to_string(),
        created_at: "2026-07-01".to_string(),
        currency: "USD".to_string(),
        unit: "token".to_string(),
        description: None,
        sources: Vec::new(),
        models: KNOWN_MODEL_PRICING
            .iter()
            .map(|price| flat_catalog_entry(price, "pricing-test"))
            .collect(),
    };

    snapshot
        .validate_with_providers(Some(KNOWN_PROVIDERS))
        .expect("every projected pricing provider is recognized");
}
