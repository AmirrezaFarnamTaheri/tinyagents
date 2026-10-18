use super::*;

#[test]
fn enrichment_json_round_trips_only_content_free_provider_and_pricing_values() {
    let enrichment = GenerationEnrichment {
        provider_id: Some("managed.provider-1".into()),
        pricing: Some(GenerationPricing {
            input_per_mtok_usd: Some(0.2),
            cached_input_per_mtok_usd: Some(0.05),
            output_per_mtok_usd: Some(0.8),
        }),
        estimated_cost_usd: Some(0.001),
    };

    let value = serde_json::to_value(&enrichment).expect("valid metadata serializes");
    assert_eq!(
        serde_json::from_value::<GenerationEnrichment>(value).expect("valid metadata loads"),
        enrichment
    );
}

#[test]
fn enrichment_serde_rejects_invalid_rates_estimates_and_provider_identifiers() {
    for value in [
        serde_json::json!({"estimated_cost_usd": -0.1}),
        serde_json::json!({"pricing": {"input_per_mtok_usd": -0.1}}),
        serde_json::json!({"provider_id": "provider / user-secret"}),
    ] {
        assert!(serde_json::from_value::<GenerationEnrichment>(value).is_err());
    }

    let invalid = GenerationEnrichment {
        provider_id: Some("/private/user-secret".into()),
        pricing: Some(GenerationPricing {
            input_per_mtok_usd: Some(f64::NAN),
            ..Default::default()
        }),
        estimated_cost_usd: Some(f64::INFINITY),
    };
    assert!(serde_json::to_value(invalid).is_err());
}

#[test]
fn generation_enriched_event_has_a_stable_durable_shape() {
    let event = crate::events::AgentEvent::GenerationEnriched {
        call_id: crate::ids::CallId::new("call-1"),
        enrichment: GenerationEnrichment {
            provider_id: Some("managed".into()),
            ..Default::default()
        },
    };
    let value = serde_json::to_value(&event).expect("valid event serializes");
    assert_eq!(value["kind"], "generation_enriched");
    assert_eq!(
        serde_json::from_value::<crate::events::AgentEvent>(value).expect("event loads"),
        event
    );
}

#[test]
fn run_finalized_event_has_a_stable_durable_shape() {
    let event = crate::events::AgentEvent::RunFinalized {
        run_id: crate::ids::RunId::new("run-1"),
    };
    let value = serde_json::to_value(&event).expect("event serializes");
    assert_eq!(value["kind"], "run_finalized");
    assert_eq!(
        serde_json::from_value::<crate::events::AgentEvent>(value).expect("event loads"),
        event
    );
}
