use super::*;

#[test]
fn outcome_metadata_round_trips_and_legacy_records_default_it() {
    let mut outcome = SubagentOutcome::completed("task", "done");
    outcome.worker_thread_id = Some("worker-thread".into());
    outcome.tier = Some("worker".into());
    outcome.model_override = Some("provider/model".into());
    outcome.cost_usd = Some(0.125);
    outcome.cost_source = Some(tinyagents_harness::cost::CostSource::Estimated);
    outcome.extensions.insert(
        "host_checkpoint".into(),
        serde_json::json!({"skill_filter_override":"docs", "elapsed_ms":17}),
    );
    let value = serde_json::to_value(&outcome).unwrap();
    assert_eq!(value["worker_thread_id"], "worker-thread");
    assert_eq!(value["tier"], "worker");
    assert_eq!(value["model_override"], "provider/model");
    assert_eq!(value["cost_usd"], 0.125);
    assert_eq!(value["cost_source"], "estimated");
    assert_eq!(value["host_checkpoint"]["elapsed_ms"], 17);
    assert_eq!(
        serde_json::from_value::<SubagentOutcome>(value).unwrap(),
        outcome
    );
    assert_eq!(
        outcome.extension("host_checkpoint").unwrap()["skill_filter_override"],
        "docs"
    );

    let legacy = serde_json::json!({
        "task_id": "legacy",
        "output": "done",
        "history": [],
        "status": "Completed",
        "usage": { "calls": 0, "usage": {
            "input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0,
            "cache_write_tokens": 0, "charged_amount": null
        }},
        "artifacts": []
    });
    let decoded: SubagentOutcome = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.worker_thread_id, None);
    assert_eq!(decoded.tier, None);
    assert_eq!(decoded.model_override, None);
    assert_eq!(decoded.cost_usd, None);
    assert_eq!(decoded.cost_source, None);
}

#[test]
fn outcome_extensions_capture_unknown_legacy_fields() {
    let mut value = serde_json::to_value(SubagentOutcome::completed("task", "done")).unwrap();
    value["skill_filter_override"] = serde_json::json!("docs");
    let outcome: SubagentOutcome = serde_json::from_value(value).unwrap();
    assert_eq!(
        outcome.extension("skill_filter_override"),
        Some(&serde_json::json!("docs"))
    );
}
