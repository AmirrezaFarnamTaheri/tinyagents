use super::*;

#[test]
fn outcome_metadata_round_trips_and_legacy_records_default_it() {
    let mut outcome = SubagentOutcome::completed("task", "done");
    outcome.worker_thread_id = Some("worker-thread".into());
    outcome.tier = Some("worker".into());
    outcome.model_override = Some("provider/model".into());
    outcome.cost_usd = Some(0.125);
    let value = serde_json::to_value(&outcome).unwrap();
    assert_eq!(value["worker_thread_id"], "worker-thread");
    assert_eq!(value["tier"], "worker");
    assert_eq!(value["model_override"], "provider/model");
    assert_eq!(value["cost_usd"], 0.125);
    assert_eq!(
        serde_json::from_value::<SubagentOutcome>(value).unwrap(),
        outcome
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
}
