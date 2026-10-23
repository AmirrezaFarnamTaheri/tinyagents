use super::*;
use tinystoragedrivers_types::Meta;

#[test]
fn lease_write_condition_pins_holder_epoch_release_state_and_expiry() {
    let lease = Lease {
        id: "lease-id".to_owned(),
        holder: "holder-a".to_owned(),
        epoch: 7,
        expires_ms: 500,
        endpoint: None,
    };
    let condition = lease
        .write_condition(tinystoragedrivers_types::Scope::new("cluster").unwrap())
        .unwrap();
    let [guard] = condition.clauses() else {
        panic!("one lease must produce one guard clause");
    };

    assert_eq!(guard.collection(), LEASES);
    assert_eq!(guard.id(), "lease-id");
    assert_eq!(guard.valid_until_field(), Some("expires_ms"));

    let active = serde_json::json!({
        "holder": "holder-a",
        "epoch": 7,
        "expires_ms": 500
    });
    assert!(guard.matches_at("lease-id", &active, &Meta::unknown(), 499));
    assert!(!guard.matches_at("lease-id", &active, &Meta::unknown(), 500));
    assert!(!guard.matches_at(
        "lease-id",
        &serde_json::json!({
            "holder": "holder-b",
            "epoch": 7,
            "expires_ms": 500
        }),
        &Meta::unknown(),
        499
    ));
    assert!(!guard.matches_at(
        "lease-id",
        &serde_json::json!({
            "holder": "holder-a",
            "epoch": 8,
            "expires_ms": 500
        }),
        &Meta::unknown(),
        499
    ));
    assert!(!guard.matches_at(
        "lease-id",
        &serde_json::json!({
            "holder": "holder-a",
            "epoch": 7,
            "expires_ms": 500,
            "released": true
        }),
        &Meta::unknown(),
        499
    ));
}
