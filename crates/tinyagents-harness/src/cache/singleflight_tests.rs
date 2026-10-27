use super::*;

#[tokio::test]
async fn shared_flight_publishes_typed_success_and_error_to_claimants() {
    let flights = SharedFlightMap::<String, usize, &'static str>::new();
    let (leader, is_leader) = flights.claim("task".to_owned());
    assert!(is_leader);
    let (follower, is_follower_leader) = flights.clone().claim("task".to_owned());
    assert!(!is_follower_leader);

    leader.complete(Ok(17));
    assert_eq!(follower.wait().await, Ok(17));
    flights.retire(&"task".to_owned(), &leader);

    let (next, is_next_leader) = flights.claim("task".to_owned());
    assert!(is_next_leader);
    let (next_follower, _) = flights.claim("task".to_owned());
    next.complete(Err("typed failure"));
    assert_eq!(next_follower.wait().await, Err("typed failure"));
}

#[tokio::test]
async fn finished_leader_drop_preserves_the_published_success() {
    let flight = SingleFlight::new();
    let (entry, leader) = flight.inflight.claim("finished".to_owned());
    assert!(leader);
    let (follower, follower_is_leader) = flight.inflight.claim("finished".to_owned());
    assert!(!follower_is_leader);
    let mut guard = LeaderGuard {
        inflight: flight.inflight.clone(),
        key: "finished".to_owned(),
        entry,
        finished: false,
    };

    guard.finish(&Ok(ModelResponse::assistant("shared success")));
    drop(guard);

    assert_eq!(follower.wait().await.unwrap().text(), "shared success");
    assert!(flight.inflight.get(&"finished".to_owned()).is_none());
}

#[tokio::test]
async fn finished_leader_drop_preserves_failure_for_retrying_followers() {
    let flight = SingleFlight::new();
    let (entry, _) = flight.inflight.claim("failed".to_owned());
    let (follower, _) = flight.inflight.claim("failed".to_owned());
    let mut guard = LeaderGuard {
        inflight: flight.inflight.clone(),
        key: "failed".to_owned(),
        entry,
        finished: false,
    };

    guard.finish(&Err(crate::TinyAgentsError::Model(
        "provider failed".into(),
    )));
    drop(guard);

    assert!(matches!(follower.wait().await, Err(())));
    assert!(flight.inflight.get(&"failed".to_owned()).is_none());
}

#[tokio::test]
async fn dropping_an_unfinished_leader_releases_waiters_to_retry() {
    let flight = SingleFlight::new();
    let (entry, _) = flight.inflight.claim("cancelled".to_owned());
    let (follower, _) = flight.inflight.claim("cancelled".to_owned());
    let guard = LeaderGuard {
        inflight: flight.inflight.clone(),
        key: "cancelled".to_owned(),
        entry,
        finished: false,
    };

    drop(guard);

    assert!(matches!(follower.wait().await, Err(())));
    assert!(flight.inflight.get(&"cancelled".to_owned()).is_none());
}
