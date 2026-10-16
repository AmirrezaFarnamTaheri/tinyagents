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
