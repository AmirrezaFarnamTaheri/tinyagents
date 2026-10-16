use super::*;

#[test]
fn a_shared_breaker_blocks_same_key_and_keeps_keys_independent() {
    let first_context = PersistentFailoverBreaker::<String>::default();
    let second_context = first_context.clone();
    let mut first = first_context
        .try_acquire("tenant-a/provider:model".into(), 0)
        .expect("first attempt is admitted");
    assert!(matches!(
        second_context.try_acquire("tenant-a/provider:model".into(), 0),
        Err(BreakerRejection::InFlight)
    ));
    assert!(second_context
        .try_acquire("tenant-b/provider:model".into(), 0)
        .is_ok());

    let failure = first.record_failure(0).expect("failure is recorded");
    assert_eq!(failure.failure_count, 1);
    assert_eq!(failure.next_attempt_ms, 30_000);
    assert!(matches!(
        second_context.try_acquire("tenant-a/provider:model".into(), 29_999),
        Err(BreakerRejection::Backoff { .. })
    ));
    assert!(second_context
        .try_acquire("tenant-a/provider:model".into(), 30_000)
        .is_ok());
}

#[test]
fn dropped_attempt_releases_in_flight_without_recording_a_failure() {
    let breaker = PersistentFailoverBreaker::<&str>::default();
    let attempt = breaker.try_acquire("key", 0).unwrap();
    drop(attempt);
    assert!(breaker.try_acquire("key", 0).is_ok());
}

#[test]
fn failure_backoff_saturates_at_the_cap_and_failure_limit() {
    let breaker = PersistentFailoverBreaker::new(PersistentFailoverBreakerConfig {
        base_backoff_ms: 10,
        max_backoff_ms: 25,
        failure_limit: 3,
    });
    let key = "provider/model";
    let first = breaker.try_acquire(key, 0).unwrap().record_failure(0).unwrap();
    assert_eq!(first.next_attempt_ms, 10);
    assert!(!first.failure_limit_reached);
    let second = breaker
        .try_acquire(key, first.next_attempt_ms)
        .unwrap()
        .record_failure(first.next_attempt_ms)
        .unwrap();
    assert_eq!(second.next_attempt_ms, 30);
    let third = breaker
        .try_acquire(key, second.next_attempt_ms)
        .unwrap()
        .record_failure(second.next_attempt_ms)
        .unwrap();
    assert_eq!(third.next_attempt_ms, 55);
    assert!(third.failure_limit_reached);
    assert!(matches!(
        breaker.try_acquire(key, third.next_attempt_ms - 1),
        Err(BreakerRejection::Backoff { failure_limit_reached: true, .. })
    ));
}

#[test]
fn success_resets_the_streak_and_stale_drop_cannot_release_a_new_attempt() {
    let breaker = PersistentFailoverBreaker::<String>::new(
        PersistentFailoverBreakerConfig {
            base_backoff_ms: 1,
            max_backoff_ms: 10,
            failure_limit: 2,
        },
    );
    let mut old = breaker.try_acquire("key".into(), 0).unwrap();
    old.record_failure(0).unwrap();
    let mut current = breaker.try_acquire("key".into(), 1).unwrap();
    drop(old);
    assert!(matches!(
        breaker.try_acquire("key".into(), 1),
        Err(BreakerRejection::InFlight)
    ));
    current.record_success();
    let mut after_success = breaker.try_acquire("key".into(), 1).unwrap();
    let failure = after_success.record_failure(1).unwrap();
    assert_eq!(failure.failure_count, 1);
    assert_eq!(failure.next_attempt_ms, 2);
}

#[test]
fn failure_delay_math_saturates_at_the_clock_limit() {
    let breaker = PersistentFailoverBreaker::new(PersistentFailoverBreakerConfig {
        base_backoff_ms: u64::MAX,
        max_backoff_ms: u64::MAX,
        failure_limit: u32::MAX,
    });
    let failure = breaker
        .try_acquire("key", u64::MAX - 1)
        .unwrap()
        .record_failure(u64::MAX - 1)
        .unwrap();
    assert_eq!(failure.next_attempt_ms, u64::MAX);
}
