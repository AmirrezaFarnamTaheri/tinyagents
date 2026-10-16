use super::*;
use std::sync::{Arc, Barrier};

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
    assert!(
        second_context
            .try_acquire("tenant-b/provider:model".into(), 0)
            .is_ok()
    );

    let failure = first.record_failure(0).expect("failure is recorded");
    assert_eq!(failure.failure_count, 1);
    assert_eq!(failure.next_attempt_ms, 30_000);
    assert!(matches!(
        second_context.try_acquire("tenant-a/provider:model".into(), 29_999),
        Err(BreakerRejection::Backoff { .. })
    ));
    assert!(
        second_context
            .try_acquire("tenant-a/provider:model".into(), 30_000)
            .is_ok()
    );
}

#[test]
fn dropped_attempt_releases_in_flight_without_recording_a_failure() {
    let breaker = PersistentFailoverBreaker::<&str>::default();
    let attempt = breaker.try_acquire("key", 0).unwrap();
    drop(attempt);
    assert!(breaker.try_acquire("key", 0).is_ok());
}

#[tokio::test]
async fn cancelling_a_future_releases_its_attempt_guard() {
    let breaker = PersistentFailoverBreaker::<String>::default();
    let task_breaker = breaker.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let attempt = task_breaker.try_acquire("key".into(), 0).unwrap();
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(attempt);
    });
    started_rx.await.unwrap();
    assert!(matches!(
        breaker.try_acquire("key".into(), 0),
        Err(BreakerRejection::InFlight)
    ));
    task.abort();
    let _ = task.await;
    assert!(breaker.try_acquire("key".into(), 0).is_ok());
}

#[test]
fn concurrent_same_key_admission_has_only_one_winner() {
    let breaker = PersistentFailoverBreaker::<String>::default();
    let start = Arc::new(Barrier::new(3));
    let attempted = Arc::new(Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let breaker = breaker.clone();
            let start = Arc::clone(&start);
            let attempted = Arc::clone(&attempted);
            std::thread::spawn(move || {
                start.wait();
                let result = breaker.try_acquire("shared".into(), 0);
                attempted.wait();
                result
            })
        })
        .collect();
    start.wait();
    attempted.wait();
    let admitted = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().is_ok())
        .filter(|admitted| *admitted)
        .count();
    assert_eq!(admitted, 1);
}

#[test]
fn failure_backoff_saturates_at_the_cap_and_failure_limit() {
    let breaker = PersistentFailoverBreaker::new(PersistentFailoverBreakerConfig {
        base_backoff_ms: 10,
        max_backoff_ms: 25,
        failure_limit: 3,
    });
    let key = "provider/model";
    let first = breaker
        .try_acquire(key, 0)
        .unwrap()
        .record_failure(0)
        .unwrap();
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
        Err(BreakerRejection::Backoff {
            failure_limit_reached: true,
            ..
        })
    ));
}

#[test]
fn reset_invalidates_a_stale_guard_and_success_resets_the_streak() {
    let breaker = PersistentFailoverBreaker::<String>::new(PersistentFailoverBreakerConfig {
        base_backoff_ms: 1,
        max_backoff_ms: 10,
        failure_limit: 2,
    });
    let mut old = breaker.try_acquire("key".into(), 0).unwrap();
    breaker.reset(&"key".to_owned());
    let mut current = breaker.try_acquire("key".into(), 0).unwrap();
    assert!(old.record_failure(0).is_none());
    drop(old);
    assert!(matches!(
        breaker.try_acquire("key".into(), 1),
        Err(BreakerRejection::InFlight)
    ));
    current.record_success();
    let mut after_success = breaker.try_acquire("key".into(), 0).unwrap();
    let failure = after_success.record_failure(0).unwrap();
    assert_eq!(failure.failure_count, 1);
    assert_eq!(failure.next_attempt_ms, 1);
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
