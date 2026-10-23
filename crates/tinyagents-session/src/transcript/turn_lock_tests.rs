use super::*;
use crate::transcript::{TranscriptHistory, TranscriptMeta, TranscriptRead};

/// Whether `lock_session_turn` for `session` would have to wait right now.
///
/// No clock: an uncontended tokio mutex is acquired on its first poll, so a
/// lock still pending after that poll is held by someone else.
async fn is_held(locator: &dyn TranscriptLocator, session: &SessionRef) -> bool {
    tokio::select! {
        biased;
        _guard = lock_session_turn(locator, session) => false,
        () = tokio::task::yield_now() => true,
    }
}

/// A locator naming a fixed destination, so separate instances stand for
/// separate handles over one store.
/// Each test needs its own destination because the lock registry is process-wide.
struct Keyed(&'static str);

impl TranscriptLocator for Keyed {
    fn destination_key(&self) -> Option<String> {
        Some(self.0.to_string())
    }
    fn latest_for_agent(&self, _: &str) -> Option<Arc<dyn TranscriptRead>> {
        None
    }
    fn root_for_thread(&self, _: &str) -> Option<Arc<dyn TranscriptRead>> {
        None
    }
    fn open_stem(&self, _: &str, _: TranscriptMeta) -> anyhow::Result<Arc<dyn TranscriptHistory>> {
        anyhow::bail!("not a store")
    }
}

#[tokio::test]
async fn a_held_turn_lock_blocks_the_same_session_until_released() {
    let locator = Keyed("held-lock-store");
    let session = SessionRef::scoped("held-lock-thread", "agent");

    let guard = lock_session_turn(&locator, &session).await.unwrap();
    assert!(is_held(&locator, &session).await);

    drop(guard);
    assert!(!is_held(&locator, &session).await);
}

#[tokio::test]
async fn every_generation_and_every_locator_over_one_destination_share_the_lock() {
    let session = SessionRef::scoped("generation-lock-thread", "agent");
    let _guard = lock_session_turn(&Keyed("generation-lock-store"), &session)
        .await
        .unwrap();

    // A compaction moves the head on; it is still the same conversation. A
    // host also rebuilds its locator per turn rather than sharing one.
    let rebuilt = Keyed("generation-lock-store");
    assert!(is_held(&rebuilt, &session.next_generation().next_generation()).await);
}

#[tokio::test]
async fn other_sessions_and_other_destinations_are_independent() {
    let locator = Keyed("independent-session-store-a");
    let session = SessionRef::scoped("independent-session-thread", "agent");
    let _guard = lock_session_turn(&locator, &session).await.unwrap();

    assert!(
        !is_held(
            &locator,
            &SessionRef::scoped("independent-other-thread", "agent")
        )
        .await
    );
    assert!(
        !is_held(
            &locator,
            &SessionRef::scoped("independent-session-thread", "other-agent")
        )
        .await
    );
    assert!(!is_held(&Keyed("independent-session-store-b"), &session).await);
}

#[tokio::test]
async fn distinct_in_memory_locators_do_not_share_a_lock() {
    use crate::port::InMemoryTranscriptLocator;
    let first = InMemoryTranscriptLocator::new("first");
    let second = InMemoryTranscriptLocator::new("second");
    let session = SessionRef::scoped("in-memory-lock-thread", "agent");
    let _guard = lock_session_turn(&first, &session).await.unwrap();

    assert!(is_held(&first, &session).await);
    assert!(!is_held(&second, &session).await);
}

/// A locator that cannot name its destination cannot be told apart from any
/// other, so there is nothing to key a lock on.
struct Anonymous;

impl TranscriptLocator for Anonymous {
    fn latest_for_agent(&self, _: &str) -> Option<Arc<dyn TranscriptRead>> {
        None
    }
    fn root_for_thread(&self, _: &str) -> Option<Arc<dyn TranscriptRead>> {
        None
    }
    fn open_stem(&self, _: &str, _: TranscriptMeta) -> anyhow::Result<Arc<dyn TranscriptHistory>> {
        anyhow::bail!("anonymous")
    }
}

#[tokio::test]
async fn an_anonymous_locator_has_no_turn_lock() {
    assert!(
        lock_session_turn(&Anonymous, &SessionRef::scoped("thread-1", "agent"))
            .await
            .is_none()
    );
}
