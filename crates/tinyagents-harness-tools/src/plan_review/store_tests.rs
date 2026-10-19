use super::*;
use std::{sync::Arc, time::Duration};
use crate::plan_review::PlanReviewRequest;
use std::sync::Mutex as StdMutex;

#[derive(Default)] struct Events(StdMutex<Vec<(String, PlanReviewReason)>>);
impl PlanReviewNotifier<u8> for Events {
    fn requested(&self, _: &ParkedReview<u8>) {}
    fn decided(&self, review: &ParkedReview<u8>, _: &PlanReviewResolution, reason: PlanReviewReason) { self.0.lock().unwrap().push((review.request_id.clone(), reason)); }
}
fn store(ttl: Duration) -> (Arc<PlanReviewStore<u8>>, Arc<Events>) {
    let events = Arc::new(Events::default());
    (Arc::new(PlanReviewStore::with_ttl(ttl, events.clone())), events)
}
fn request(store: Arc<PlanReviewStore<u8>>, scope: u8, thread: &str) -> tokio::task::JoinHandle<PlanReviewResolution> {
    let thread = thread.to_owned();
    tokio::spawn(async move { store.request_review(scope, thread, None, PlanReviewRequest { summary: "plan".into(), steps: vec!["one".into()], tool_call_id: None }).await })
}

#[tokio::test(start_paused = true)]
async fn timeout_rejects_and_reports_expired() {
    let (store, events) = store(Duration::from_secs(600));
    let task = request(store.clone(), 1, "t");
    tokio::task::yield_now().await;
    assert!(store.parked_review_for_thread(&1, "t").is_some());
    tokio::time::advance(Duration::from_secs(600)).await;
    assert_eq!(task.await.unwrap(), PlanReviewResolution::Reject);
    assert_eq!(events.0.lock().unwrap()[0].1, PlanReviewReason::Expired);
    assert!(!store.has_parked_review(&1, "t"));
}

#[tokio::test]
async fn approve_resolves_the_parked_turn() {
    let (store, _) = store(Duration::from_secs(60));
    let task = request(store.clone(), 1, "approve");
    tokio::task::yield_now().await;
    assert!(store.decide_by_thread(&1, "approve", PlanReviewResolution::Approve));
    assert_eq!(task.await.unwrap(), PlanReviewResolution::Approve);
}

#[tokio::test]
async fn revise_returns_feedback_to_the_parked_turn() {
    let (store, _) = store(Duration::from_secs(60));
    let task = request(store.clone(), 1, "revise");
    tokio::task::yield_now().await;
    let resolution = PlanReviewResolution::Revise { feedback: "add tests".into() };
    assert!(store.decide_by_thread(&1, "revise", resolution.clone()));
    assert_eq!(task.await.unwrap(), resolution);
}

#[test]
fn unknown_request_cannot_be_decided() {
    let (store, _) = store(Duration::from_secs(60));
    assert!(!store.decide(&1, "missing", PlanReviewResolution::Approve));
    assert!(!store.decide_by_thread(&1, "missing", PlanReviewResolution::Approve));
}

#[tokio::test]
async fn parked_snapshot_replays_routing_call_id_and_expiry() {
    let (store, _) = store(Duration::from_secs(60));
    let parked_store = store.clone();
    let task = tokio::spawn(async move { parked_store.request_review(1, "replay".into(), Some("client".into()), PlanReviewRequest { summary: "plan".into(), steps: vec!["step".into()], tool_call_id: Some("call".into()) }).await });
    tokio::task::yield_now().await;
    let snapshot = store.parked_review_for_thread(&1, "replay").unwrap();
    assert_eq!(snapshot.client_id.as_deref(), Some("client"));
    assert_eq!(snapshot.tool_call_id.as_deref(), Some("call"));
    assert_eq!(snapshot.steps, vec!["step"]);
    assert!(!snapshot.expires_at.is_empty());
    assert!(store.decide(&1, &snapshot.request_id, PlanReviewResolution::Approve));
    assert_eq!(task.await.unwrap(), PlanReviewResolution::Approve);
    assert!(store.parked_review_for_thread(&1, "replay").is_none());
}

#[test]
fn no_snapshot_exists_for_an_unparked_thread() {
    let (store, _) = store(Duration::from_secs(60));
    assert!(store.parked_review_for_thread(&1, "missing").is_none());
}

#[tokio::test]
async fn dropped_park_reports_cancelled_and_cleans_snapshot() {
    let (store, events) = store(Duration::from_secs(600));
    let task = request(store.clone(), 1, "t");
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;
    assert!(!store.has_parked_review(&1, "t"));
    assert_eq!(events.0.lock().unwrap()[0].1, PlanReviewReason::Cancelled);
}

#[tokio::test]
async fn dropped_sender_is_cancelled_not_expired() {
    let (store, events) = store(Duration::from_secs(600));
    let task = request(store.clone(), 1, "t");
    tokio::task::yield_now().await;
    let id = store.parked_review_for_thread(&1, "t").unwrap().request_id;
    assert!(store.cancel(&1, &id));
    assert_eq!(task.await.unwrap(), PlanReviewResolution::Reject);
    assert_eq!(events.0.lock().unwrap()[0].1, PlanReviewReason::Cancelled);
}

#[tokio::test]
async fn decisions_are_scope_isolated_and_newest_review_is_replayed() {
    let (store, _) = store(Duration::from_secs(60));
    let one = request(store.clone(), 1, "same"); tokio::task::yield_now().await;
    let old = store.parked_review_for_thread(&1, "same").unwrap();
    let two = request(store.clone(), 1, "same"); tokio::task::yield_now().await;
    let newest = store.parked_review_for_thread(&1, "same").unwrap();
    assert_ne!(old.request_id, newest.request_id);
    assert!(store.parked_review_for_thread(&2, "same").is_none());
    assert!(!store.decide(&2, &newest.request_id, PlanReviewResolution::Approve));
    assert!(store.decide_by_thread(&1, "same", PlanReviewResolution::Approve));
    assert_eq!(two.await.unwrap(), PlanReviewResolution::Approve);
    assert!(!store.decide(&1, &old.request_id, PlanReviewResolution::Reject));
    assert_eq!(one.await.unwrap(), PlanReviewResolution::Reject);
}
