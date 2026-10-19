//! In-memory, fail-closed state for human plan reviews.
use super::PlanReviewResolution;
use parking_lot::Mutex;
use std::{collections::HashMap, hash::Hash, sync::Arc, time::Duration};
use tokio::sync::oneshot;
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanReviewReason { Approved, Rejected, Revised, Expired, Cancelled }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedReview<S> {
    pub request_id: String,
    pub scope: S,
    pub thread_id: String,
    pub client_id: Option<String>,
    pub summary: String,
    pub steps: Vec<String>,
    pub tool_call_id: Option<String>,
    pub expires_at: String,
}

pub trait PlanReviewNotifier<S>: Send + Sync {
    fn requested(&self, review: &ParkedReview<S>);
    fn decided(&self, review: &ParkedReview<S>, resolution: &PlanReviewResolution, reason: PlanReviewReason);
}

struct Entry<S> { snapshot: ParkedReview<S>, sender: oneshot::Sender<PlanReviewResolution> }
struct State<S> {
    entries: HashMap<String, Entry<S>>,
    newest: HashMap<(S, String), String>,
}

/// A scoped collection of parked reviews. Scope keys are supplied by the host;
/// this crate never assigns meaning to tenant or user identity.
pub struct PlanReviewStore<S> {
    ttl: Duration,
    state: Mutex<State<S>>,
    notifier: Arc<dyn PlanReviewNotifier<S>>,
}

impl<S> PlanReviewStore<S>
where S: Clone + Eq + Hash + Send + Sync + 'static {
    pub fn new(notifier: Arc<dyn PlanReviewNotifier<S>>) -> Self { Self::with_ttl(DEFAULT_TTL, notifier) }
    pub fn with_ttl(ttl: Duration, notifier: Arc<dyn PlanReviewNotifier<S>>) -> Self {
        Self { ttl, state: Mutex::new(State { entries: HashMap::new(), newest: HashMap::new() }), notifier }
    }

    pub async fn request_review(&self, scope: S, thread_id: String, client_id: Option<String>, request: super::PlanReviewRequest) -> PlanReviewResolution {
        let id = format!("plan-{}", Uuid::new_v4());
        let expires_at = (chrono::Utc::now() + chrono::Duration::from_std(self.ttl).unwrap_or_default()).to_rfc3339();
        let snapshot = ParkedReview { request_id: id.clone(), scope: scope.clone(), thread_id: thread_id.clone(), client_id, summary: request.summary, steps: request.steps, tool_call_id: request.tool_call_id, expires_at };
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock();
            state.newest.insert((scope.clone(), thread_id.clone()), id.clone());
            state.entries.insert(id.clone(), Entry { snapshot: snapshot.clone(), sender: tx });
        }
        let mut guard = ParkGuard { store: self, snapshot: snapshot.clone(), armed: true };
        self.notifier.requested(&snapshot);
        let (resolution, reason) = match tokio::time::timeout(self.ttl, rx).await {
            Ok(Ok(value)) => { let reason = reason_for(&value); (value, reason) }
            Ok(Err(_)) => (PlanReviewResolution::Reject, PlanReviewReason::Cancelled),
            Err(_) => (PlanReviewResolution::Reject, PlanReviewReason::Expired),
        };
        guard.armed = false;
        self.remove(&snapshot);
        self.notifier.decided(&snapshot, &resolution, reason);
        resolution
    }

    pub fn decide(&self, scope: &S, request_id: &str, resolution: PlanReviewResolution) -> bool {
        let mut state = self.state.lock();
        if !state.entries.get(request_id).is_some_and(|entry| entry.snapshot.scope.eq(scope)) { return false; }
        let entry = state.entries.remove(request_id).expect("entry checked above");
        let key = (entry.snapshot.scope.clone(), entry.snapshot.thread_id.clone());
        if state.newest.get(&key).is_some_and(|id| id == request_id) { state.newest.remove(&key); }
        drop(state);
        entry.sender.send(resolution).is_ok()
    }
    /// Drops a pending sender, waking its waiter into the fail-closed cancelled
    /// path. Hosts use this when the review surface is withdrawn.
    pub fn cancel(&self, scope: &S, request_id: &str) -> bool {
        let mut state = self.state.lock();
        if !state.entries.get(request_id).is_some_and(|entry| entry.snapshot.scope.eq(scope)) { return false; }
        let entry = state.entries.remove(request_id).expect("entry checked above");
        let key = (entry.snapshot.scope.clone(), entry.snapshot.thread_id.clone());
        if state.newest.get(&key).is_some_and(|id| id == request_id) { state.newest.remove(&key); }
        drop(entry);
        true
    }
    pub fn decide_by_thread(&self, scope: &S, thread_id: &str, resolution: PlanReviewResolution) -> bool {
        let id: Option<String> = self.state.lock().newest.get(&(scope.clone(), thread_id.to_owned())).map(String::clone);
        id.is_some_and(|id| self.decide(scope, &id, resolution))
    }
    pub fn parked_review_for_thread(&self, scope: &S, thread_id: &str) -> Option<ParkedReview<S>> {
        let state = self.state.lock();
        let id = state.newest.get(&(scope.clone(), thread_id.to_owned()))?;
        state.entries.get(id).map(|entry| entry.snapshot.clone())
    }
    pub fn has_parked_review(&self, scope: &S, thread_id: &str) -> bool { self.parked_review_for_thread(scope, thread_id).is_some() }
    fn remove(&self, snapshot: &ParkedReview<S>) {
        let mut state = self.state.lock();
        state.entries.remove(&snapshot.request_id);
        let key = (snapshot.scope.clone(), snapshot.thread_id.clone());
        if state.newest.get(&key).is_some_and(|id| id == &snapshot.request_id) { state.newest.remove(&key); }
    }
}

fn reason_for(resolution: &PlanReviewResolution) -> PlanReviewReason {
    match resolution { PlanReviewResolution::Approve | PlanReviewResolution::NonInteractiveApprove => PlanReviewReason::Approved, PlanReviewResolution::Reject => PlanReviewReason::Rejected, PlanReviewResolution::Revise { .. } => PlanReviewReason::Revised, PlanReviewResolution::NotApplicable => PlanReviewReason::Cancelled }
}
struct ParkGuard<'a, S> where S: Clone + Eq + Hash + Send + Sync + 'static { store: &'a PlanReviewStore<S>, snapshot: ParkedReview<S>, armed: bool }
impl<S> Drop for ParkGuard<'_, S> where S: Clone + Eq + Hash + Send + Sync + 'static {
    fn drop(&mut self) { if self.armed { self.store.remove(&self.snapshot); self.store.notifier.decided(&self.snapshot, &PlanReviewResolution::Reject, PlanReviewReason::Cancelled); } }
}
