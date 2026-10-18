//! Runtime-neutral callback adapter for session lifecycle hooks.

use std::{future::Future, pin::Pin, sync::Arc};

/// Session-specific types supplied by the runtime that consumes the adapter.
pub trait SessionHookTypes: Send + Sync + 'static {
    /// Context carried by turn options.
    type Context: Clone + Send + Sync + 'static;
    /// Mutable turn request.
    type Request: Send;
    /// Turn options.
    type Options: Send;
    /// Borrowed state view supplied at a lifecycle boundary.
    type StateView<'a>: Send
    where
        Self: 'a;
    /// Preparation returned before resume.
    type ResumePreparation: Send;
    /// Preparation returned before driving a turn.
    type TurnPreparation: Send;
    /// Candidate turn outcome before commit.
    type Outcome: Sync;
    /// Options used to construct the committed transcript turn.
    type TranscriptOptions: Sync;
    /// Receipt after durable commit.
    type CommitReceipt: Send;
    /// Terminal turn state.
    type Terminal: Send;
    /// Runtime-specific error type.
    type Error: Send;
}

/// Boxed callback future used by [`FnSessionHooks`].
pub type SessionHookFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

type BeforeResume<T> = dyn for<'a> Fn(
        &'a mut <T as SessionHookTypes>::Request,
        &'a mut <T as SessionHookTypes>::Options,
        <T as SessionHookTypes>::StateView<'a>,
    ) -> SessionHookFuture<
        'a,
        <T as SessionHookTypes>::ResumePreparation,
        <T as SessionHookTypes>::Error,
    > + Send
    + Sync;
type BeforeTurn<T> = dyn for<'a> Fn(
        &'a mut <T as SessionHookTypes>::Request,
        &'a mut <T as SessionHookTypes>::Options,
        <T as SessionHookTypes>::StateView<'a>,
    ) -> SessionHookFuture<
        'a,
        <T as SessionHookTypes>::TurnPreparation,
        <T as SessionHookTypes>::Error,
    > + Send
    + Sync;
type BeforeCommit<T> = dyn for<'a> Fn(
        &'a <T as SessionHookTypes>::Outcome,
        &'a <T as SessionHookTypes>::TranscriptOptions,
    ) -> SessionHookFuture<'a, (), <T as SessionHookTypes>::Error>
    + Send
    + Sync;
type AfterCommit<T> = dyn Fn(
        <T as SessionHookTypes>::CommitReceipt,
    ) -> SessionHookFuture<'static, (), <T as SessionHookTypes>::Error>
    + Send
    + Sync;
type OnTerminal<T> = dyn Fn(
        <T as SessionHookTypes>::Terminal,
    ) -> SessionHookFuture<'static, (), <T as SessionHookTypes>::Error>
    + Send
    + Sync;

/// Closure-backed callbacks for a runtime's session lifecycle.
pub struct FnSessionHooks<T: SessionHookTypes> {
    before_resume: Arc<BeforeResume<T>>,
    before_turn: Arc<BeforeTurn<T>>,
    before_commit: Arc<BeforeCommit<T>>,
    after_commit: Arc<AfterCommit<T>>,
    on_terminal: Arc<OnTerminal<T>>,
}

impl<T: SessionHookTypes> FnSessionHooks<T> {
    /// Create a callback-backed session hook set.
    #[allow(clippy::type_complexity)]
    pub fn new(
        before_resume: impl for<'a> Fn(
            &'a mut T::Request,
            &'a mut T::Options,
            T::StateView<'a>,
        ) -> SessionHookFuture<'a, T::ResumePreparation, T::Error>
        + Send
        + Sync
        + 'static,
        before_turn: impl for<'a> Fn(
            &'a mut T::Request,
            &'a mut T::Options,
            T::StateView<'a>,
        ) -> SessionHookFuture<'a, T::TurnPreparation, T::Error>
        + Send
        + Sync
        + 'static,
        before_commit: impl for<'a> Fn(
            &'a T::Outcome,
            &'a T::TranscriptOptions,
        ) -> SessionHookFuture<'a, (), T::Error>
        + Send
        + Sync
        + 'static,
        after_commit: impl Fn(T::CommitReceipt) -> SessionHookFuture<'static, (), T::Error>
        + Send
        + Sync
        + 'static,
        on_terminal: impl Fn(T::Terminal) -> SessionHookFuture<'static, (), T::Error>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            before_resume: Arc::new(before_resume),
            before_turn: Arc::new(before_turn),
            before_commit: Arc::new(before_commit),
            after_commit: Arc::new(after_commit),
            on_terminal: Arc::new(on_terminal),
        }
    }

    /// Run the callback before transcript resume.
    pub async fn before_resume<'a>(
        &'a self,
        request: &'a mut T::Request,
        options: &'a mut T::Options,
        state: T::StateView<'a>,
    ) -> Result<T::ResumePreparation, T::Error> {
        (self.before_resume)(request, options, state).await
    }

    /// Run the callback before the driver sees the turn.
    pub async fn before_turn<'a>(
        &'a self,
        request: &'a mut T::Request,
        options: &'a mut T::Options,
        state: T::StateView<'a>,
    ) -> Result<T::TurnPreparation, T::Error> {
        (self.before_turn)(request, options, state).await
    }

    /// Run the callback before a candidate is committed.
    pub async fn before_commit(
        &self,
        outcome: &T::Outcome,
        options: &T::TranscriptOptions,
    ) -> Result<(), T::Error> {
        (self.before_commit)(outcome, options).await
    }

    /// Run the callback after a durable commit.
    pub async fn after_commit(&self, receipt: T::CommitReceipt) -> Result<(), T::Error> {
        (self.after_commit)(receipt).await
    }

    /// Run the callback for every terminal turn result.
    pub async fn on_terminal(&self, terminal: T::Terminal) -> Result<(), T::Error> {
        (self.on_terminal)(terminal).await
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
