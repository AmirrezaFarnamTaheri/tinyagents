use async_trait::async_trait;
use tinyagents_harness::terminal::TerminalOutcome;

use crate::{
    CommitReceipt, ResumePreparation, RuntimeError, SessionStateView, SessionTerminal,
    SessionTurnOutcome, SessionTurnRequest, TranscriptTurnOptions, TurnOptions, TurnPreparation,
};

/// Host observation/preparation around a session turn.
///
/// Hooks do not grant tools, select models, compose product prompts, or own
/// transcript state. A session serializes these mutable calls, so a host can
/// keep preparation state without task-local runtime state.
#[async_trait]
pub trait SessionHooks<C: Clone + Send + Sync + 'static = ()>: Send + Sync {
    /// Runs after the turn's initial cancellation check and before transcript
    /// target binding or resume. It may mutate the request and live options.
    /// Its target remains lazy until resume or the first append needs it.
    async fn before_resume(
        &self,
        _: &mut SessionTurnRequest,
        _: &mut TurnOptions<C>,
        _: SessionStateView<'_>,
    ) -> Result<ResumePreparation, RuntimeError> {
        Ok(ResumePreparation::default())
    }
    /// Runs before the driver sees the request and consumes the explicit
    /// options, after any requested transcript has been loaded. Returned
    /// values apply only to this driver invocation.
    async fn before_turn(
        &self,
        request: &mut SessionTurnRequest,
        options: &mut TurnOptions<C>,
        state: SessionStateView<'_>,
    ) -> Result<TurnPreparation, RuntimeError>;
    /// Runs after the driver has produced a candidate and before it commits.
    /// Returning an error or observing cancellation therefore leaves no
    /// durable session mutation behind.
    async fn before_commit(
        &self,
        outcome: &SessionTurnOutcome,
        options: &TranscriptTurnOptions<C>,
    ) -> Result<(), RuntimeError>;
    /// Runs exactly once after a successful durable transcript commit.
    /// Errors and cancellation observed here are deliberately observational:
    /// the result has already become durable and remains successful.
    async fn after_commit(&self, _: CommitReceipt<C>) -> Result<(), RuntimeError> {
        Ok(())
    }
    /// Receives the typed [`TerminalOutcome`] of the turn, exactly once,
    /// immediately before [`Self::on_terminal`]. A driver failure carries the
    /// driver's own classification (timeout, provider failure, ...);
    /// everything else is derived with [`SessionTerminal::outcome`]. The
    /// default ignores it, so existing hooks are unaffected.
    async fn on_terminal_outcome(&self, _: TerminalOutcome) -> Result<(), RuntimeError> {
        Ok(())
    }
    /// Runs exactly once for every terminal turn result.
    async fn on_terminal(&self, terminal: SessionTerminal) -> Result<(), RuntimeError>;
}

/// A no-op hook set for hosts that need no lifecycle observation.
#[derive(Default)]
pub struct NoopSessionHooks;

#[async_trait]
impl<C: Clone + Send + Sync + 'static> SessionHooks<C> for NoopSessionHooks {
    async fn before_resume(
        &self,
        _: &mut SessionTurnRequest,
        _: &mut TurnOptions<C>,
        _: SessionStateView<'_>,
    ) -> Result<ResumePreparation, RuntimeError> {
        Ok(ResumePreparation::default())
    }
    async fn before_turn(
        &self,
        _: &mut SessionTurnRequest,
        _: &mut TurnOptions<C>,
        _: SessionStateView<'_>,
    ) -> Result<TurnPreparation, RuntimeError> {
        Ok(TurnPreparation::default())
    }
    async fn before_commit(
        &self,
        _: &SessionTurnOutcome,
        _: &TranscriptTurnOptions<C>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn after_commit(&self, _: CommitReceipt<C>) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn on_terminal(&self, _: SessionTerminal) -> Result<(), RuntimeError> {
        Ok(())
    }
}

/// Boxed future used by [`FnSessionHooks`] callbacks.
pub type SessionHookFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, RuntimeError>> + Send + 'a>>;

/// Closure-backed implementation of [`SessionHooks`].
///
/// This adapter belongs to the runtime crate because its callbacks use the
/// runtime's session request and commit types; placing it in `tinyagents-hooks`
/// would create a dependency cycle (`runtime -> hooks -> runtime`).
pub struct FnSessionHooks<C: Clone + Send + Sync + 'static = ()> {
    before_resume: std::sync::Arc<dyn for<'a> Fn(&'a mut SessionTurnRequest, &'a mut TurnOptions<C>, SessionStateView<'a>) -> SessionHookFuture<'a, ResumePreparation> + Send + Sync>,
    before_turn: std::sync::Arc<dyn for<'a> Fn(&'a mut SessionTurnRequest, &'a mut TurnOptions<C>, SessionStateView<'a>) -> SessionHookFuture<'a, TurnPreparation> + Send + Sync>,
    before_commit: std::sync::Arc<dyn for<'a> Fn(&'a SessionTurnOutcome, &'a TranscriptTurnOptions<C>) -> SessionHookFuture<'a, ()> + Send + Sync>,
    after_commit: std::sync::Arc<dyn Fn(CommitReceipt<C>) -> SessionHookFuture<'static, ()> + Send + Sync>,
    on_terminal: std::sync::Arc<dyn Fn(SessionTerminal) -> SessionHookFuture<'static, ()> + Send + Sync>,
}

impl<C: Clone + Send + Sync + 'static> FnSessionHooks<C> {
    /// Create a callback-backed session hook set.
    #[allow(clippy::type_complexity)]
    pub fn new(
        before_resume: impl for<'a> Fn(&'a mut SessionTurnRequest, &'a mut TurnOptions<C>, SessionStateView<'a>) -> SessionHookFuture<'a, ResumePreparation> + Send + Sync + 'static,
        before_turn: impl for<'a> Fn(&'a mut SessionTurnRequest, &'a mut TurnOptions<C>, SessionStateView<'a>) -> SessionHookFuture<'a, TurnPreparation> + Send + Sync + 'static,
        before_commit: impl for<'a> Fn(&'a SessionTurnOutcome, &'a TranscriptTurnOptions<C>) -> SessionHookFuture<'a, ()> + Send + Sync + 'static,
        after_commit: impl Fn(CommitReceipt<C>) -> SessionHookFuture<'static, ()> + Send + Sync + 'static,
        on_terminal: impl Fn(SessionTerminal) -> SessionHookFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        Self { before_resume: std::sync::Arc::new(before_resume), before_turn: std::sync::Arc::new(before_turn), before_commit: std::sync::Arc::new(before_commit), after_commit: std::sync::Arc::new(after_commit), on_terminal: std::sync::Arc::new(on_terminal) }
    }
}

#[async_trait]
impl<C: Clone + Send + Sync + 'static> SessionHooks<C> for FnSessionHooks<C> {
    async fn before_resume(&self, request: &mut SessionTurnRequest, options: &mut TurnOptions<C>, state: SessionStateView<'_>) -> Result<ResumePreparation, RuntimeError> { (self.before_resume)(request, options, state).await }
    async fn before_turn(&self, request: &mut SessionTurnRequest, options: &mut TurnOptions<C>, state: SessionStateView<'_>) -> Result<TurnPreparation, RuntimeError> { (self.before_turn)(request, options, state).await }
    async fn before_commit(&self, outcome: &SessionTurnOutcome, options: &TranscriptTurnOptions<C>) -> Result<(), RuntimeError> { (self.before_commit)(outcome, options).await }
    async fn after_commit(&self, receipt: CommitReceipt<C>) -> Result<(), RuntimeError> { (self.after_commit)(receipt).await }
    async fn on_terminal(&self, terminal: SessionTerminal) -> Result<(), RuntimeError> { (self.on_terminal)(terminal).await }
}
