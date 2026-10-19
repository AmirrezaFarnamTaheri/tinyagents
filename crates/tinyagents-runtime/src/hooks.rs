use async_trait::async_trait;
use tinyagents_harness::terminal::TerminalOutcome;
use std::sync::Arc;

use crate::{
    CommitReceipt, ResumePreparation, RuntimeError, SessionStateView, SessionTerminal,
    PostCommitObservation, SessionTurnOutcome, SessionTurnRequest, TranscriptTurnOptions,
    TurnOptions, TurnPreparation,
};

/// Receives captured execution facts after a successful durable commit.
#[async_trait]
pub trait PostCommitSink<C: Clone + Send + Sync + 'static = ()>: Send + Sync {
    /// Observes a committed turn. Errors cannot roll back the durable append.
    async fn after_commit(&self, observation: &PostCommitObservation<C>) -> Result<(), RuntimeError>;
}

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

/// Runtime-specific binding for the neutral hooks callback adapter.
pub struct RuntimeSessionHookTypes<C: Clone + Send + Sync + 'static>(std::marker::PhantomData<C>);

impl<C: Clone + Send + Sync + 'static> tinyagents_hooks::SessionHookTypes
    for RuntimeSessionHookTypes<C>
{
    type Context = C;
    type Request = SessionTurnRequest;
    type Options = TurnOptions<C>;
    type StateView<'a> = SessionStateView<'a>;
    type ResumePreparation = ResumePreparation;
    type TurnPreparation = TurnPreparation;
    type Outcome = SessionTurnOutcome;
    type TranscriptOptions = TranscriptTurnOptions<C>;
    type CommitReceipt = CommitReceipt<C>;
    type Terminal = SessionTerminal;
    type Error = RuntimeError;
}

/// Compatibility alias retaining the old runtime generic parameter.
pub type FnSessionHooks<C = ()> = tinyagents_hooks::FnSessionHooks<RuntimeSessionHookTypes<C>>;

/// Compatibility future type for runtime callbacks.
pub type SessionHookFuture<'a, T> = tinyagents_hooks::SessionHookFuture<'a, T, RuntimeError>;

#[async_trait]
impl<C: Clone + Send + Sync + 'static> SessionHooks<C>
    for tinyagents_hooks::FnSessionHooks<RuntimeSessionHookTypes<C>>
{
    async fn before_resume(
        &self,
        request: &mut SessionTurnRequest,
        options: &mut TurnOptions<C>,
        state: SessionStateView<'_>,
    ) -> Result<ResumePreparation, RuntimeError> {
        tinyagents_hooks::FnSessionHooks::before_resume(self, request, options, state).await
    }
    async fn before_turn(
        &self,
        request: &mut SessionTurnRequest,
        options: &mut TurnOptions<C>,
        state: SessionStateView<'_>,
    ) -> Result<TurnPreparation, RuntimeError> {
        tinyagents_hooks::FnSessionHooks::before_turn(self, request, options, state).await
    }
    async fn before_commit(
        &self,
        outcome: &SessionTurnOutcome,
        options: &TranscriptTurnOptions<C>,
    ) -> Result<(), RuntimeError> {
        tinyagents_hooks::FnSessionHooks::before_commit(self, outcome, options).await
    }
    async fn after_commit(&self, receipt: CommitReceipt<C>) -> Result<(), RuntimeError> {
        tinyagents_hooks::FnSessionHooks::after_commit(self, receipt).await
    }
    async fn on_terminal(&self, terminal: SessionTerminal) -> Result<(), RuntimeError> {
        tinyagents_hooks::FnSessionHooks::on_terminal(self, terminal).await
    }
}

#[cfg(test)]
#[path = "hooks_tests.rs"]
mod tests;
