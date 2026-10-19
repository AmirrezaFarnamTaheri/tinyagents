use std::{collections::HashMap, future::Future, sync::Arc};

use tinyagents_tasks::{CompletionRouter, DeliveryFence};
use tokio::sync::Mutex as AsyncMutex;

use super::completion::{CompletionOrigin, deliver};
use super::policy::{AttemptSource, apply_outcome_policies, may_retry};
use super::{
    IncompleteKind, SpawnAdmission, SpawnRejection, SubagentIncomplete, restrict_tools_with_scope,
};
use super::{
    PersistedSubagentPause, SubagentError, SubagentExecution, SubagentExecutor, SubagentOutcome,
    SubagentOutcomeKind, SubagentPausePersistenceDisposition, SubagentPersistence,
    SubagentPersistenceDisposition, SubagentPlanner, SubagentRequest, SubagentRunResult,
    SubagentTaskKey, SubagentTerminalPersistenceDisposition,
};
use tinyagents_harness::error::TinyAgentsError;

const LOG_PREFIX: &str = "[subagent-driver]";

fn ensure_turn_fence(
    fence: Option<&Arc<dyn DeliveryFence>>,
    cancellation: Option<&CancellationToken>,
) -> Result<(), SubagentError> {
    if fence.is_some_and(|fence| !fence.admitted()) {
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        Err(SubagentError::Cancelled)
    } else {
        Ok(())
    }
}

async fn await_with_turn_fence<T>(
    fence: Option<&Arc<dyn DeliveryFence>>,
    cancellation: Option<&CancellationToken>,
    future: impl Future<Output = Result<T, SubagentError>>,
) -> Result<T, SubagentError> {
    ensure_turn_fence(fence, cancellation)?;
    let Some(fence) = fence else {
        return future.await;
    };
    tokio::select! {
        biased;
        _ = fence.lost() => {
            if let Some(cancellation) = cancellation {
                cancellation.cancel();
            }
            Err(SubagentError::Cancelled)
        }
        result = future => {
            ensure_turn_fence(Some(fence), cancellation)?;
            result
        }
    }
}

/// Cloneable process-scope in-flight registry that can be shared by drivers.
pub type SubagentInFlight = SharedFlightMap<SubagentTaskKey, SubagentRunResult, SubagentError>;
use tinyagents_harness::{
    CancellationToken,
    cache::{SharedFlightEntry, SharedFlightMap},
    context::RunConfig,
    ids::RunId,
};

/// Optional host seams accepted by [`SubagentDriver::new`].
///
/// Hosts that cannot provide every seam must receive a typed construction error
/// rather than accidentally executing a partial lifecycle.
pub struct SubagentCapabilities<C: Send + 'static = (), H: Send + 'static = ()> {
    /// Host planner, which resolves policy and explicit execution inputs.
    pub planner: Option<Arc<dyn SubagentPlanner<C, H>>>,
    /// Host executor, which drives the prepared run.
    pub executor: Option<Arc<dyn SubagentExecutor<C>>>,
    /// Host persistence for resume and one lifecycle record.
    pub persistence: Option<Arc<dyn SubagentPersistence>>,
}

/// Generic lifecycle driver for one host's subagent runs.
///
/// Concurrent calls for one scoped task key coalesce, while task ids from
/// distinct parents, roots, or threads remain independent. Hosts still need
/// idempotent persistence for multiple processes/drivers.
pub struct SubagentDriver<C: Send + 'static = (), H: Send + 'static = ()> {
    planner: Arc<dyn SubagentPlanner<C, H>>,
    executor: Arc<dyn SubagentExecutor<C>>,
    persistence: Arc<dyn SubagentPersistence>,
    terminal_outcomes: AsyncMutex<HashMap<SubagentTaskKey, SubagentOutcome>>,
    in_flight: SharedFlightMap<SubagentTaskKey, SubagentRunResult, SubagentError>,
    admission: SpawnAdmission,
    completions: Option<Arc<CompletionRouter>>,
}

/// Owns the leader's in-flight reservation until its result is published.
///
/// Dropping a caller future must also release its reservation: otherwise a
/// later caller becomes a follower of work that will never publish a result.
struct LeaderReservation {
    in_flight: SharedFlightMap<SubagentTaskKey, SubagentRunResult, SubagentError>,
    task_key: SubagentTaskKey,
    entry: SharedFlightEntry<SubagentRunResult, SubagentError>,
    finished: bool,
}

impl LeaderReservation {
    fn finish(mut self, result: Result<SubagentRunResult, SubagentError>) {
        self.entry.complete(result);
        self.in_flight.retire(&self.task_key, &self.entry);
        self.finished = true;
    }
}

impl Drop for LeaderReservation {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Publish a typed result before removal so existing followers cannot
        // wait indefinitely, even if the future is dropped outside a runtime.
        self.entry.complete(Err(SubagentError::Cancelled));
        self.in_flight.retire(&self.task_key, &self.entry);
    }
}

impl<C: Send + 'static, H: Send + 'static> SubagentDriver<C, H> {
    /// Validates host capability availability before exposing a runnable driver.
    pub fn new(capabilities: SubagentCapabilities<C, H>) -> Result<Self, SubagentError> {
        Ok(Self {
            planner: capabilities
                .planner
                .ok_or(SubagentError::MissingCapability("planner"))?,
            executor: capabilities
                .executor
                .ok_or(SubagentError::MissingCapability("executor"))?,
            persistence: capabilities
                .persistence
                .ok_or(SubagentError::MissingCapability("persistence"))?,
            terminal_outcomes: AsyncMutex::new(HashMap::new()),
            in_flight: SharedFlightMap::new(),
            admission: SpawnAdmission::default(),
            completions: None,
        })
    }

    /// Records every child this driver finishes with `router`, so its parent is
    /// told according to the child's
    /// [`NotifyMode`](tinyagents_tasks::NotifyMode).
    ///
    /// Only the invocation that wins the durable terminal write records, so a
    /// coalesced follower or a replayed terminal never produces a second
    /// completion. Only a spawn that set a notify mode is recorded:
    ///
    /// - a normal finish is recorded as success or incomplete;
    /// - a cancellation is recorded as `Cancelled` (also when it lands after
    ///   the planner but before the child launches; one before planning has no
    ///   notify mode yet and is not recorded);
    /// - an executor error (`Execution`, or `Transient` once retries are
    ///   exhausted) is recorded as `Failed` and still returned to the caller.
    ///   Host seam faults (task id mismatch, persistence, missing capability)
    ///   are not the child failing and are not recorded. The router keeps the
    ///   first record per task id, so a task re-run under the same id after a
    ///   recorded failure does not add a second completion;
    /// - a pause (awaiting input) is not recorded: it is not terminal, and the
    ///   resume that eventually finishes the same task id records then. For
    ///   the same reason an error while resuming a paused task is not
    ///   recorded: its pause is still durable.
    ///
    /// A failure to record is logged and never fails the run. Without this
    /// call the driver behaves exactly as before.
    pub fn with_completion_router(mut self, router: Arc<CompletionRouter>) -> Self {
        self.completions = Some(router);
        self
    }

    /// Shares in-flight deduplication with other drivers using this registry.
    ///
    /// Hosts should keep one registry for the process and inject clones into
    /// independently composed drivers. Durable persistence remains necessary
    /// to deduplicate across processes.
    pub fn with_shared_in_flight(mut self, in_flight: SubagentInFlight) -> Self {
        self.in_flight = in_flight;
        self
    }

    /// Enforces spawn limits on every lifecycle this driver launches.
    ///
    /// A slot is reserved after cancellation and resume loading but before the
    /// planner runs, and released when the lifecycle returns; a lifecycle that
    /// fails before the executor launches also refunds its total-budget unit.
    /// Coalesced followers and cached terminal results never reserve a slot.
    /// Without this call spawning is unlimited.
    pub fn with_spawn_admission(mut self, admission: SpawnAdmission) -> Self {
        self.admission = admission;
        self
    }

    /// The admission ledger this driver reserves from.
    pub fn spawn_admission(&self) -> &SpawnAdmission {
        &self.admission
    }

    /// Runs `load -> prepare -> execute -> one persistence action`.
    ///
    /// A caller-supplied resume bypasses loading. Cancellation is checked before
    /// preparation, after every awaited lifecycle stage, and after execution;
    /// a cancellation observed after an executor result preserves its output,
    /// history, usage, and artifact references while changing only the status.
    pub async fn run(
        &self,
        request: SubagentRequest<C, H>,
        cancellation: CancellationToken,
    ) -> Result<SubagentRunResult, SubagentError> {
        let task_key = request.task_key().clone();
        if let Some(outcome) = self.terminal_outcomes.lock().await.get(&task_key).cloned() {
            return Ok(SubagentRunResult::new(
                outcome,
                SubagentPersistenceDisposition::TerminalExisting,
            ));
        }
        if let Some(outcome) = self.persistence.load_terminal(&task_key).await? {
            self.cache_terminal(task_key, &outcome).await;
            return Ok(SubagentRunResult::new(
                outcome,
                SubagentPersistenceDisposition::TerminalExisting,
            ));
        }

        let (entry, is_leader) = self.in_flight.claim(task_key.clone());
        if !is_leader {
            return match entry.wait_or_cancel(&cancellation).await {
                None => Ok(SubagentRunResult::new(
                    SubagentOutcome::cancelled(task_key.task_id.clone()),
                    SubagentPersistenceDisposition::ObserverCancelled,
                )),
                Some(Err(error)) => Err(error),
                Some(Ok(result)) => Ok({
                    SubagentRunResult::new(
                        result.outcome,
                        match result.disposition {
                            SubagentPersistenceDisposition::PauseCommitted
                            | SubagentPersistenceDisposition::PauseReplaced
                            | SubagentPersistenceDisposition::PauseExisting => {
                                SubagentPersistenceDisposition::PauseExisting
                            }
                            SubagentPersistenceDisposition::TerminalInserted
                            | SubagentPersistenceDisposition::TerminalExisting => {
                                SubagentPersistenceDisposition::TerminalExisting
                            }
                            SubagentPersistenceDisposition::ObserverCancelled => {
                                SubagentPersistenceDisposition::ObserverCancelled
                            }
                        },
                    )
                }),
            };
        }
        let reservation = LeaderReservation {
            in_flight: self.in_flight.clone(),
            task_key: task_key.clone(),
            entry: entry.clone(),
            finished: false,
        };
        // A preceding caller may have committed a terminal result between the
        // initial cache check and this reservation. Do not reopen that task
        // after its in-flight entry has been removed.
        if let Some(outcome) = self.terminal_outcomes.lock().await.get(&task_key).cloned() {
            reservation.finish(Ok(SubagentRunResult::new(
                outcome.clone(),
                SubagentPersistenceDisposition::TerminalExisting,
            )));
            return Ok(SubagentRunResult::new(
                outcome,
                SubagentPersistenceDisposition::TerminalExisting,
            ));
        }

        let result = self
            .run_reserved(request, task_key.clone(), cancellation)
            .await;
        reservation.finish(result.clone());
        result
    }

    async fn run_reserved(
        &self,
        mut request: SubagentRequest<C, H>,
        task_key: SubagentTaskKey,
        cancellation: CancellationToken,
    ) -> Result<SubagentRunResult, SubagentError> {
        let task_id = request.task_id().to_owned();
        let turn_fence = request.turn_fence();

        ensure_turn_fence(turn_fence.as_ref(), None)?;

        if cancellation.is_cancelled() {
            let result = await_with_turn_fence(
                turn_fence.as_ref(),
                None,
                self.persist_cancelled(task_key, SubagentOutcome::cancelled(task_id), None),
            )
            .await?;
            self.record_completion_under_fence(turn_fence.as_ref(), None, &result, 0)
                .await?;
            return Ok(result);
        }

        if request.resume().is_none() {
            let resume =
                await_with_turn_fence(turn_fence.as_ref(), None, self.persistence.load(&task_key))
                    .await?;
            request.set_resume(resume);
        }
        // Keep the exact pause seen at the durable read boundary. The
        // persistence seam uses it as a scoped compare-and-swap expectation
        // if execution pauses again, so a resumed pause can advance while a
        // duplicate continuation returns the newer durable winner.
        let expected_pause = request.resume().cloned();
        if cancellation.is_cancelled() {
            let result = await_with_turn_fence(
                turn_fence.as_ref(),
                None,
                self.persist_cancelled(
                    task_key,
                    SubagentOutcome::cancelled(task_id),
                    expected_pause,
                ),
            )
            .await?;
            self.record_completion_under_fence(turn_fence.as_ref(), None, &result, 0)
                .await?;
            return Ok(result);
        }

        // Reserve before any planner/executor work. A resumed lifecycle is an
        // existing child, so it takes a live slot but no new total budget.
        // The scope resolves from the parent as the key describes it: the
        // durable thread when there is one, else the parent run id. Fresh spawns
        // and continuations resolve identically.
        //
        // The key carries the parent's run id, thread and the tree's root run
        // id, so a custom scope rule that groups by root
        // (`cfg.lineage.root_run_id`) sees the real root, not the immediate
        // parent. Tags and metadata are not part of the durable key and so are
        // not available to a resolver here.
        let mut parent_config = RunConfig::new(task_key.parent_run_id.as_str());
        parent_config.lineage.root_run_id = RunId::new(task_key.root_run_id.as_str());
        if let Some(thread) = &task_key.thread_id {
            parent_config = parent_config.with_thread(thread.as_str());
        }
        let target = match request.target() {
            Some(target) => target,
            // Fail closed: an allowlist cannot vet a target it was never told.
            None if self.admission.policy().allowed_targets.is_some() => {
                return Err(SubagentError::SpawnRejected(
                    SpawnRejection::TargetNotAllowed {
                        target: String::new(),
                    },
                ));
            }
            None => "",
        };
        let reservation = if request.resume().is_some() {
            self.admission
                .try_reserve_continuation(&parent_config, target)
        } else {
            self.admission.try_reserve(&parent_config, target)
        };
        let mut reservation = reservation.map_err(SubagentError::SpawnRejected)?;

        ensure_turn_fence(turn_fence.as_ref(), None)?;
        let mut prepared =
            await_with_turn_fence(turn_fence.as_ref(), None, self.planner.prepare(request)).await?;
        if prepared.task_id != task_id {
            return Err(SubagentError::TaskIdMismatch {
                expected: task_id,
                actual: prepared.task_id,
            });
        }
        // Known as soon as the planner has named the spawn's notify mode, so a
        // cancel from here on is recorded too.
        let completion_origin = if let Some(router) = &self.completions {
            CompletionOrigin::new(&task_key, &prepared, router)?
        } else {
            None
        };
        ensure_turn_fence(turn_fence.as_ref(), None)?;
        if cancellation.is_cancelled() {
            let result = await_with_turn_fence(
                turn_fence.as_ref(),
                None,
                self.persist_cancelled(
                    task_key,
                    SubagentOutcome::cancelled(prepared.task_id),
                    expected_pause,
                ),
            )
            .await?;
            self.record_completion_under_fence(
                turn_fence.as_ref(),
                completion_origin.as_ref(),
                &result,
                0,
            )
            .await?;
            return Ok(result);
        }
        prepared.tools = restrict_tools_with_scope(
            &prepared.tools,
            prepared.role,
            prepared.tool_ceiling.as_ref(),
            &prepared.delegation_tools,
            prepared.tool_scope.as_ref(),
            prepared.skill_filter.as_deref(),
        );
        prepared
            .policy
            .budget
            .apply_call_caps(&mut prepared.run_context.config);
        // The child runs under its own token, linked to the lifecycle's: a
        // lifecycle cancellation reaches it, but a policy timeout cancels only
        // the child and never the caller's token.
        let child_token = cancellation.child_token();
        prepared.run_context = prepared.run_context.with_cancellation(child_token.clone());
        // The child is about to launch: from here its total-budget unit stays
        // spent, and the live slot is released when this function returns.
        reservation.commit();

        let policy = prepared.policy.clone();
        let result_policy = prepared.result_policy.clone();
        let mut attempts = AttemptSource::from_prepared(&prepared);
        let mut attempt = 0usize;
        let mut execution = SubagentExecution {
            prepared,
            task_key: task_key.clone(),
            cancellation: child_token,
            turn_fence: turn_fence.clone(),
        };
        let mut timed_out = false;
        let mut turn_lease_lost = false;
        let mut omitted_chars = 0usize;
        let mut token = execution.cancellation.clone();
        let executed = loop {
            if turn_fence.as_ref().is_some_and(|fence| !fence.admitted()) {
                turn_lease_lost = true;
                break Err(SubagentError::Cancelled);
            }
            token = execution.cancellation.clone();
            let run = self.executor.execute(execution);
            let timeout_token = token.clone();
            let run = async {
                match policy.timeout {
                    Some(limit) => match tokio::time::timeout(limit, run).await {
                        Ok(result) => result,
                        Err(_) => {
                            timeout_token.cancel();
                            timed_out = true;
                            tracing::debug!(
                                "{LOG_PREFIX} timeout task_id={task_id} after_ms={}",
                                limit.as_millis()
                            );
                            Ok(SubagentOutcome::incomplete(
                                task_id.clone(),
                                SubagentIncomplete::new(format!(
                                    "subagent timed out after {limit:?}"
                                ))
                                .with_kind(IncompleteKind::Timeout),
                            ))
                        }
                    },
                    None => run.await,
                }
            };
            let result = if let Some(fence) = &turn_fence {
                tokio::select! {
                    biased;
                    _ = fence.lost() => {
                        token.cancel();
                        turn_lease_lost = true;
                        Err(SubagentError::Cancelled)
                    }
                    result = run => result,
                }
            } else {
                run.await
            };
            if turn_lease_lost {
                break result;
            }
            let Err(SubagentError::Transient { message, tools_ran }) = &result else {
                break result;
            };
            let error = TinyAgentsError::Tool(message.clone());
            if cancellation.is_cancelled()
                || token.is_cancelled()
                || !may_retry(&policy, attempt, &error, *tools_ran)
                || !attempts.can_retry()
            {
                break result;
            }
            attempt += 1;
            tracing::debug!("{LOG_PREFIX} retry task_id={task_id} attempt={attempt}");
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Err(SubagentError::Cancelled),
                _ = policy.retry.sleep_backoff(attempt) => {}
            }
            let next_token = cancellation.child_token();
            match attempts.next(attempt as u32, next_token.clone()) {
                Ok(next) => {
                    execution = SubagentExecution {
                        prepared: next,
                        task_key: task_key.clone(),
                        cancellation: next_token,
                        turn_fence: turn_fence.clone(),
                    };
                }
                Err(error) => break Err(error),
            }
        };

        if turn_lease_lost || turn_fence.as_ref().is_some_and(|fence| !fence.admitted()) {
            return Err(SubagentError::Cancelled);
        }
        let outcome = match executed {
            Ok(outcome) => {
                if outcome.task_id != task_id {
                    return Err(SubagentError::TaskIdMismatch {
                        expected: task_id,
                        actual: outcome.task_id,
                    });
                }
                // The execution token is the lifecycle's own or a child of it;
                // an executor that cancels it has cancelled the run, but the
                // driver's own timeout cancel is not a cancellation.
                if cancellation.is_cancelled() || (!timed_out && token.is_cancelled()) {
                    outcome.cancelled_preserving()
                } else {
                    let (outcome, omitted) =
                        await_with_turn_fence(turn_fence.as_ref(), Some(&token), async {
                            Ok(apply_outcome_policies(outcome, &policy, &result_policy).await)
                        })
                        .await?;
                    omitted_chars = omitted;
                    outcome
                }
            }
            Err(SubagentError::Cancelled) => SubagentOutcome::cancelled(task_id),
            Err(_) if cancellation.is_cancelled() => SubagentOutcome::cancelled(task_id),
            Err(error) => {
                // A resumed child still has its durable pause: it is not
                // finished, and the router keeps the first record per task id,
                // so a failed record here would shadow the eventual finish.
                if expected_pause.is_none()
                    && let (Some(router), Some(origin)) = (&self.completions, &completion_origin)
                    && let Some(record) = origin.record_for_error(&error)
                {
                    await_with_turn_fence(turn_fence.as_ref(), Some(&token), async {
                        deliver(router, record).await;
                        Ok(())
                    })
                    .await?;
                }
                return Err(error);
            }
        };
        let result = await_with_turn_fence(
            turn_fence.as_ref(),
            Some(&token),
            self.persist(task_key, outcome, expected_pause, &cancellation),
        )
        .await?;
        self.record_completion_under_fence(
            turn_fence.as_ref(),
            completion_origin.as_ref(),
            &result,
            omitted_chars,
        )
        .await?;
        Ok(result)
    }

    async fn record_completion_under_fence(
        &self,
        fence: Option<&Arc<dyn DeliveryFence>>,
        origin: Option<&CompletionOrigin>,
        result: &SubagentRunResult,
        omitted_chars: usize,
    ) -> Result<(), SubagentError> {
        await_with_turn_fence(fence, None, async {
            self.record_completion(origin, result, omitted_chars).await;
            Ok(())
        })
        .await
    }

    /// Records `result` when this invocation owns its host effects and the
    /// outcome maps to a completion (a pause does not).
    async fn record_completion(
        &self,
        origin: Option<&CompletionOrigin>,
        result: &SubagentRunResult,
        omitted_chars: usize,
    ) {
        if let (Some(router), Some(origin)) = (&self.completions, origin)
            && result.should_emit_host_effects()
            && let Some(record) = origin.record_for_outcome(&result.outcome, omitted_chars)
        {
            deliver(router, record).await;
        }
    }

    async fn persist_cancelled(
        &self,
        task_key: SubagentTaskKey,
        mut outcome: SubagentOutcome,
        expected_pause: Option<super::SubagentResume>,
    ) -> Result<SubagentRunResult, SubagentError> {
        outcome.status = SubagentOutcomeKind::Cancelled;
        // Once cancellation has won, this is the one terminal action. Do not
        // race it with the already-latched token or a caller could observe an
        // indeterminate terminal write.
        match self
            .persistence
            .record_terminal(&task_key, &outcome, expected_pause.as_ref())
            .await?
        {
            SubagentTerminalPersistenceDisposition::Inserted => {
                self.cache_terminal(task_key, &outcome).await;
                Ok(SubagentRunResult::new(
                    outcome,
                    SubagentPersistenceDisposition::TerminalInserted,
                ))
            }
            SubagentTerminalPersistenceDisposition::Existing => {
                self.load_terminal_winner(task_key).await
            }
            SubagentTerminalPersistenceDisposition::PauseExisting => {
                self.load_pause_winner(task_key).await
            }
        }
    }

    async fn persist(
        &self,
        task_key: SubagentTaskKey,
        outcome: SubagentOutcome,
        expected_pause: Option<super::SubagentResume>,
        cancellation: &CancellationToken,
    ) -> Result<SubagentRunResult, SubagentError> {
        if matches!(&outcome.status, SubagentOutcomeKind::Cancelled) {
            return self
                .persist_cancelled(task_key, outcome, expected_pause)
                .await;
        }
        let disposition = match &outcome.status {
            SubagentOutcomeKind::AwaitingInput(_) => {
                match self
                    .commit_or_cancel(
                        self.persistence.save_pause(PersistedSubagentPause {
                            key: task_key.clone(),
                            outcome: outcome.clone(),
                            replaces: expected_pause.clone(),
                        }),
                        cancellation,
                    )
                    .await?
                {
                    Some(SubagentPausePersistenceDisposition::Inserted) => {
                        SubagentPersistenceDisposition::PauseCommitted
                    }
                    Some(SubagentPausePersistenceDisposition::Replaced) => {
                        SubagentPersistenceDisposition::PauseReplaced
                    }
                    Some(SubagentPausePersistenceDisposition::Existing) => {
                        return self.load_pause_winner(task_key).await;
                    }
                    Some(SubagentPausePersistenceDisposition::TerminalExisting) => {
                        return self.load_terminal_winner(task_key).await;
                    }
                    None => {
                        return self
                            .persist_cancelled(task_key, outcome, expected_pause)
                            .await;
                    }
                }
            }
            SubagentOutcomeKind::Completed | SubagentOutcomeKind::Incomplete(_) => {
                match self
                    .commit_or_cancel(
                        async {
                            self.persistence
                                .record_terminal(&task_key, &outcome, expected_pause.as_ref())
                                .await
                        },
                        cancellation,
                    )
                    .await?
                {
                    Some(SubagentTerminalPersistenceDisposition::Inserted) => {
                        SubagentPersistenceDisposition::TerminalInserted
                    }
                    Some(SubagentTerminalPersistenceDisposition::Existing) => {
                        SubagentPersistenceDisposition::TerminalExisting
                    }
                    Some(SubagentTerminalPersistenceDisposition::PauseExisting) => {
                        return self.load_pause_winner(task_key).await;
                    }
                    None => {
                        return self
                            .persist_cancelled(task_key, outcome, expected_pause)
                            .await;
                    }
                }
            }
            SubagentOutcomeKind::Cancelled => unreachable!("handled before persistence race"),
        };
        if matches!(
            &outcome.status,
            SubagentOutcomeKind::Completed | SubagentOutcomeKind::Incomplete(_)
        ) {
            let terminal = if matches!(
                disposition,
                SubagentPersistenceDisposition::TerminalExisting
            ) {
                self.persistence
                    .load_terminal(&task_key)
                    .await?
                    .ok_or_else(|| {
                        SubagentError::Persistence(
                            "terminal insert lost without durable outcome".into(),
                        )
                    })?
            } else {
                outcome.clone()
            };
            self.cache_terminal(task_key, &terminal).await;
            return Ok(SubagentRunResult::new(terminal, disposition));
        }
        Ok(SubagentRunResult::new(outcome, disposition))
    }

    async fn load_pause_winner(
        &self,
        task_key: SubagentTaskKey,
    ) -> Result<SubagentRunResult, SubagentError> {
        // A terminal can win after the pause CAS reports contention. It is
        // authoritative over every prior pause and must never be reopened.
        if let Some(terminal) = self.persistence.load_terminal(&task_key).await? {
            self.cache_terminal(task_key, &terminal).await;
            return Ok(SubagentRunResult::new(
                terminal,
                SubagentPersistenceDisposition::TerminalExisting,
            ));
        }
        let Some(paused) = self.persistence.load_pause(&task_key).await? else {
            // A different driver can consume the pause into a terminal between
            // the first terminal read and this pause read. Recheck the
            // authoritative terminal before classifying that interleaving as a
            // persistence failure.
            if let Some(terminal) = self.persistence.load_terminal(&task_key).await? {
                self.cache_terminal(task_key, &terminal).await;
                return Ok(SubagentRunResult::new(
                    terminal,
                    SubagentPersistenceDisposition::TerminalExisting,
                ));
            }
            return Err(SubagentError::Persistence(
                "pause compare-and-swap lost without a durable pause or terminal outcome".into(),
            ));
        };
        if !matches!(paused.status, SubagentOutcomeKind::AwaitingInput(_)) {
            return Err(SubagentError::Persistence(
                "durable pause record did not contain an awaiting-input outcome".into(),
            ));
        }
        Ok(SubagentRunResult::new(
            paused,
            SubagentPersistenceDisposition::PauseExisting,
        ))
    }

    async fn load_terminal_winner(
        &self,
        task_key: SubagentTaskKey,
    ) -> Result<SubagentRunResult, SubagentError> {
        let terminal = self
            .persistence
            .load_terminal(&task_key)
            .await?
            .ok_or_else(|| {
                SubagentError::Persistence(
                    "terminal compare-and-swap lost without a durable terminal outcome".into(),
                )
            })?;
        self.cache_terminal(task_key, &terminal).await;
        Ok(SubagentRunResult::new(
            terminal,
            SubagentPersistenceDisposition::TerminalExisting,
        ))
    }

    async fn commit_or_cancel<F, T>(
        &self,
        operation: F,
        cancellation: &CancellationToken,
    ) -> Result<Option<T>, SubagentError>
    where
        F: Future<Output = Result<T, SubagentError>>,
    {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(None),
            result = operation => {
                Ok(Some(result?))
            }
        }
    }

    async fn cache_terminal(&self, key: SubagentTaskKey, outcome: &SubagentOutcome) {
        self.terminal_outcomes
            .lock()
            .await
            .insert(key, outcome.clone());
    }
}
