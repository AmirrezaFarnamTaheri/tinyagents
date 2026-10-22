//! Evaluate host stop policies after each model response.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use tinyinference_llm::model::ModelResponse;

use crate::context::{MiddlewareControl, RunContext};
use crate::cost::TurnCost;
use crate::error::Result;
use crate::middleware::{Middleware, StopDecision, StopHook, StopHookMetrics, StopReport, TurnState};

type StopObserver = Arc<dyn Fn(StopReport) + Send + Sync>;

struct RunState<M> {
    lifecycle: Weak<()>,
    metrics: M,
    iteration: u32,
}

/// Evaluates host-provided stop hooks after every model response.
///
/// Metrics are kept per `RunContext`, so one middleware instance can safely
/// serve concurrent runs. The first stop vote wins and becomes an interrupt at
/// the agent loop's next safe checkpoint.
pub struct StopHookMiddleware<M: StopHookMetrics = TurnCost> {
    model: String,
    hooks: Vec<Arc<dyn StopHook<M>>>,
    metrics_seed: M,
    runs: Mutex<HashMap<u64, RunState<M>>>,
    observer: Option<StopObserver>,
}

impl<M: StopHookMetrics + Default> StopHookMiddleware<M> {
    /// Creates a stop-hook middleware with default metrics.
    pub fn new(model: impl Into<String>, hooks: Vec<Arc<dyn StopHook<M>>>) -> Self {
        Self::with_metrics_seed(model, hooks, M::default())
    }
}

impl<M: StopHookMetrics> StopHookMiddleware<M> {
    /// Creates a stop-hook middleware with an initial metrics snapshot.
    pub fn with_metrics_seed(
        model: impl Into<String>,
        hooks: Vec<Arc<dyn StopHook<M>>>,
        metrics_seed: M,
    ) -> Self {
        Self {
            model: model.into(),
            hooks,
            metrics_seed,
            runs: Mutex::new(HashMap::new()),
            observer: None,
        }
    }

    /// Sets an observer for stop votes after the winning hook is selected.
    pub fn with_stop_observer(
        mut self,
        observer: impl Fn(StopReport) + Send + Sync + 'static,
    ) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }

    fn lock_runs(&self) -> std::sync::MutexGuard<'_, HashMap<u64, RunState<M>>> {
        self.runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl<State, Ctx, M> Middleware<State, Ctx> for StopHookMiddleware<M>
where
    State: Send + Sync,
    Ctx: Send + Sync,
    M: StopHookMetrics,
{
    fn name(&self) -> &str {
        "stop_hooks"
    }

    async fn after_model(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        response: &mut ModelResponse,
    ) -> Result<()> {
        let (metrics, iteration) = {
            let mut runs = self.lock_runs();
            runs.retain(|_, run| run.lifecycle.strong_count() > 0);
            let run = runs
                .entry(ctx.instance_id())
                .or_insert_with(|| RunState {
                    lifecycle: Arc::downgrade(&ctx.lifecycle),
                    metrics: self.metrics_seed.clone(),
                    iteration: 0,
                });
            run.iteration = run.iteration.saturating_add(1);
            run.metrics.record_model(&self.model, response);
            (run.metrics.clone(), run.iteration)
        };
        let max_iterations = u32::try_from(ctx.config.effective_max_model_calls())
            .unwrap_or(u32::MAX);
        let state = TurnState {
            iteration,
            max_iterations,
            cost: &metrics,
            model: &self.model,
        };
        for hook in &self.hooks {
            if let StopDecision::Stop { reason } = hook.check(&state).await {
                let report = StopReport {
                    hook: hook.name().to_owned(),
                    iteration,
                    model: self.model.clone(),
                    reason: reason.clone(),
                };
                if let Some(observer) = self.observer.as_ref() {
                    observer(report);
                }
                ctx.request_control(MiddlewareControl::Interrupt {
                    node: "stop_hook".into(),
                    message: reason,
                });
                break;
            }
        }
        Ok(())
    }

    async fn after_agent(
        &self,
        ctx: &mut RunContext<Ctx>,
        _state: &State,
        _run: &mut crate::middleware::AgentRun,
    ) -> Result<()> {
        self.lock_runs().remove(&ctx.instance_id());
        Ok(())
    }
}

#[cfg(test)]
#[path = "stop_hooks_tests.rs"]
mod tests;
