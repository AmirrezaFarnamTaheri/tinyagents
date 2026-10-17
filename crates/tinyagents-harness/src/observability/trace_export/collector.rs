//! Event-driven span collection for one root run and its child run tree.
//!
//! The collector consumes durable `AgentObservation`s directly. It owns only
//! generic event-to-span projection; product identity and content policy are
//! supplied in [`TraceContext`].

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Value, json};

use crate::events::AgentEvent;
use crate::ids::{CallId, RunId};
use crate::observability::AgentObservation;

pub use crate::events::{GenerationEnrichment, GenerationPricing};

use super::serialize::{
    MAX_ERROR_MESSAGE_CHARS, capture_model_content, json_f64, json_str, json_u64,
    truncate_capture_text,
};
use super::{SpanKind, SpanStatus, TraceContext, TraceSpan};

const UNSTREAMED_DELTA_WINDOW_MS: u64 = 100;

#[derive(Debug, Clone)]
struct CallStart {
    index: usize,
    started_at_ms: u64,
    first_delta_ms: Option<u64>,
    first_text_ms: Option<u64>,
}

/// Failure to enrich a generation span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GenerationEnrichmentError {
    /// No generation exists for this exact run and call identity.
    #[error("generation not found for the given run and call IDs")]
    GenerationNotFound,
    /// The supplied estimated cost or price is negative or not finite.
    #[error("generation pricing and estimated cost must be finite and non-negative")]
    InvalidPricing,
}

/// Folds native harness observations into a trace span tree.
///
/// Create one collector per root run, call [`record`](Self::record) with
/// observations from that run tree, then call [`finish`](Self::finish) when
/// the stream closes. Call IDs and observation offsets are scoped to their
/// explicit run identities. Duplicate starts force-close the abandoned span;
/// unmatched terminal events synthesize a span so partial journals retain
/// meaningful terminal outcomes.
#[derive(Debug)]
pub struct SpanCollector {
    context: TraceContext,
    spans: Vec<TraceSpan>,
    root: Option<usize>,
    open_models: HashMap<String, CallStart>,
    generation_by_call: HashMap<String, usize>,
    cached_calls: HashSet<String>,
    open_tools: HashMap<String, usize>,
    open_subagents: HashMap<String, usize>,
    linked_subagent_names: HashSet<String>,
    run_roots: HashMap<String, usize>,
    current_run_id: Option<String>,
    root_run_id: Option<String>,
    observation_root_id: Option<String>,
    current_turns: HashMap<String, u32>,
    open_iterations: HashMap<String, usize>,
    pending_turn_stops: HashMap<String, (u32, crate::turn_stop::TurnStop)>,
    pending_generation_enrichment: HashMap<String, GenerationEnrichment>,
    last_generation: HashMap<String, usize>,
    last_activity_ms: u64,
    first_input_recorded: bool,
    finished: bool,
    last_offsets: HashMap<String, u64>,
}

impl SpanCollector {
    /// Creates an empty collector for `context`.
    pub fn new(context: TraceContext) -> Self {
        let root_run_id = context
            .root_run_id
            .clone()
            .or_else(|| context.run_id.clone());
        Self {
            context,
            spans: Vec::new(),
            root: None,
            open_models: HashMap::new(),
            generation_by_call: HashMap::new(),
            cached_calls: HashSet::new(),
            open_tools: HashMap::new(),
            open_subagents: HashMap::new(),
            linked_subagent_names: HashSet::new(),
            run_roots: HashMap::new(),
            current_run_id: None,
            root_run_id,
            observation_root_id: None,
            current_turns: HashMap::new(),
            open_iterations: HashMap::new(),
            pending_turn_stops: HashMap::new(),
            pending_generation_enrichment: HashMap::new(),
            last_generation: HashMap::new(),
            last_activity_ms: 0,
            first_input_recorded: false,
            finished: false,
            last_offsets: HashMap::new(),
        }
    }

    /// Selects the observation view root independently of trace lineage
    /// metadata.
    ///
    /// A host rebasing a child journal can preserve its original parent/root
    /// IDs in [`TraceContext`] while selecting the child run as the observation
    /// root. Without this override, observations are filtered against the
    /// context's `root_run_id` when present.
    pub fn with_observation_root(mut self, run_id: RunId) -> Self {
        self.observation_root_id = Some(run_id.as_str().to_owned());
        self
    }

    /// Folds one durable observation using its recorded timestamp.
    pub fn record(&mut self, observation: &AgentObservation) {
        let run_id = observation.run_id.as_str();
        if self
            .observation_root_id
            .as_deref()
            .or(self.root_run_id.as_deref())
            .is_some_and(|root| root != observation.root_run_id.as_str())
        {
            return;
        }
        self.root_run_id
            .get_or_insert_with(|| observation.root_run_id.as_str().to_owned());
        if self
            .last_offsets
            .get(run_id)
            .is_some_and(|offset| observation.offset <= *offset)
        {
            return;
        }
        self.last_offsets
            .insert(run_id.to_owned(), observation.offset);
        let previous_run_id = self.current_run_id.replace(run_id.to_owned());
        if !self.run_roots.contains_key(run_id) {
            let index = if observation.parent_run_id.is_some() {
                let parent_id = observation
                    .parent_run_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned());
                let parent = parent_id
                    .as_deref()
                    .and_then(|id| self.run_roots.get(id))
                    .copied()
                    .unwrap_or_else(|| self.ensure_root(observation.ts_ms));
                self.open_subagent_span("child", 1, observation.ts_ms, parent)
            } else {
                self.ensure_root(observation.ts_ms)
            };
            self.run_roots.insert(run_id.to_owned(), index);
        }
        self.fold_event(&observation.event, observation.ts_ms);
        self.current_run_id = previous_run_id;
    }

    /// Folds a live event using the caller's observation timestamp.
    ///
    /// Without an observation identity this is a root-run convenience; child
    /// events should be passed through [`Self::record`] so lineage and scoped
    /// call IDs are preserved.
    pub fn record_event(&mut self, event: &AgentEvent, timestamp_ms: u64) {
        let previous_run_id = self.current_run_id.take();
        let root_run_id = self
            .root_run_id
            .clone()
            .or_else(|| self.context.run_id.clone())
            .or_else(|| match event {
                AgentEvent::RunStarted { run_id, .. } => Some(run_id.as_str().to_owned()),
                _ => None,
            })
            .unwrap_or_else(|| "root".into());
        self.root_run_id.get_or_insert_with(|| root_run_id.clone());
        self.current_run_id = Some(root_run_id);
        self.fold_event(event, timestamp_ms);
        self.current_run_id = previous_run_id;
    }

    fn fold_event(&mut self, event: &AgentEvent, timestamp_ms: u64) {
        if self.finished {
            return;
        }
        self.last_activity_ms = self.last_activity_ms.max(timestamp_ms);
        match event {
            AgentEvent::RunStarted { .. } => {
                self.ensure_run_root(timestamp_ms);
            }
            AgentEvent::ModelStarted { call_id, model } => {
                if let Some(turn) = self.current_turns.get(&self.active_run_id()).copied() {
                    self.start_iteration(turn, timestamp_ms);
                }
                let parent = self.active_parent(timestamp_ms);
                let mut attrs = BTreeMap::new();
                attrs.insert("gen_ai.request.model".into(), json_str(model));
                let (index, _) = self.open_span(
                    SpanKind::Generation,
                    format!("llm.{}", nonempty(model, "unknown")),
                    Some(parent),
                    timestamp_ms,
                    attrs,
                );
                let call_key = self.scoped_id(call_id.as_str());
                self.pending_generation_enrichment.remove(&call_key);
                self.generation_by_call.insert(call_key.clone(), index);
                if let Some(previous) = self.open_models.insert(
                    call_key,
                    CallStart {
                        index,
                        started_at_ms: timestamp_ms,
                        first_delta_ms: None,
                        first_text_ms: None,
                    },
                ) {
                    self.force_close_span(previous.index, timestamp_ms);
                }
            }
            AgentEvent::ModelDelta { call_id, delta, .. } => {
                let has_content = !delta.text.is_empty()
                    || !delta.reasoning.is_empty()
                    || delta.tool_call.is_some();
                if has_content
                    && let Some(call) = self.open_models.get_mut(&self.scoped_id(call_id.as_str()))
                {
                    call.first_delta_ms.get_or_insert(timestamp_ms);
                    if !delta.text.is_empty() {
                        call.first_text_ms.get_or_insert(timestamp_ms);
                    }
                }
            }
            AgentEvent::ModelCompleted {
                call_id,
                started_at_ms,
                usage,
                input,
                output,
            } => {
                let call_key = self.scoped_id(call_id.as_str());
                let cache_hit = self.cached_calls.remove(&call_key);
                let call = self.open_models.remove(&call_key).unwrap_or_else(|| {
                    let parent = self.active_parent(timestamp_ms);
                    let mut attrs = BTreeMap::new();
                    attrs.insert("gen_ai.request.model".into(), json_str("unknown"));
                    let (index, _) = self.open_span(
                        SpanKind::Generation,
                        "llm.unknown".into(),
                        Some(parent),
                        started_at_ms.unwrap_or(timestamp_ms),
                        attrs,
                    );
                    CallStart {
                        index,
                        started_at_ms: started_at_ms.unwrap_or(timestamp_ms),
                        first_delta_ms: None,
                        first_text_ms: None,
                    }
                });
                self.generation_by_call
                    .entry(call_key.clone())
                    .or_insert(call.index);
                let completed_index = call.index;
                let cached_usage = usage.as_ref().filter(|_| cache_hit).map(|usage| {
                    let mut usage = *usage;
                    usage.charged_amount = None;
                    usage
                });
                self.finish_model(
                    call,
                    *started_at_ms,
                    if cache_hit {
                        cached_usage.as_ref()
                    } else {
                        usage.as_ref()
                    },
                    input.as_ref(),
                    output.as_ref(),
                    timestamp_ms,
                );
                if cache_hit {
                    self.spans[completed_index]
                        .attributes
                        .insert("gen_ai.cache.hit".into(), Value::Bool(true));
                    self.spans[completed_index]
                        .attributes
                        .insert("gen_ai.cost.source".into(), json_str("cached"));
                }
                if let Some(enrichment) = self.pending_generation_enrichment.remove(&call_key) {
                    let run_id = RunId::new(self.active_run_id());
                    let _ = self.enrich_generation(&run_id, call_id, enrichment);
                }
                self.last_generation
                    .insert(self.active_run_id(), completed_index);
            }
            AgentEvent::ModelFailed {
                call_id,
                model,
                started_at_ms,
                error,
                ..
            } => {
                let call = self
                    .open_models
                    .remove(&self.scoped_id(call_id.as_str()))
                    .unwrap_or_else(|| {
                        let parent = self.active_parent(timestamp_ms);
                        let mut attrs = BTreeMap::new();
                        attrs.insert("gen_ai.request.model".into(), json_str(model));
                        let (index, _) = self.open_span(
                            SpanKind::Generation,
                            format!("llm.{}", nonempty(model, "unknown")),
                            Some(parent),
                            started_at_ms.unwrap_or(timestamp_ms),
                            attrs,
                        );
                        CallStart {
                            index,
                            started_at_ms: started_at_ms.unwrap_or(timestamp_ms),
                            first_delta_ms: None,
                            first_text_ms: None,
                        }
                    });
                let start = started_at_ms
                    .unwrap_or(call.started_at_ms)
                    .min(timestamp_ms);
                self.close_span(call.index, timestamp_ms, SpanStatus::Error);
                self.put_error(call.index, error);
                self.spans[call.index].start_unix_ms = start;
                let key = self.scoped_id(call_id.as_str());
                if let Some(mut enrichment) = self.pending_generation_enrichment.remove(&key) {
                    // Provider and price metadata remain useful for a failed
                    // generation, but no host estimate represents a charge
                    // unless the generation completed.
                    enrichment.estimated_cost_usd = None;
                    let run_id = RunId::new(self.active_run_id());
                    let _ = self.enrich_generation(&run_id, call_id, enrichment);
                }
            }
            AgentEvent::ToolStarted {
                call_id,
                tool_name,
                input,
                parent_call_id,
            } => {
                let parent = parent_call_id
                    .as_ref()
                    .and_then(|id| self.open_tools.get(&self.scoped_id(id.as_str())).copied())
                    .unwrap_or_else(|| self.active_parent(timestamp_ms));
                let mut attrs = BTreeMap::new();
                attrs.insert("tool.name".into(), json_str(tool_name));
                attrs.insert("tool.call_id".into(), json_str(call_id.as_str()));
                let (index, _) = self.open_span(
                    SpanKind::Tool,
                    format!("tool.{tool_name}"),
                    Some(parent),
                    timestamp_ms,
                    attrs,
                );
                if self.context.capture_content
                    && let Some(input) = input.as_ref().filter(|v| !v.is_null())
                {
                    self.spans[index].input =
                        Some(Value::String(truncate_capture_text(&input.to_string())));
                }
                if let Some(previous) = self
                    .open_tools
                    .insert(self.scoped_id(call_id.as_str()), index)
                {
                    self.force_close_span(previous, timestamp_ms);
                }
            }
            AgentEvent::ToolCompleted {
                call_id,
                tool_name,
                started_at_ms,
                input,
                output,
                duration_ms,
                error,
                ..
            } => {
                let index = self
                    .open_tools
                    .remove(&self.scoped_id(call_id.as_str()))
                    .unwrap_or_else(|| {
                        let parent = self.active_parent(timestamp_ms);
                        let mut attrs = BTreeMap::new();
                        attrs.insert("tool.name".into(), json_str(tool_name));
                        attrs.insert("tool.call_id".into(), json_str(call_id.as_str()));
                        self.open_span(
                            SpanKind::Tool,
                            format!("tool.{tool_name}"),
                            Some(parent),
                            started_at_ms.unwrap_or(timestamp_ms),
                            attrs,
                        )
                        .0
                    });
                {
                    if self.context.capture_content {
                        if self.spans[index].input.is_none()
                            && let Some(input) = input.as_ref().filter(|v| !v.is_null())
                        {
                            self.spans[index].input =
                                Some(Value::String(truncate_capture_text(&input.to_string())));
                        }
                        self.spans[index].output = output
                            .as_ref()
                            .map(|v| Value::String(truncate_capture_text(&v.to_string())));
                    }
                    if let Some(duration) = duration_ms {
                        let end = self.spans[index].start_unix_ms.saturating_add(*duration);
                        self.close_span(
                            index,
                            end,
                            if error.is_some() {
                                SpanStatus::Error
                            } else {
                                SpanStatus::Ok
                            },
                        );
                    } else {
                        self.close_span(
                            index,
                            timestamp_ms,
                            if error.is_some() {
                                SpanStatus::Error
                            } else {
                                SpanStatus::Ok
                            },
                        );
                    }
                    if let Some(error) = error {
                        self.put_error(index, error);
                    }
                }
            }
            AgentEvent::ToolFailed {
                call_id,
                tool_name,
                started_at_ms,
                duration_ms,
                error,
                ..
            } => {
                let index = self
                    .open_tools
                    .remove(&self.scoped_id(call_id.as_str()))
                    .unwrap_or_else(|| {
                        let parent = self.active_parent(timestamp_ms);
                        let mut attrs = BTreeMap::new();
                        attrs.insert("tool.name".into(), json_str(tool_name));
                        attrs.insert("tool.call_id".into(), json_str(call_id.as_str()));
                        self.open_span(
                            SpanKind::Tool,
                            format!("tool.{tool_name}"),
                            Some(parent),
                            started_at_ms.unwrap_or(timestamp_ms),
                            attrs,
                        )
                        .0
                    });
                {
                    let end = duration_ms
                        .map(|duration| self.spans[index].start_unix_ms.saturating_add(duration))
                        .unwrap_or(timestamp_ms);
                    self.close_span(index, end, SpanStatus::Error);
                    self.put_error(index, error);
                }
            }
            AgentEvent::SubAgentStarted { name, depth } => {
                let index = self.open_subagent(name, *depth, timestamp_ms);
                self.open_subagents.insert(name.clone(), index);
            }
            AgentEvent::SubAgentRunStarted {
                name,
                depth,
                run_id,
            } => {
                let index = self
                    .run_roots
                    .get(run_id.as_str())
                    .copied()
                    .unwrap_or_else(|| {
                        self.open_subagents
                            .remove(name)
                            .unwrap_or_else(|| self.open_subagent(name, *depth, timestamp_ms))
                    });
                self.spans[index].name = format!("subagent.{name}");
                self.spans[index]
                    .attributes
                    .insert("subagent.name".into(), json_str(name));
                self.spans[index]
                    .attributes
                    .insert("subagent.depth".into(), json!(*depth));
                self.spans[index]
                    .attributes
                    .insert("run.id".into(), json_str(run_id.as_str()));
                self.linked_subagent_names.insert(name.clone());
                self.run_roots.insert(run_id.as_str().to_owned(), index);
            }
            AgentEvent::SubAgentRunCompleted { run_id } => {
                if let Some(index) = self.run_roots.get(run_id.as_str()).copied() {
                    self.close_span(index, timestamp_ms, SpanStatus::Ok);
                }
            }
            AgentEvent::SubAgentCompleted { name, .. } => {
                if let Some(index) = self.open_subagents.remove(name) {
                    self.close_span(index, timestamp_ms, SpanStatus::Ok);
                } else if !self.linked_subagent_names.contains(name) {
                    let index = self.open_subagent(name, 0, timestamp_ms);
                    self.close_span(index, timestamp_ms, SpanStatus::Ok);
                }
            }
            AgentEvent::SubAgentFailed { name, depth, error } => {
                if let Some(index) = self.open_subagents.remove(name) {
                    self.close_span(index, timestamp_ms, SpanStatus::Error);
                    self.put_error(index, error);
                } else if !self.linked_subagent_names.contains(name) {
                    let index = self.open_subagent(name, *depth, timestamp_ms);
                    self.close_span(index, timestamp_ms, SpanStatus::Error);
                    self.put_error(index, error);
                }
            }
            AgentEvent::MessageAppended { role, message, .. } if self.context.capture_content => {
                if role == "user" && !self.first_input_recorded {
                    if let Some(message) = message
                        && let Some(root) = self.root
                    {
                        self.spans[root].input = Some(capture_model_content(message));
                        self.first_input_recorded = true;
                    }
                } else if role == "assistant"
                    && let (Some(root), Some(message)) = (self.root, message)
                {
                    self.spans[root].output = Some(capture_model_content(message));
                }
            }
            AgentEvent::UsageRecorded { usage } => {
                let root = self.ensure_run_root(timestamp_ms);
                let cached = usage
                    .cache_read_tokens
                    .saturating_add(usage.cache_creation_tokens);
                let input = usage
                    .input_tokens
                    .max(cached)
                    .max(
                        usage
                            .input_tokens
                            .saturating_add(if usage.input_tokens < cached {
                                cached
                            } else {
                                0
                            }),
                    );
                for (key, add) in [
                    ("gen_ai.usage.input_tokens", input),
                    ("gen_ai.usage.output_tokens", usage.output_tokens),
                    ("gen_ai.usage.cached_input_tokens", usage.cache_read_tokens),
                    (
                        "gen_ai.usage.cache_creation_tokens",
                        usage.cache_creation_tokens,
                    ),
                    ("gen_ai.usage.reasoning_tokens", usage.reasoning_tokens),
                ] {
                    let prior = self.spans[root]
                        .attributes
                        .get(key)
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    self.spans[root]
                        .attributes
                        .insert(key.into(), json_u64(prior.saturating_add(add)));
                }
                if let Some(charge) = usage.charged_amount {
                    let prior = self.spans[root]
                        .attributes
                        .get("gen_ai.usage.cost_usd")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0);
                    self.spans[root].attributes.insert(
                        "gen_ai.usage.cost_usd".into(),
                        json_f64(prior + charge.micros as f64 / 1_000_000.0),
                    );
                    self.spans[root]
                        .attributes
                        .insert("gen_ai.cost.source".into(), json_str("provider_reported"));
                }
            }
            AgentEvent::GenerationEnriched {
                call_id,
                enrichment,
            } => {
                if enrichment.validate().is_ok() {
                    let key = self.scoped_id(call_id.as_str());
                    let mut projected = enrichment.clone();
                    if self
                        .generation_by_call
                        .get(&key)
                        .is_some_and(|index| self.spans[*index].status == SpanStatus::Error)
                    {
                        // A late host enrichment may retain provider and rate
                        // metadata on a failed generation, but cannot imply a
                        // completed-call charge.
                        projected.estimated_cost_usd = None;
                    }
                    if self.open_models.contains_key(&key) {
                        self.pending_generation_enrichment
                            .entry(key)
                            .and_modify(|pending| merge_generation_enrichment(pending, &projected))
                            .or_insert(projected);
                    } else {
                        let run_id = RunId::new(self.active_run_id());
                        let _ = self.enrich_generation(&run_id, call_id, projected);
                    }
                }
            }
            AgentEvent::CostRecorded { cost } => {
                let amount = cost.total_cost;
                if let Some(generation) = self.last_generation.get(&self.active_run_id()).copied() {
                    let provider_reported = self.spans[generation]
                        .attributes
                        .get("gen_ai.cost.source")
                        .and_then(Value::as_str)
                        == Some("provider_reported");
                    let cached = self.spans[generation]
                        .attributes
                        .get("gen_ai.cache.hit")
                        .and_then(Value::as_bool)
                        == Some(true);
                    if !provider_reported && !cached {
                        self.set_generation_cost(generation, amount, "estimated");
                    }
                }
            }
            AgentEvent::CacheHit { call_id, .. } => {
                self.cached_calls.insert(self.scoped_id(call_id.as_str()));
            }
            AgentEvent::RunCompleted { outcome, .. } => {
                self.close_open_iteration(
                    timestamp_ms,
                    terminal_iteration_status(outcome.as_ref()),
                );
                self.close_completed_run(outcome.as_ref(), timestamp_ms);
                self.apply_pending_turn_stop();
            }
            AgentEvent::RunFailed { error, outcome, .. } => {
                self.close_open_iteration(timestamp_ms, failed_iteration_status(outcome.as_ref()));
                self.close_failed_run(outcome.as_ref(), error, timestamp_ms);
                self.apply_pending_turn_stop();
            }
            AgentEvent::RunFinalized { run_id } => {
                if run_id.as_str() == self.active_run_id() {
                    self.finalize_run(run_id, timestamp_ms);
                }
            }
            AgentEvent::TurnStarted { turn } => {
                self.current_turns.insert(self.active_run_id(), *turn);
            }
            AgentEvent::TurnCompleted { turn, .. } => {
                let run_id = self.active_run_id();
                let open_turn = self.open_iterations.get(&run_id).and_then(|index| {
                    self.spans[*index]
                        .attributes
                        .get("agent.iteration")
                        .and_then(Value::as_u64)
                });
                if open_turn == Some(u64::from(*turn)) {
                    self.close_open_iteration(timestamp_ms, SpanStatus::Ok);
                }
                if self.current_turns.get(&run_id) == Some(turn) {
                    self.current_turns.remove(&run_id);
                }
            }
            AgentEvent::TurnStopped { turn, stop } => {
                self.record_turn_stop(*turn, stop, timestamp_ms)
            }
            _ => {}
        }
    }

    fn record_turn_stop(&mut self, turn: u32, stop: &crate::turn_stop::TurnStop, at: u64) {
        let run_id = self.active_run_id();
        let stop = stop.sanitized();
        let root = self
            .run_roots
            .get(&run_id)
            .copied()
            .unwrap_or_else(|| self.ensure_run_root(at));
        match self.spans[root]
            .attributes
            .get("turn.outcome")
            .and_then(Value::as_str)
        {
            Some("completed" | "stopped") => self.apply_turn_stop(root, turn, &stop),
            None => {
                self.pending_turn_stops.insert(run_id, (turn, stop));
            }
            Some(_) => {
                self.pending_turn_stops.remove(&run_id);
            }
        }
    }

    fn start_iteration(&mut self, turn: u32, at: u64) {
        self.close_open_iteration(at, SpanStatus::Unset);
        let parent = self.active_parent(at);
        let is_subagent = self.spans[parent].kind == SpanKind::Subagent;
        let (kind, prefix) = if is_subagent {
            (SpanKind::SubagentIteration, "subagent.iteration")
        } else {
            (SpanKind::Iteration, "agent.iteration")
        };
        let mut attrs = BTreeMap::new();
        attrs.insert("agent.iteration".into(), json!(turn));
        let (index, _) = self.open_span(kind, format!("{prefix}#{turn}"), Some(parent), at, attrs);
        self.open_iterations.insert(self.active_run_id(), index);
    }

    fn close_open_iteration(&mut self, at: u64, status: SpanStatus) {
        if let Some(index) = self.open_iterations.remove(&self.active_run_id()) {
            self.close_span(index, at, status);
        }
    }

    fn apply_pending_turn_stop(&mut self) {
        let run_id = self.active_run_id();
        let Some((turn, stop)) = self.pending_turn_stops.remove(&run_id) else {
            return;
        };
        if let Some(root) = self.run_roots.get(&run_id).copied() {
            self.apply_turn_stop(root, turn, &stop);
        }
    }

    fn apply_turn_stop(&mut self, root: usize, turn: u32, stop: &crate::turn_stop::TurnStop) {
        let terminal_outcome = self.spans[root]
            .attributes
            .get("turn.outcome")
            .and_then(Value::as_str);
        let can_enrich = match terminal_outcome {
            Some("completed") => self.spans[root].status == SpanStatus::Ok,
            Some("stopped") => self.spans[root].status == SpanStatus::Unset,
            _ => false,
        };
        if !can_enrich {
            return;
        }

        let stop = stop.sanitized();
        let message = stop.status_message();
        let attrs = &mut self.spans[root].attributes;
        attrs.insert("turn.outcome".into(), json_str("stopped"));
        attrs.insert("turn.stop_turn".into(), json!(turn));
        attrs.insert("turn.stop_kind".into(), json_str(stop.kind.as_str()));
        attrs.remove("turn.stop_limit");
        if let Some(class) = stop.failure_class.as_ref() {
            attrs.insert("turn.stop_class".into(), json_str(class.as_str()));
        } else {
            attrs.remove("turn.stop_class");
        }
        if let Some(operation) = stop.operation.as_deref() {
            attrs.insert("turn.stop_operation".into(), json_str(operation));
        } else {
            attrs.remove("turn.stop_operation");
        }
        attrs.insert("observation.level".into(), json_str("WARNING"));
        attrs.insert("observation.status_message".into(), json_str(&message));
        attrs.insert("error.message".into(), json_str(&message));
        self.spans[root].status = SpanStatus::Unset;
    }

    /// Adds host-owned provider and pricing metadata to the generation
    /// identified by its native run and call IDs.
    ///
    /// This intentionally does not infer values from the model name. It lets a
    /// host enrich a generation after the native observations have been folded
    /// without implementing a second event state machine. Native provider
    /// charges and costs recorded by the harness remain authoritative over
    /// the optional host estimate; a new estimate is rolled up exactly once.
    pub fn enrich_generation(
        &mut self,
        run_id: &RunId,
        call_id: &CallId,
        enrichment: GenerationEnrichment,
    ) -> Result<(), GenerationEnrichmentError> {
        let key = format!("{}\0{}", run_id.as_str(), call_id.as_str());
        let index = self
            .generation_by_call
            .get(&key)
            .copied()
            .ok_or(GenerationEnrichmentError::GenerationNotFound)?;
        let pricing_values = enrichment.pricing.iter().flat_map(|pricing| {
            [
                (
                    "gen_ai.pricing.input_per_mtok_usd",
                    pricing.input_per_mtok_usd,
                ),
                (
                    "gen_ai.pricing.cached_input_per_mtok_usd",
                    pricing.cached_input_per_mtok_usd,
                ),
                (
                    "gen_ai.pricing.output_per_mtok_usd",
                    pricing.output_per_mtok_usd,
                ),
            ]
        });
        if pricing_values
            .clone()
            .any(|(_, value)| value.is_some_and(|v| !v.is_finite() || v < 0.0))
            || enrichment
                .estimated_cost_usd
                .is_some_and(|v| !v.is_finite() || v < 0.0)
        {
            return Err(GenerationEnrichmentError::InvalidPricing);
        }
        if let Some(provider) = enrichment.provider_id {
            self.spans[index]
                .attributes
                .insert("gen_ai.provider".into(), json_str(&provider));
        }
        for (key, value) in pricing_values {
            if let Some(value) = value {
                self.spans[index]
                    .attributes
                    .insert(key.into(), json_f64(value));
            }
        }
        let cached = self.spans[index]
            .attributes
            .get("gen_ai.cache.hit")
            .and_then(Value::as_bool)
            == Some(true);
        if let Some(estimate) = enrichment.estimated_cost_usd.filter(|_| !cached) {
            let has_authoritative_cost = self.spans[index]
                .attributes
                .contains_key("gen_ai.usage.cost_usd");
            if !has_authoritative_cost {
                self.set_generation_cost(index, estimate, "estimated");
            }
        }
        Ok(())
    }

    fn finalize_run(&mut self, run_id: &RunId, at: u64) {
        let run_key = run_id.as_str();
        let prefix = format!("{run_key}\0");

        let open_model_keys: Vec<_> = self
            .open_models
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in open_model_keys {
            if let Some(call) = self.open_models.remove(&key) {
                self.force_close_span(call.index, at);
            }
        }

        let open_tool_keys: Vec<_> = self
            .open_tools
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in open_tool_keys {
            if let Some(index) = self.open_tools.remove(&key) {
                self.force_close_span(index, at);
            }
        }

        if let Some(index) = self.open_iterations.remove(run_key) {
            self.force_close_span(index, at);
        }
        self.pending_generation_enrichment
            .retain(|key, _| !key.starts_with(&prefix));

        if let Some(root) = self.run_roots.get(run_key).copied() {
            if let Some(end) = self.spans[root].end_unix_ms {
                self.spans[root].end_unix_ms = Some(end.max(at));
            } else {
                self.force_close_span(root, at);
            }
        }
    }

    fn set_generation_cost(&mut self, index: usize, amount: f64, source: &str) {
        let prior = self.spans[index]
            .attributes
            .get("gen_ai.usage.cost_usd")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let prior_source = self.spans[index]
            .attributes
            .get("gen_ai.cost.source")
            .and_then(Value::as_str)
            .unwrap_or("unreported")
            .to_owned();
        self.spans[index]
            .attributes
            .insert("gen_ai.usage.cost_usd".into(), json_f64(amount));
        self.spans[index]
            .attributes
            .insert("gen_ai.cost.source".into(), json_str(source));
        let delta = match source {
            // UsageRecorded carries provider charges to the run span. Remove
            // an earlier estimate here; the native per-run event will add
            // the authoritative charge once it arrives.
            "provider_reported" if prior_source == "estimated" => -prior,
            "provider_reported" => 0.0,
            _ => {
                amount
                    - if prior_source == "estimated" {
                        prior
                    } else {
                        0.0
                    }
            }
        };
        let direct_parent = self.spans[index]
            .parent_span_id
            .as_deref()
            .and_then(|id| self.spans.iter().position(|span| span.span_id == id));
        let mut run_root = direct_parent;
        while let Some(parent) = run_root {
            if matches!(self.spans[parent].kind, SpanKind::Turn | SpanKind::Subagent) {
                break;
            }
            run_root = self.spans[parent]
                .parent_span_id
                .as_deref()
                .and_then(|id| self.spans.iter().position(|span| span.span_id == id));
        }

        if source == "provider_reported" {
            if let Some(parent) = direct_parent
                && matches!(
                    self.spans[parent].kind,
                    SpanKind::Iteration | SpanKind::SubagentIteration
                )
            {
                let iteration_delta = amount
                    - if prior_source == "estimated" {
                        prior
                    } else {
                        0.0
                    };
                self.add_cost_delta(parent, iteration_delta, source);
            }
            // Run-level provider totals arrive through UsageRecorded. Remove
            // an earlier estimate here, but do not add this per-call charge to
            // the run root a second time.
            if prior_source == "estimated"
                && let Some(root) = run_root
            {
                self.add_cost_delta(root, -prior, source);
            }
        } else {
            if let Some(parent) = direct_parent {
                self.add_cost_delta(parent, delta, source);
            }
            if let Some(root) = run_root
                && Some(root) != direct_parent
            {
                self.add_cost_delta(root, delta, source);
            }
        }
    }

    fn add_cost_delta(&mut self, index: usize, delta: f64, source: &str) {
        let prior = self.spans[index]
            .attributes
            .get("gen_ai.usage.cost_usd")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        self.spans[index].attributes.insert(
            "gen_ai.usage.cost_usd".into(),
            json_f64((prior + delta).max(0.0)),
        );
        self.spans[index]
            .attributes
            .insert("gen_ai.cost.source".into(), json_str(source));
    }

    /// Closes open spans deterministically. Missing terminal events are marked
    /// as force-closed warnings so their durations are not mistaken for measured latency.
    pub fn finish(&mut self, timestamp_ms: u64) {
        if self.finished {
            return;
        }
        let end = timestamp_ms.max(self.last_activity_ms);
        for index in 0..self.spans.len() {
            if self.spans[index].end_unix_ms.is_none() {
                self.force_close_span(index, end);
            }
        }
        self.finished = true;
    }

    /// Returns the spans accumulated so far.
    pub fn spans(&self) -> &[TraceSpan] {
        &self.spans
    }

    /// Consumes the collector and returns its spans, sealing any open spans.
    pub fn into_spans(mut self, timestamp_ms: u64) -> Vec<TraceSpan> {
        self.finish(timestamp_ms);
        self.spans
    }

    fn finish_model(
        &mut self,
        call: CallStart,
        started_at_ms: Option<u64>,
        usage: Option<&tinyinference_llm::usage::Usage>,
        input: Option<&Value>,
        output: Option<&Value>,
        now: u64,
    ) {
        let start = started_at_ms.unwrap_or(call.started_at_ms).min(now);
        let provider_cost = usage
            .and_then(|usage| usage.charged_amount)
            .map(|charged| charged.micros as f64 / 1_000_000.0);
        {
            let span = &mut self.spans[call.index];
            span.start_unix_ms = start;
            span.status = SpanStatus::Ok;
            span.end_unix_ms = Some(now.max(start));
            if let Some(usage) = usage {
                let cached = usage
                    .cache_read_tokens
                    .saturating_add(usage.cache_creation_tokens);
                let input_total =
                    usage
                        .input_tokens
                        .max(cached)
                        .max(
                            usage
                                .input_tokens
                                .saturating_add(if usage.input_tokens < cached {
                                    cached
                                } else {
                                    0
                                }),
                        );
                for (key, value) in [
                    ("gen_ai.usage.input_tokens", json_u64(input_total)),
                    (
                        "gen_ai.usage.uncached_input_tokens",
                        json_u64(input_total.saturating_sub(cached)),
                    ),
                    (
                        "gen_ai.usage.cached_input_tokens",
                        json_u64(usage.cache_read_tokens),
                    ),
                    (
                        "gen_ai.usage.cache_creation_tokens",
                        json_u64(usage.cache_creation_tokens),
                    ),
                    ("gen_ai.usage.output_tokens", json_u64(usage.output_tokens)),
                    (
                        "gen_ai.usage.total_tokens",
                        json_u64(input_total.saturating_add(usage.output_tokens)),
                    ),
                    (
                        "gen_ai.usage.reasoning_tokens",
                        json_u64(usage.reasoning_tokens),
                    ),
                ] {
                    span.attributes.insert(key.into(), value);
                }
                if provider_cost.is_none() && !span.attributes.contains_key("gen_ai.usage.cost_usd")
                {
                    span.attributes
                        .insert("gen_ai.cost.source".into(), json_str("unreported"));
                }
            }
            if let Some(delta) = call.first_delta_ms {
                let unstreamed = now.saturating_sub(delta) <= UNSTREAMED_DELTA_WINDOW_MS
                    && delta.saturating_sub(start) > UNSTREAMED_DELTA_WINDOW_MS;
                if unstreamed {
                    span.attributes
                        .insert("gen_ai.response.streamed".into(), Value::Bool(false));
                } else {
                    let first = delta.max(start);
                    span.attributes.insert(
                        "gen_ai.response.first_token_unix_ms".into(),
                        json_u64(first),
                    );
                    span.attributes.insert(
                        "gen_ai.response.time_to_first_token_ms".into(),
                        json_u64(first.saturating_sub(start)),
                    );
                    if let Some(text) = call.first_text_ms {
                        span.attributes.insert(
                            "gen_ai.response.time_to_first_text_ms".into(),
                            json_u64(text.max(start).saturating_sub(start)),
                        );
                    }
                }
            }
            if self.context.capture_content {
                if let Some(input) = input {
                    self.spans[call.index].input = Some(capture_model_content(input));
                }
                if let Some(output) = output {
                    self.spans[call.index].output = Some(capture_model_content(output));
                }
            }
        }
        if let Some(cost) = provider_cost {
            self.set_generation_cost(call.index, cost, "provider_reported");
        }
    }

    fn ensure_root(&mut self, at: u64) -> usize {
        if let Some(index) = self.root {
            return index;
        }
        let mut attrs = BTreeMap::new();
        attrs.insert("session.id".into(), json_str(&self.context.session_id));
        attrs.insert("run.type".into(), json_str(self.context.run_type.as_str()));
        attrs.insert(
            "thread.id".into(),
            json_str(
                self.context
                    .session_group
                    .as_deref()
                    .unwrap_or(&self.context.session_id),
            ),
        );
        for (key, value) in [
            ("user.id", self.context.user_id.as_deref()),
            ("client.id", self.context.client_id.as_deref()),
            ("agent.id", self.context.agent_id.as_deref()),
            ("channel.source", self.context.channel_source.as_deref()),
            ("run.id", self.context.run_id.as_deref()),
            ("parent_run_id", self.context.parent_run_id.as_deref()),
            ("root_run_id", self.context.root_run_id.as_deref()),
        ] {
            if let Some(value) = value {
                attrs.insert(key.into(), json_str(value));
            }
        }
        let name = self
            .context
            .agent_id
            .as_deref()
            .map(|id| format!("agent.turn:{id}"))
            .unwrap_or_else(|| "agent.turn".into());
        let (index, _) = self.open_span(SpanKind::Turn, name, None, at, attrs);
        self.root = Some(index);
        index
    }

    fn open_subagent(&mut self, name: &str, depth: usize, at: u64) -> usize {
        let parent = self.active_parent(at);
        self.open_subagent_span(name, depth, at, parent)
    }

    fn open_subagent_span(&mut self, name: &str, depth: usize, at: u64, parent: usize) -> usize {
        let mut attrs = BTreeMap::new();
        attrs.insert("subagent.name".into(), json_str(name));
        attrs.insert("subagent.depth".into(), json!(depth));
        self.open_span(
            SpanKind::Subagent,
            format!("subagent.{name}"),
            Some(parent),
            at,
            attrs,
        )
        .0
    }

    fn active_parent(&mut self, at: u64) -> usize {
        self.open_iterations
            .get(&self.active_run_id())
            .copied()
            .unwrap_or_else(|| self.ensure_run_root(at))
    }

    fn active_run_id(&self) -> String {
        self.current_run_id.clone().unwrap_or_else(|| "root".into())
    }

    fn scoped_id(&self, id: &str) -> String {
        format!("{}\0{id}", self.active_run_id())
    }

    fn ensure_run_root(&mut self, at: u64) -> usize {
        let run_id = self.active_run_id();
        if let Some(index) = self.run_roots.get(&run_id).copied() {
            return index;
        }
        let index = self.ensure_root(at);
        self.run_roots.insert(run_id, index);
        index
    }

    fn open_span(
        &mut self,
        kind: SpanKind,
        name: String,
        parent: Option<usize>,
        at: u64,
        attributes: BTreeMap<String, Value>,
    ) -> (usize, String) {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let parent_id = parent.map(|idx| self.spans[idx].span_id.clone());
        let index = self.spans.len();
        self.spans.push(TraceSpan {
            trace_id: self.context.session_id.clone(),
            span_id: id.clone(),
            parent_span_id: parent_id,
            name,
            kind,
            start_unix_ms: at,
            end_unix_ms: None,
            status: SpanStatus::Unset,
            attributes,
            input: None,
            output: None,
        });
        (index, id)
    }

    fn close_span(&mut self, index: usize, at: u64, status: SpanStatus) {
        let Some(span) = self.spans.get(index) else {
            return;
        };
        let end = at.max(span.start_unix_ms);
        let parent_id = span.parent_span_id.clone();
        if let Some(span) = self.spans.get_mut(index) {
            let already_closed = span.end_unix_ms.is_some();
            span.end_unix_ms = Some(span.end_unix_ms.unwrap_or(end).max(end));
            if !already_closed {
                span.status = status;
            }
        }
        if let Some(parent) = parent_id
            .as_deref()
            .and_then(|id| self.spans.iter().position(|span| span.span_id == id))
            && let Some(parent_end) = self.spans[parent].end_unix_ms
            && end > parent_end
        {
            self.spans[parent].end_unix_ms = Some(end);
        }
    }

    fn force_close_span(&mut self, index: usize, at: u64) {
        if let Some(span) = self.spans.get_mut(index) {
            span.end_unix_ms = Some(at.max(span.start_unix_ms));
            span.status = SpanStatus::Unset;
            span.attributes
                .insert("force_closed".into(), Value::Bool(true));
            span.attributes
                .insert("observation.level".into(), json_str("WARNING"));
        }
    }

    fn close_root(&mut self, at: u64, status: SpanStatus, outcome: &str) {
        let root = self.ensure_run_root(at);
        let attrs = &mut self.spans[root].attributes;
        let replacing_typed_stop = attrs.contains_key("turn.stop_turn") && outcome != "stopped";
        attrs.insert("turn.outcome".into(), json_str(outcome));
        if replacing_typed_stop {
            attrs.remove("turn.stop_turn");
            attrs.remove("turn.stop_kind");
            attrs.remove("turn.stop_class");
            attrs.remove("turn.stop_operation");
            attrs.remove("turn.stop_limit");
            attrs.remove("observation.level");
            attrs.remove("observation.status_message");
        }
        if status == SpanStatus::Error {
            attrs.insert("error".into(), Value::Bool(true));
        }
        self.close_span(root, at, status);
        if replacing_typed_stop {
            self.spans[root].status = status;
        }
    }

    fn close_completed_run(&mut self, outcome: Option<&crate::terminal::TerminalOutcome>, at: u64) {
        self.close_terminal_outcome(outcome, None, at);
    }

    fn close_terminal_outcome(
        &mut self,
        outcome: Option<&crate::terminal::TerminalOutcome>,
        fallback_error: Option<&str>,
        at: u64,
    ) {
        use crate::terminal::TerminalReason;
        let Some(outcome) = outcome else {
            self.close_root(at, SpanStatus::Ok, "completed");
            return;
        };
        if outcome.reason.class() == crate::terminal::TerminalClass::Timeout {
            self.close_root(at, SpanStatus::Error, "timed_out");
            let root = self.ensure_run_root(at);
            self.put_error(
                root,
                terminal_error_message(&outcome.message, fallback_error, "Operation timed out"),
            );
            return;
        }
        match outcome.reason {
            TerminalReason::Completed => self.close_root(at, SpanStatus::Ok, "completed"),
            TerminalReason::Cancelled => {
                self.close_root(at, SpanStatus::Unset, "cancelled");
                if let Some(root) = self.run_roots.get(&self.active_run_id()).copied() {
                    self.spans[root]
                        .attributes
                        .insert("observation.level".into(), json_str("WARNING"));
                    self.put_error(
                        root,
                        terminal_error_message(&outcome.message, fallback_error, "Run cancelled"),
                    );
                }
            }
            TerminalReason::LimitReached(_)
            | TerminalReason::Halted
            | TerminalReason::Paused
            | TerminalReason::Deferred => {
                self.close_root(at, SpanStatus::Unset, "stopped");
                if let Some(root) = self.run_roots.get(&self.active_run_id()).copied() {
                    let kind = match outcome.reason {
                        TerminalReason::LimitReached(_) => "limit_reached",
                        TerminalReason::Paused => "paused",
                        TerminalReason::Halted => "halted",
                        _ => "deferred",
                    };
                    let status_message = format!("stopped: {kind}");
                    self.spans[root]
                        .attributes
                        .insert("observation.level".into(), json_str("WARNING"));
                    self.spans[root]
                        .attributes
                        .insert("turn.stop_kind".into(), json_str(kind));
                    if let TerminalReason::LimitReached(Some(limit)) = outcome.reason {
                        self.spans[root]
                            .attributes
                            .insert("turn.stop_limit".into(), json_str(limit.as_str()));
                    }
                    self.spans[root].attributes.insert(
                        "observation.status_message".into(),
                        json_str(&status_message),
                    );
                    self.spans[root]
                        .attributes
                        .insert("error.message".into(), json_str(&status_message));
                }
            }
            _ => {
                self.close_root(at, SpanStatus::Error, "failed");
                if let Some(root) = self.run_roots.get(&self.active_run_id()).copied() {
                    self.put_error(
                        root,
                        terminal_error_message(
                            &outcome.message,
                            fallback_error,
                            "Operation failed",
                        ),
                    );
                }
            }
        }
    }

    fn close_failed_run(
        &mut self,
        outcome: Option<&crate::terminal::TerminalOutcome>,
        error: &str,
        at: u64,
    ) {
        use crate::terminal::TerminalReason;
        let root = self.ensure_run_root(at);
        let prior_end = self.spans[root].end_unix_ms;

        // Reuse the generic terminal projection for typed cancellation, stops,
        // and failures. A RunFailed event is authoritative if it contradicts
        // a typed Completed outcome: never turn a failed run into success.
        if let Some(outcome) = outcome
            && outcome.reason != TerminalReason::Completed
        {
            self.close_terminal_outcome(Some(outcome), Some(error), at);
            self.override_closed_failed_run_status(root, prior_end, Some(outcome));
            return;
        }

        self.close_root(at, SpanStatus::Error, "failed");
        self.put_error(root, error);
        self.override_closed_failed_run_status(root, prior_end, outcome);
    }

    fn override_closed_failed_run_status(
        &mut self,
        root: usize,
        prior_end: Option<u64>,
        outcome: Option<&crate::terminal::TerminalOutcome>,
    ) {
        if prior_end.is_some() {
            self.spans[root].status = failed_iteration_status(outcome);
        }
    }

    fn put_error(&mut self, index: usize, error: &str) {
        let message = if self.context.capture_content {
            super::serialize::truncate_chars(error, MAX_ERROR_MESSAGE_CHARS)
        } else {
            "Operation failed".into()
        };
        self.spans[index]
            .attributes
            .insert("error.message".into(), json_str(&message));
    }
}

fn merge_generation_enrichment(
    current: &mut GenerationEnrichment,
    incoming: &GenerationEnrichment,
) {
    if incoming.provider_id.is_some() {
        current.provider_id.clone_from(&incoming.provider_id);
    }
    if let Some(incoming_pricing) = &incoming.pricing {
        let pricing = current
            .pricing
            .get_or_insert_with(GenerationPricing::default);
        if incoming_pricing.input_per_mtok_usd.is_some() {
            pricing
                .input_per_mtok_usd
                .clone_from(&incoming_pricing.input_per_mtok_usd);
        }
        if incoming_pricing.cached_input_per_mtok_usd.is_some() {
            pricing
                .cached_input_per_mtok_usd
                .clone_from(&incoming_pricing.cached_input_per_mtok_usd);
        }
        if incoming_pricing.output_per_mtok_usd.is_some() {
            pricing
                .output_per_mtok_usd
                .clone_from(&incoming_pricing.output_per_mtok_usd);
        }
    }
    if incoming.estimated_cost_usd.is_some() {
        current.estimated_cost_usd = incoming.estimated_cost_usd;
    }
}

fn terminal_error_message<'a>(
    outcome_message: &'a str,
    fallback_error: Option<&'a str>,
    fallback_message: &'a str,
) -> &'a str {
    if !outcome_message.trim().is_empty() {
        outcome_message
    } else {
        fallback_error
            .filter(|message| !message.trim().is_empty())
            .unwrap_or(fallback_message)
    }
}

fn terminal_iteration_status(outcome: Option<&crate::terminal::TerminalOutcome>) -> SpanStatus {
    use crate::terminal::TerminalClass;
    match outcome.map(|outcome| outcome.reason.class()) {
        None | Some(TerminalClass::Success) => SpanStatus::Ok,
        Some(TerminalClass::Cancellation | TerminalClass::Suspended) => SpanStatus::Unset,
        Some(TerminalClass::Timeout | TerminalClass::Failure) => SpanStatus::Error,
    }
}

fn failed_iteration_status(outcome: Option<&crate::terminal::TerminalOutcome>) -> SpanStatus {
    use crate::terminal::TerminalClass;
    match outcome.map(|outcome| outcome.reason.class()) {
        Some(TerminalClass::Cancellation | TerminalClass::Suspended) => SpanStatus::Unset,
        None | Some(TerminalClass::Success | TerminalClass::Timeout | TerminalClass::Failure) => {
            SpanStatus::Error
        }
    }
}

fn nonempty<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
