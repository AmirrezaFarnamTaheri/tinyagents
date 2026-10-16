//! Event-driven span collection for a single harness run.
//!
//! The collector consumes durable `AgentObservation`s directly. It owns only
//! generic event-to-span projection; product identity and content policy are
//! supplied in [`TraceContext`].

use std::collections::{BTreeMap, HashMap};

use serde_json::{Value, json};

use crate::events::AgentEvent;
use crate::observability::AgentObservation;

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
    parent_index: usize,
    first_delta_ms: Option<u64>,
    first_text_ms: Option<u64>,
}

/// Folds native harness observations into a trace span tree.
///
/// Create one collector per run, call [`record`](Self::record) in observation
/// order, then call [`finish`](Self::finish) when the stream closes. Duplicate
/// starts replace the abandoned call; terminal events without a matching
/// start are ignored, which makes replay of partial journals deterministic.
#[derive(Debug)]
pub struct SpanCollector {
    context: TraceContext,
    spans: Vec<TraceSpan>,
    root: Option<usize>,
    open_models: HashMap<String, CallStart>,
    open_tools: HashMap<String, usize>,
    open_subagents: HashMap<String, usize>,
    last_activity_ms: u64,
    first_input_recorded: bool,
    finished: bool,
    last_offset: Option<u64>,
}

impl SpanCollector {
    /// Creates an empty collector for `context`.
    pub fn new(context: TraceContext) -> Self {
        Self {
            context,
            spans: Vec::new(),
            root: None,
            open_models: HashMap::new(),
            open_tools: HashMap::new(),
            open_subagents: HashMap::new(),
            last_activity_ms: 0,
            first_input_recorded: false,
            finished: false,
            last_offset: None,
        }
    }

    /// Folds one durable observation using its recorded timestamp.
    pub fn record(&mut self, observation: &AgentObservation) {
        if self
            .last_offset
            .is_some_and(|offset| observation.offset <= offset)
        {
            return;
        }
        self.last_offset = Some(observation.offset);
        self.record_event(&observation.event, observation.ts_ms);
    }

    /// Folds a live event using the caller's observation timestamp.
    pub fn record_event(&mut self, event: &AgentEvent, timestamp_ms: u64) {
        if self.finished {
            return;
        }
        self.last_activity_ms = self.last_activity_ms.max(timestamp_ms);
        match event {
            AgentEvent::RunStarted { .. } => {
                self.ensure_root(timestamp_ms);
            }
            AgentEvent::ModelStarted { call_id, model } => {
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
                self.open_models.insert(
                    call_id.as_str().to_owned(),
                    CallStart {
                        index,
                        parent_index: parent,
                        started_at_ms: timestamp_ms,
                        first_delta_ms: None,
                        first_text_ms: None,
                    },
                );
            }
            AgentEvent::ModelDelta { call_id, delta, .. } => {
                if let Some(call) = self.open_models.get_mut(call_id.as_str()) {
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
                let call = self
                    .open_models
                    .remove(call_id.as_str())
                    .unwrap_or_else(|| {
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
                            parent_index: parent,
                            started_at_ms: started_at_ms.unwrap_or(timestamp_ms),
                            first_delta_ms: None,
                            first_text_ms: None,
                        }
                    });
                self.finish_model(
                    call,
                    *started_at_ms,
                    usage.as_ref(),
                    input.as_ref(),
                    output.as_ref(),
                    timestamp_ms,
                );
            }
            AgentEvent::ModelFailed {
                call_id,
                started_at_ms,
                error,
                ..
            } => {
                if let Some(call) = self.open_models.remove(call_id.as_str()) {
                    let start = started_at_ms
                        .unwrap_or(call.started_at_ms)
                        .min(timestamp_ms);
                    self.close_span(call.index, timestamp_ms, SpanStatus::Error);
                    self.put_error(call.index, error);
                    self.spans[call.index].start_unix_ms = start;
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
                    .and_then(|id| self.open_tools.get(id.as_str()).copied())
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
                self.open_tools.insert(call_id.as_str().to_owned(), index);
            }
            AgentEvent::ToolCompleted {
                call_id,
                input,
                output,
                duration_ms,
                error,
                ..
            } => {
                if let Some(index) = self.open_tools.remove(call_id.as_str()) {
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
                duration_ms,
                error,
                ..
            } => {
                if let Some(index) = self.open_tools.remove(call_id.as_str()) {
                    let end = duration_ms
                        .map(|duration| self.spans[index].start_unix_ms.saturating_add(duration))
                        .unwrap_or(timestamp_ms);
                    self.close_span(index, end, SpanStatus::Error);
                    self.put_error(index, error);
                }
            }
            AgentEvent::SubAgentStarted { name, depth } => {
                let parent = self.active_parent(timestamp_ms);
                let mut attrs = BTreeMap::new();
                attrs.insert("subagent.name".into(), json_str(name));
                attrs.insert("subagent.depth".into(), json!(*depth));
                let (index, _) = self.open_span(
                    SpanKind::Subagent,
                    format!("subagent.{name}"),
                    Some(parent),
                    timestamp_ms,
                    attrs,
                );
                self.open_subagents.insert(name.clone(), index);
            }
            AgentEvent::SubAgentCompleted { name, .. } => {
                if let Some(index) = self.open_subagents.remove(name) {
                    self.close_span(index, timestamp_ms, SpanStatus::Ok);
                }
            }
            AgentEvent::SubAgentFailed { name, error, .. } => {
                if let Some(index) = self.open_subagents.remove(name) {
                    self.close_span(index, timestamp_ms, SpanStatus::Error);
                    self.put_error(index, error);
                }
            }
            AgentEvent::MessageAppended { role, message, .. } if self.context.capture_content => {
                if role == "user" && !self.first_input_recorded {
                    if let Some(message) = message {
                        if let Some(root) = self.root {
                            self.spans[root].input = Some(capture_model_content(message));
                            self.first_input_recorded = true;
                        }
                    }
                } else if role == "assistant"
                    && let (Some(root), Some(message)) = (self.root, message)
                {
                    self.spans[root].output = Some(capture_model_content(message));
                }
            }
            AgentEvent::RunCompleted { .. } => {
                self.close_root(timestamp_ms, SpanStatus::Ok, "completed")
            }
            AgentEvent::RunFailed { error, .. } => {
                self.close_root(timestamp_ms, SpanStatus::Error, "failed");
                if let Some(root) = self.root {
                    self.put_error(root, error);
                }
            }
            _ => {}
        }
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
                self.spans[index].end_unix_ms = Some(end.max(self.spans[index].start_unix_ms));
                self.spans[index].status = SpanStatus::Unset;
                self.spans[index]
                    .attributes
                    .insert("force_closed".into(), Value::Bool(true));
                self.spans[index]
                    .attributes
                    .insert("observation.level".into(), json_str("WARNING"));
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
        let usage_summary = usage.map(|usage| {
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
            (
                input_total,
                usage.cache_read_tokens,
                usage.cache_creation_tokens,
                usage.output_tokens,
                usage.reasoning_tokens,
                usage.charged_amount.map(|c| c.micros as f64 / 1_000_000.0),
            )
        });
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
                let cost = usage.charged_amount.map(|c| c.micros as f64 / 1_000_000.0);
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
                span.attributes.insert(
                    "gen_ai.cost.source".into(),
                    json_str(if cost.is_some() {
                        "provider_reported"
                    } else {
                        "unreported"
                    }),
                );
                if let Some(cost) = cost {
                    span.attributes
                        .insert("gen_ai.usage.cost".into(), json_f64(cost));
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
        if self.spans[call.parent_index].kind == SpanKind::Subagent
            && let Some((input, cached, cache_creation, output, reasoning, cost)) = usage_summary
        {
            let parent = &mut self.spans[call.parent_index];
            for (key, add) in [
                ("gen_ai.usage.input_tokens", input),
                ("gen_ai.usage.output_tokens", output),
                ("gen_ai.usage.cached_input_tokens", cached),
                ("gen_ai.usage.cache_creation_tokens", cache_creation),
                ("gen_ai.usage.reasoning_tokens", reasoning),
            ] {
                let prior = parent
                    .attributes
                    .get(key)
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                parent
                    .attributes
                    .insert(key.into(), json_u64(prior.saturating_add(add)));
            }
            if let Some(cost) = cost {
                let prior = parent
                    .attributes
                    .get("gen_ai.usage.cost")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                parent
                    .attributes
                    .insert("gen_ai.usage.cost".into(), json_f64(prior + cost));
                parent
                    .attributes
                    .insert("gen_ai.cost.source".into(), json_str("provider_reported"));
            }
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

    fn active_parent(&mut self, at: u64) -> usize {
        self.open_subagents
            .values()
            .next()
            .copied()
            .unwrap_or_else(|| self.ensure_root(at))
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
        if let Some(span) = self.spans.get_mut(index) {
            span.end_unix_ms = Some(at.max(span.start_unix_ms));
            span.status = status;
        }
    }

    fn close_root(&mut self, at: u64, status: SpanStatus, outcome: &str) {
        let root = self.ensure_root(at);
        self.spans[root]
            .attributes
            .insert("turn.outcome".into(), json_str(outcome));
        if status == SpanStatus::Error {
            self.spans[root]
                .attributes
                .insert("error".into(), Value::Bool(true));
        }
        self.close_span(root, at, status);
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
