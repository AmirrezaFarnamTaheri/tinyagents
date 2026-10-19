//! Host-neutral span records and Langfuse exporters.
//!
//! Hosts translate their own run, step, and span observations into [`Span`]
//! records. This module owns the common Langfuse envelope and OTLP conversion;
//! it does not collect events, choose capture policy, or own a transport.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::Result;
use crate::observability::trace_export::ingestion_otlp::ingestion_batch_otlp_requests;
use crate::observability::{LangfuseClient, LangfuseTraceConfig, clean_nulls, iso_ms};

/// Completion state for a span-like observation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanState {
    /// The observation has no explicit completion result.
    #[default]
    Unset,
    /// The observation completed successfully.
    Ok,
    /// The observation completed with an error.
    Error,
}

/// One host-mapped run, step, span, generation, or point event.
///
/// `span_id` is the stable observation identity used for parent links. A host
/// may supply a separate `event_id` for Langfuse's ingestion-envelope id.
/// `run_id` and `step_id` are correlation coordinates; the exporter preserves
/// them in metadata without interpreting host-specific values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    /// Stable span or observation identity.
    pub span_id: String,
    /// Stable run identity that produced this observation.
    pub run_id: String,
    /// Optional host-defined step identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    /// Optional parent observation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<String>,
    /// Optional distinct event id for the ingestion envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// Human-readable operation name.
    pub name: String,
    /// Langfuse ingestion type (`span-create`, `event-create`, or `generation-create`).
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Start time in Unix epoch milliseconds.
    pub start_ms: u64,
    /// End time in Unix epoch milliseconds; absent while still running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<u64>,
    /// Completion state.
    #[serde(default)]
    pub status: SpanState,
    /// Optional status detail, commonly an error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// Optional input payload, subject to the host's capture policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// Optional output payload, subject to the host's capture policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Optional model token/cost usage details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    /// Additional host-owned observation metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

fn default_kind() -> String {
    "span-create".to_string()
}

impl Span {
    /// Creates a timed span with an explicit stable id and parent.
    pub fn new(
        span_id: impl Into<String>,
        run_id: impl Into<String>,
        name: impl Into<String>,
        start_ms: u64,
    ) -> Self {
        Self {
            span_id: span_id.into(),
            run_id: run_id.into(),
            step_id: None,
            parent_span_id: None,
            event_id: None,
            name: name.into(),
            kind: default_kind(),
            start_ms,
            end_ms: None,
            status: SpanState::Unset,
            status_message: None,
            input: None,
            output: None,
            usage: None,
            metadata: BTreeMap::new(),
        }
    }

    /// Sets the step coordinate.
    pub fn with_step_id(mut self, step_id: impl Into<String>) -> Self {
        self.step_id = Some(step_id.into());
        self
    }

    /// Sets the parent span identity.
    pub fn with_parent(mut self, parent_span_id: impl Into<String>) -> Self {
        self.parent_span_id = Some(parent_span_id.into());
        self
    }

    /// Sets the event id used by the Langfuse ingestion envelope.
    pub fn with_event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    /// Sets the end time and completion state.
    pub fn finish(mut self, end_ms: u64, status: SpanState) -> Self {
        self.end_ms = Some(end_ms);
        self.status = status;
        self
    }

    /// Sets the input and output payloads.
    pub fn with_io(mut self, input: Option<Value>, output: Option<Value>) -> Self {
        self.input = input;
        self.output = output;
        self
    }

    /// Sets model usage details.
    pub fn with_usage(mut self, usage: Value) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Sets an explanatory status message, usually for a failed observation.
    pub fn with_status_message(mut self, message: impl Into<String>) -> Self {
        self.status_message = Some(message.into());
        self
    }
}

/// Builds a Langfuse ingestion payload from host-mapped spans.
///
/// The first span's `run_id` supplies the default trace id. A trace-create
/// record is followed by one observation per span, preserving input order.
/// Empty input is rejected because a trace cannot be identified safely.
pub fn build_ingestion_batch(trace: LangfuseTraceConfig, spans: &[Span]) -> Result<Value> {
    let first = spans.first().ok_or_else(|| {
        crate::error::TinyAgentsError::Validation("at least one span is required".to_string())
    })?;
    let trace_id = trace
        .trace_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .unwrap_or(&first.run_id);
    let trace_ts = iso_ms(first.start_ms);
    let trace_event = json!({
        "id": format!("{trace_id}:trace"),
        "timestamp": trace_ts,
        "type": "trace-create",
        "body": clean_nulls(json!({
            "id": trace_id,
            "timestamp": trace_ts,
            "name": trace.name,
            "userId": trace.user_id,
            "sessionId": trace.session_id,
            "environment": trace.environment,
            "release": trace.release,
            "version": trace.version,
            "tags": if trace.tags.is_empty() { Value::Null } else { json!(trace.tags) },
            "metadata": trace.metadata,
        })),
    });
    let mut batch = Vec::with_capacity(spans.len() + 1);
    batch.push(trace_event);
    for span in spans {
        let start = iso_ms(span.start_ms);
        let end = span.end_ms.map(iso_ms);
        let event_id = span.event_id.as_deref().unwrap_or(&span.span_id);
        let (level, status_message) = match span.status {
            SpanState::Error => (Some("ERROR"), span.status_message.as_deref()),
            SpanState::Unset | SpanState::Ok => (None, None),
        };
        let mut metadata = span.metadata.clone();
        metadata.insert("run_id".to_string(), json!(span.run_id));
        if let Some(step_id) = &span.step_id {
            metadata.insert("step_id".to_string(), json!(step_id));
        }
        let body = clean_nulls(json!({
            "id": span.span_id,
            "traceId": trace_id,
            "parentObservationId": span.parent_span_id,
            "name": span.name,
            "startTime": start,
            "endTime": end,
            "level": level,
            "statusMessage": status_message,
            "input": span.input,
            "output": span.output,
            "usageDetails": span.usage,
            "metadata": metadata,
        }));
        batch.push(json!({
            "id": event_id,
            "timestamp": end.as_deref().unwrap_or(&start),
            "type": span.kind,
            "body": body,
        }));
    }
    Ok(json!({ "batch": batch }))
}

/// Langfuse exporter over the shared client; host identity and capture policy
/// remain with the caller that builds [`Span`] values.
#[derive(Clone, Debug)]
pub struct SpanExporter {
    client: LangfuseClient,
}

impl SpanExporter {
    /// Wraps the shared Langfuse transport client.
    pub fn new(client: LangfuseClient) -> Self {
        Self { client }
    }

    /// Returns the underlying client.
    pub fn client(&self) -> &LangfuseClient {
        &self.client
    }

    /// Builds a legacy Langfuse ingestion batch without sending it.
    pub fn build_ingestion_batch(
        &self,
        trace: LangfuseTraceConfig,
        spans: &[Span],
    ) -> Result<Value> {
        build_ingestion_batch(trace, spans)
    }

    /// Builds bounded Langfuse v4 OTLP requests without sending them.
    pub fn build_otlp_requests(
        &self,
        trace: LangfuseTraceConfig,
        spans: &[Span],
        service: &str,
    ) -> Result<Vec<Value>> {
        let payload = build_ingestion_batch(trace, spans)?;
        ingestion_batch_otlp_requests(&payload, service, &format!("{service}.spans"))
    }

    /// Sends spans through Langfuse v4 OTLP, stopping on the first failure.
    pub async fn send_otlp(
        &self,
        trace: LangfuseTraceConfig,
        spans: &[Span],
        service: &str,
    ) -> Result<Vec<Value>> {
        let mut responses = Vec::new();
        for request in self.build_otlp_requests(trace, spans, service)? {
            responses.push(self.client.send_otlp(request).await?);
        }
        Ok(responses)
    }

    /// Sends spans through the legacy Langfuse ingestion API.
    pub async fn send(&self, trace: LangfuseTraceConfig, spans: &[Span]) -> Result<Value> {
        self.client
            .send_batch(build_ingestion_batch(trace, spans)?)
            .await
    }
}

#[cfg(test)]
#[path = "spans_tests.rs"]
mod tests;
