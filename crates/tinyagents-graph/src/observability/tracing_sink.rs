//! [`GraphTracingSink`] — mirrors the graph executor's lifecycle
//! stream onto `tracing` diagnostics.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::stream::{GraphEvent, GraphEventEnvelope, GraphEventSink};

/// A [`GraphEventSink`] that mirrors the graph executor's lifecycle
/// stream onto `tracing` diagnostics — an observability journal for
/// graph runs. Node/step/run/route transitions land as
/// grep-friendly `[graph]` lines tagged with `label`; the running event count is
/// exposed for tests. Hosts share one per graph family (fan-out,
/// delegation, …).
pub struct GraphTracingSink {
    label: String,
    count: Arc<std::sync::atomic::AtomicUsize>,
}

impl GraphTracingSink {
    /// Build a sink tagging its lines with `label` (e.g. `"delegation:graph"`).
    /// Accepts both string literals and runtime-built labels.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Shared counter of events observed.
    pub fn counter(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.count.clone()
    }
}

impl GraphEventSink for GraphTracingSink {
    fn emit(&self, envelope: GraphEventEnvelope) {
        let event = envelope.event;
        self.count.fetch_add(1, Ordering::Relaxed);
        let label = self.label.as_str();
        match &event {
            GraphEvent::RunStarted { run_id } => {
                tracing::debug!(label, ?run_id, "[graph] run started")
            }
            GraphEvent::RunCompleted { steps, .. } => {
                tracing::debug!(label, steps, "[graph] run completed")
            }
            GraphEvent::RunFailed { error, .. } => {
                tracing::warn!(label, %error, "[graph] run failed")
            }
            GraphEvent::NodeStarted { node, step } => {
                tracing::debug!(label, ?node, step, "[graph] node started")
            }
            GraphEvent::NodeCompleted { node, step } => {
                tracing::debug!(label, ?node, step, "[graph] node completed")
            }
            GraphEvent::NodeFailed { node, error, .. } => {
                tracing::warn!(label, ?node, %error, "[graph] node failed")
            }
            GraphEvent::RouteSelected { node, target } => {
                tracing::trace!(label, ?node, ?target, "[graph] route selected")
            }
            _ => tracing::trace!(label, "[graph] event"),
        }
    }
}

#[cfg(test)]
#[path = "tracing_sink_tests.rs"]
mod tests;
