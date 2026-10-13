use super::*;
use crate::summarization::CompactionReason;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::span;

/// Counts info-level events carrying the compaction log message.
struct Counter(Arc<AtomicUsize>);

impl tracing::Subscriber for Counter {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::INFO {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

fn compacted() -> AgentEvent {
    AgentEvent::Compacted {
        reason: CompactionReason::Manual,
        tokens_before: 10,
        tokens_after: 20,
        usage: None,
        latency_ms: Some(5),
    }
}

#[test]
fn compacted_event_logs_once_at_info_even_when_tokens_grew() {
    let count = Arc::new(AtomicUsize::new(0));
    tracing::subscriber::with_default(Counter(count.clone()), || log_compacted(&compacted()));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn other_events_are_not_logged() {
    let count = Arc::new(AtomicUsize::new(0));
    tracing::subscriber::with_default(Counter(count.clone()), || {
        log_compacted(&AgentEvent::MemoryLoaded)
    });
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
