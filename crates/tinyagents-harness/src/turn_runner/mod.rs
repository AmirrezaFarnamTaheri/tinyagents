//! Channel-independent turn lifecycle policy for hosts.
//!
//! [`TurnRunner`] owns ingress ordering, per-scope session serialization,
//! cancellation generations, deadline/idle handling, reply persistence, and
//! exactly-once terminal delivery. Hosts provide the side effects through the
//! ports in this module; channel protocols and their wire events stay host
//! owned.

mod types;

pub use types::{
    deterministic_reply_id, EventSink, FailureClassifier, IngressDecision, IngressPolicy,
    IngressStage, QueueDisposition, ReplyStore, ScopeKey, Summarizer, TerminalEvent,
    TurnHost, TurnRequest, TurnResult, TurnRunner, TurnRunnerConfig, TurnRunnerError,
};
