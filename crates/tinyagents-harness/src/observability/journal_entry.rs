//! The stored form of a journal entry.
//!
//! [`AgentObservation`] is the replay / RPC wire type and repeats the run's
//! identity in every row. The journal stream is already named by the run
//! (`harness.journal/<run id>`), so a stored entry leaves out what the stream
//! name gives back: the run id, and the event id `{run id}-evt-{offset}` that
//! the emitting sink minted. A reader rebuilds the wire type with
//! [`JournalEntry::into_observation`], byte-identical to what was appended.
//!
//! Two values cannot always be derived and are kept when they differ from the
//! derivation, so nothing is ever lost: the `root_run_id` of a child run
//! (equal to the run id for a top-level run), and an `event_id` minted by a
//! sink with another stream id (a child's events numbered by its root's sink,
//! or a sink with the default `s<n>` stream id).

use serde::{Deserialize, Serialize};

use super::AgentObservation;
use crate::events::{AgentEvent, canonical_event_id};
use crate::ids::{EventId, RunId};

/// One journal row as stored: an [`AgentObservation`] without the fields its
/// stream name and position repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct JournalEntry {
    /// Parent run when the run was spawned by another run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_run_id: Option<RunId>,
    /// The root run, only for a run that is not its own root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_run_id: Option<RunId>,
    /// The event id, only when it is not `{run id}-evt-{offset}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    /// The emitting sink's event counter (the `offset` of the observation),
    /// which can run ahead of the stream position when events were dropped.
    offset: u64,
    /// Wall-clock time the observation was created, in Unix-epoch milliseconds.
    ts_ms: u64,
    /// The typed event payload.
    event: AgentEvent,
}

impl JournalEntry {
    /// The stored form of `obs`, appended to the stream of `obs.run_id`.
    pub(crate) fn from_observation(obs: &AgentObservation) -> Self {
        let run = obs.run_id.as_str();
        Self {
            parent_run_id: obs.parent_run_id.clone(),
            root_run_id: (obs.root_run_id != obs.run_id).then(|| obs.root_run_id.clone()),
            event_id: (obs.event_id != canonical_event_id(run, obs.offset))
                .then(|| obs.event_id.clone()),
            offset: obs.offset,
            ts_ms: obs.ts_ms,
            event: obs.event.clone(),
        }
    }

    /// The wire observation of the run whose journal stream held this entry.
    pub(crate) fn into_observation(self, run_id: &str) -> AgentObservation {
        let run = RunId::new(run_id);
        AgentObservation {
            event_id: self
                .event_id
                .unwrap_or_else(|| canonical_event_id(run_id, self.offset)),
            root_run_id: self.root_run_id.unwrap_or_else(|| run.clone()),
            run_id: run,
            parent_run_id: self.parent_run_id,
            offset: self.offset,
            ts_ms: self.ts_ms,
            event: self.event,
        }
    }
}

#[cfg(test)]
#[path = "journal_entry_tests.rs"]
mod tests;
