//! Atomic document outbox and explicit replay into the native event stream.
//!
//! Status and event receipts commit together. Stream projection is separate:
//! no post-commit projection failure is represented as a rolled-back status CAS.
use super::*;
use super::storage::{AGENTS, CAS_ATTEMPTS, EVENTS, EVENT_HEADS, encode, key};
use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{ErrorKind, Precondition, Versioned, WriteOp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct EventHead { pub(super) next: u64 }

struct EventCommit {
    event: RunEvent,
    writes: Vec<WriteOp>,
}

impl DriverRunLedger {
    async fn event_commit(&self, input: &RunEventAppend) -> Result<EventCommit> {
        let head = self.read::<EventHead>(EVENT_HEADS, &input.run_id).await?;
        let sequence = head.as_ref().map_or(1, |head| head.doc.next);
        let next = sequence.checked_add(1).filter(|value| *value <= i64::MAX as u64)
            .ok_or_else(|| StorageError::serialization("event sequence exceeds classic range"))?;
        let event = RunEvent { run_id: input.run_id.clone(), sequence, event_type: input.event_type.clone(),
            payload: input.payload.clone(), timestamp: Utc::now() };
        Ok(EventCommit {
            writes: vec![
                WriteOp::Put { collection: EVENT_HEADS.into(), id: key(&[&input.run_id]),
                    doc: encode(&EventHead { next })?, precondition: condition(&head) },
                WriteOp::Put { collection: EVENTS.into(), id: key(&[&input.run_id, &sequence.to_string()]),
                    doc: encode(&event)?, precondition: Precondition::Absent },
            ], event,
        })
    }

    pub(super) async fn commit_event(&self, input: RunEventAppend) -> Result<RunEvent> {
        for _ in 0..CAS_ATTEMPTS {
            let commit = self.event_commit(&input).await?;
            match self.scoped.documents().atomic_batch_checked(vec![], commit.writes).await {
                Ok(_) => return Ok(commit.event),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::conflict("event allocation contention budget exhausted").into())
    }

    pub(super) async fn transition_agent(&self, id: &str, expected: Option<AgentRunStatus>,
        status: AgentRunStatus, error: Option<&str>, completed_at: Option<DateTime<Utc>>,
        event: Option<RunEventAppend>) -> Result<RunTransition> {
        for _ in 0..CAS_ATTEMPTS {
            let Some(previous) = self.read::<AgentRun>(AGENTS, id).await? else { return Ok(RunTransition::NotFound); };
            if expected.is_some_and(|expected| previous.doc.status != expected) {
                return Ok(RunTransition::StatusChanged(self.agent_telemetry(previous.doc).await?));
            }
            let condition = Precondition::Version(previous.version);
            let mut row = previous.doc;
            row.status = status; row.error = error.map(str::to_owned); row.completed_at = completed_at;
            row.updated_at = Utc::now(); row.telemetry = None;
            let mut writes = vec![WriteOp::Put { collection: AGENTS.into(), id: key(&[id]),
                doc: encode(&row)?, precondition: condition }];
            if let Some(input) = &event { writes.extend(self.event_commit(input).await?.writes); }
            match self.scoped.documents().atomic_batch_checked(vec![], writes).await {
                Ok(_) => return Ok(RunTransition::Applied(self.agent_telemetry(row).await?)),
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::conflict("agent transition contention budget exhausted").into())
    }

    pub(super) async fn events(&self, request: &RunEventListRequest) -> Result<RunEventListResponse> {
        let limit = request.limit.unwrap_or(100).min(1000) as usize;
        let after = request.after_sequence.unwrap_or(0);
        let filter = Filter::eq("record.runId", request.run_id.clone()).and(Filter::gt("record.sequence", after));
        let mut events: Vec<RunEvent> = self.records(EVENTS, Query::filter(filter).sort(Sort::asc("record.sequence"))).await?;
        events.truncate(limit);
        Ok(RunEventListResponse { count: events.len(), events })
    }

    async fn validate_projection(&self, run_id: &str, stream: &str, next: u64) -> Result<u64> {
        let mut offset = 0;
        let mut after = 0;
        while offset < next {
            let limit = usize::try_from((next - offset).min(256)).expect("bounded projection page");
            let entries = self.scoped.streams().read_window(stream, offset, limit).await?;
            if entries.len() != limit { return Err(StorageError::serialization("event projection was truncated").into()); }
            for entry in entries {
                if entry.offset != offset { return Err(StorageError::serialization("event projection offset has a gap").into()); }
                let event: RunEvent = serde_json::from_value(entry.value).map_err(StorageError::from)?;
                if event.run_id != run_id || event.sequence <= after {
                    return Err(StorageError::serialization("event projection identity or order differs").into());
                }
                // Event addresses include run and sequence separately to avoid ambiguous ids.
                let native = self.scoped.documents().get(EVENTS, &key(&[run_id, &event.sequence.to_string()])).await?;
                let expected = native.map(|row| super::storage::decode::<RunEvent>(row.doc)).transpose()?;
                if expected.as_ref() != Some(&event) { return Err(StorageError::serialization("event projection differs from committed receipt").into()); }
                offset += 1; after = event.sequence;
            }
        }
        Ok(after)
    }

    /// Replay committed event receipts into their native stream, without duplicate appends.
    ///
    /// Document receipts remain authoritative for atomic status/event visibility.
    /// Hosts may retry projection after a crash or native refusal. This operation
    /// never changes status, allocates a sequence, or selects a different backend.
    ///
    /// # Errors
    /// Retains native stream refusal and conditional-append capability errors;
    /// a conflicting or truncated projection is refused rather than overwritten.
    pub async fn reconcile_run_events(&self, run_id: &str) -> Result<u64> {
        self.declare().await?;
        let stream = format!("run_events:{}", key(&[run_id]));
        for _ in 0..CAS_ATTEMPTS {
            let next = self.scoped.streams().len(&stream).await?;
            let after = self.validate_projection(run_id, &stream, next).await?;
            let records = self.events(&RunEventListRequest { run_id: run_id.into(),
                after_sequence: Some(after), limit: Some(1000) }).await?.events;
            if records.is_empty() { return Ok(next); }
            let values = records.into_iter().map(serde_json::to_value).collect::<std::result::Result<Vec<_>, _>>().map_err(StorageError::from)?;
            match self.scoped.streams().append_batch_if_offset(&stream, next, values).await {
                Ok(_) => continue,
                Err(error) if error.kind() == ErrorKind::Conflict => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(StorageError::conflict("event projection contention budget exhausted").into())
    }
}

fn condition<T>(previous: &Option<Versioned<T>>) -> Precondition {
    previous.as_ref().map_or(Precondition::Absent, |previous| Precondition::Version(previous.version))
}
