# Run progress ledger

`run_progress_ledger` records the progress stream for one foreground agent
turn in the host-installed `RunLedgerStore`. It keeps the durable run status
and event history aligned with the turn lifecycle, and carries subagent
parent-call correlation back to the host's progress projection.

## Public surface

- `RunProgressIdentity` is assigned by the host and identifies the client,
  thread, request, source, and agent. The request id is also the durable run
  id, so callers must keep it stable for the lifetime of the turn.
- `RunProgressLedger::new` creates an observer. Pass `Some(store)` to persist
  events or `None` when no run store is installed; without a store the ledger
  still maintains in-memory subagent correlation and an event count.
- `RunProgressLedger::observe` accepts each `AgentProgress` in stream order
  and returns a `ProgressLedgerContext` for the host's projection.
- `RunProgressLedger::finish` settles an open run after the progress stream
  ends. `events_seen` reports the number of observed events, with or without
  persistence.

```rust,ignore
let ledger = RunProgressLedger::new(Some(store), identity);
let projection_context = ledger.observe(&progress).await;
// Project the event using `projection_context.parent_call_id`.
ledger.finish().await;
```

## Event and status behavior

Serializable progress events are appended to the run's event history. A
successful `TurnCompleted` transitions the run to `Completed`; a completion
stopped by the failure breaker transitions it to `Failed`. The terminal status
and its corresponding event use `transition_agent_run_status_from`, which
commits both atomically. Other progress events append without changing status.

`SubagentSpawned` records the task-to-parent-call mapping in memory. Later
subagent lifecycle and model events carry that parent call id in the returned
projection context; completion or failure consumes the mapping. Correlation is
local to this observer and is not written to the run ledger.

## Operational constraints

Ledger reads and writes are best-effort: a storage error is logged and never
fails or cancels the live agent turn. If a terminal status-and-event write
fails, the observer keeps the terminal intent in memory. `finish` retries it
before considering an `Interrupted` transition, so cleanup cannot replace a
known completion with an interruption. This retry is process-local; if the
process exits before it succeeds, normal startup orphan recovery handles the
still-open durable run.

The observer serializes calls with one async mutex, preserving event order and
correlation state. Hosts should call `observe` from their single ordered
progress projection and call `finish` only after the stream has ended. Use one
ledger instance and request id per foreground turn; the module does not
coordinate duplicate request ids across separate instances or processes.
