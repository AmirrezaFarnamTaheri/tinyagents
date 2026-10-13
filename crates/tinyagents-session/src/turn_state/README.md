# Turn state

This module persists snapshots of active and recently
completed agent turns. It is separate from transcript storage: snapshots hold
live progress and UI-oriented tool/sub-agent state, while transcript JSONL
remains the durable conversation history.

## Public surface

The `TurnStates` port (`crate::port`) loads, writes, queries, settles, and
recovers snapshots; `InMemoryTurnStates` and the driver-backed
`DriverTurnStates` implement it. `types.rs` defines the serializable `TurnState`, lifecycle, phase,
tool timeline, and sub-agent activity shapes. The crate re-exports the common
types from its turn-state module.

## Persistence and lifecycle

Each request has one snapshot in the store the host installed (memory by
default, or any `tinystoragedrivers` backend). Snapshots survive a restart only
with a durable driver-backed store; the in-memory default does not. Conditional writes are
compare-and-swap operations on the snapshot document. Startup recovery
marks leftover non-terminal turns interrupted. Completed snapshots are pruned
to the newest retention window per thread; active snapshots are retained.
Settlement is idempotent for missing or already-terminal snapshots.

Snapshots are operational state, not an audit log. Callers
should persist progress at meaningful turn boundaries and settle a turn when
its final lifecycle becomes known.
