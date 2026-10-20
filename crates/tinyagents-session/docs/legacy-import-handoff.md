# Legacy session import handoff to OpenHuman

OpenHuman owns the former workspace layout and the source-reading workflow.
`tinyagents-session` accepts host-bound storage and exposes target writes and
owner-local in-storage migrations; it does not discover workspace files, open
SQLite sources, or rewrite JSONL files.

The removed implementation is recoverable at TinyAgents commit
[`bec73972c1a8c96ce0bc54dd77f56da2e145f875`](https://github.com/senamakel/tinyagents/tree/bec73972c1a8c96ce0bc54dd77f56da2e145f875/crates/tinyagents-session).
That is the parent of `c30844a4e`, which removed the source import and workspace
opening code. OpenHuman #440 should take over these paths and behaviors:

| Former source | Former entry point | Host responsibility |
| --- | --- | --- |
| `src/legacy/source.rs`, `rows.rs`, `workspace.rs` | `legacy::open_source`, `legacy::import_workspace` | Verify ownership/quiescence, read `session_db/sessions.db` as an immutable snapshot, enumerate old schema rows and coordinate the whole migration. |
| `src/run_ledger/drivers/import.rs` | `DriverRunLedger::import_legacy_database` | Decode classic sessions, messages, tool calls, runs, events, telemetry, teams, tasks and entry-tree rows, then write them to the host-selected target. |
| `src/run_ledger/drivers/legacy_import.rs` | `DriverRunLedger::import_agent_family` | Preserve the operation-id receipt and all-or-none target batch behavior while the host owns source authorization and bytes. |
| `src/legacy/transcripts.rs`, `transcripts_once.rs`, `fixtures.rs` | `legacy::import_transcripts`, `legacy::import_transcripts_once` | Read `session_raw/*.jsonl`, preserve stems and request identity, and track resumable completion. |
| `src/legacy/turn_states.rs` | `legacy::import_turn_states` | Read the former `memory/conversations/turn_states/` JSON snapshots and write turn-state ports. |
| `src/legacy/rewrite.rs`, `src/transcript/writer.rs` | `legacy::rewrite_legacy_transcripts` | Own any requested in-place JSONL rewrite, including backup/recovery and file lifecycle. |
| `src/port/workspace.rs`, `src/port/types.rs` | `WorkspaceStores::open(workspace_dir, agent_id)` | Resolve workspace and agent identity, select/open storage, import old files, and construct the host's `SessionStoreProvider`. |

For ordinary target writes, the host receives a `ScopedStorage` already bound
by its own identity policy and can build `DriverSessionStores` with an
`AgentStorageFactory`. Session history uses `SessionHistoryStore` methods such
as `record_session_start`, `record_message`, `record_tool_call`, and
`record_session_end`; turn snapshots use `TurnStates`; transcripts use
`TranscriptLocator::open_stem` and `TranscriptHistory::append_turn`; chat rows
use `ConversationPersistence`. Imported entry trees use
`RunLedgerStore::entry_import_legacy(session_id, entries)`, which accepts typed
entries and performs target writes only. None of these methods accepts a path,
SQLite connection, workspace, tenant grammar, or source authority.

The source files are no longer present in this branch. Their main regression
fixtures and tests were under `src/legacy/{files_tests.rs,mod_tests.rs,
rewrite_tests.rs,transcripts_once_tests.rs}` and
`src/run_ledger/drivers/{import_tests.rs,legacy_state_tests.rs}`. OpenHuman #440
should port source lifecycle, read-only behavior, schema compatibility,
idempotence and resume tests to the host importer. Keep this crate's
`run_ledger/drivers/legacy_state.rs` self-migration: it reads old domain
documents already in the supplied storage scope and moves live workflow leases
and history counters to shared primitives before the owner's schema migration.

The existing `storage_schema()` steps likewise migrate records already in the
host-supplied backend. They do not inspect the former workspace files. Session
scope resolution and any profile re-homing remain host-owned operations.
