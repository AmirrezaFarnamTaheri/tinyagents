# Transcript view

This module projects a thread's persisted transcripts (listed by a
`TranscriptLocator`) into display records and serves newest-first pages. The
store stays the source of truth; nothing is cached, so every page reflects the
store as it is now. Agent-scoped reads list only the roots that agent owns.

## Public surface

`get_page_in` returns `TranscriptPage` values. The `project` module exposes
record and thread projection helpers (`project_thread_in`, `project_transcripts`), while `types.rs` defines
the serialized display item and status shapes. Cursors are opaque exclusive
chronological upper bounds; callers should pass back `next_cursor` unchanged.

## Resolution and ordering

The locator's `thread_transcripts` selects root generations and sub-agent
transcripts; `resolve.rs` drops the rows a successor generation re-wrote from
its predecessor.
`subagents.rs` correlates child sessions with spawning calls and places their
records in the root conversation. A scoped child must have a durable ownership
relationship to an accepted root; a matching thread identifier alone is not
sufficient. Projection retains chronology before pagination reverses each page
for display.

## Operational constraints

Transcript reads are best-effort over append-only sources. Unreadable
candidates are skipped with diagnostics. Keep pagination cursors stable.

`status_map.rs` expresses `TranscriptSubagentStatus` in terms of the canonical `OrchestrationTaskStatus` (`From` in, `TryFrom` out; `Cancelled` has no projection). Its serialized strings are unchanged. Mapping table: `tinyagents-tasks` README.
