# Conversations storage schema v2

`tinyagents_session::conversations_storage_schema()` declares what
`DriverConversationStore` stores, for the host's `Registry::compose` (owner
`conversations`, scope class Tenant). The store is shared by a tenant's agents,
so a host binds it to the tenant scope, not to an agent.

| v1 | v2 |
|---|---|
| `conversation_threads_v1` (id: hex SHA-256 of the thread id; `{schema, incarnation, deleted, visible_end, thread: {camelCase}}`) | `conversations.threads`, id `Key::of([thread_id])`; flat snake_case body (`id`, `title`, `chat_id`, `is_active`, `created_ms`, `last_message_ms`, `parent_thread_id`, `labels`, `personality_id`, `working_dir`, `incarnation`, `deleted`, `visible_end`, `messages_key`); the count is `visible_end` |
| `conversation_appends_v1` (id `<sha>:<incarnation>`; every field a string; `record` a JSON string) | `conversations.appends`, id `Key::of([thread_id, incarnation])`; `{token, thread_id, incarnation, offset, record}` with `offset` a number and `record` an object |
| stream `conversation_messages_v1:<sha>:<incarnation>` (`{schema, incarnation, message: {camelCase}}`) | stream `conversations.messages/<thread>/<incarnation>`; entries `{id, content, message_type, extra_metadata, sender, created_ms}` |

`ConversationThread` and `ThreadMessage` are wire types the app mirrors; only
their stored form changes (`dto.rs`). `extra_metadata` is kept verbatim.

## Steps

`threads_v2` and `appends_v2` are document steps over the v1 collections;
`messages_v2` is a stream step over the `conversation_messages_v1:` prefix. They
run with `Registry::migrate` inside one scope. A document that does not decode
is dropped. A migrated thread keeps its `_created_ms`.

### The thread id of a migrated stream

A v1 stream is named by a digest of the thread id, and neither the stream nor
its entries hold the id; a stream step sees nothing else. So `messages_v2` moves
the stream to `conversations.messages/<sha>/<incarnation>`, and `threads_v2`,
which does hold the id, sets `messages_key` to that digest on the migrated
thread. The store reads (and appends to) the stream named by `messages_key`
while it is set. A message edit or tail cut copies the log to a fresh
incarnation whose stream is named by the thread id, and the document drops
`messages_key`. Intents need no such field: their body holds the thread id.

## Behaviour changes

- Times are stored as whole milliseconds and read back as UTC
  (`2026-01-01T00:00:00.250Z`); a sub-millisecond fraction or a non-UTC offset
  does not survive. A time that is not RFC 3339 is refused when written
  (`InvalidInput`); one found in v1 data migrates as the epoch.
- A migrated stream ends at its first row that is not a v1 record of its
  incarnation (the v1 reader refused it at that row too).
- The append intent is no longer all strings. That guarded against a body TTL on
  its fields; a record's expiry is now its `_expires_ms`, which the store never
  sets.
- A thread update (`ensure_thread`, title, labels, working directory, delete) is
  one `Repo::update` loop, which backs off with `tokio::time::sleep` on
  contention: a runtime that drives the store needs timers enabled. The append
  and rewrite loops stay hand-written: they coordinate an intent, a stream and
  the thread, which one document update cannot express.
